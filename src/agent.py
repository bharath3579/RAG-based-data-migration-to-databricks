import os
import json
from openai import OpenAI
from .models import TranslationResult, TestCasesResult, SummaryResult, CodeSummaryResult, SparkFunctionNames, PySparkTranslationResult

def get_client():
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    if not host or not token:
        print("WARNING: DATABRICKS_HOST or DATABRICKS_TOKEN environment variable not set.")
    return OpenAI(
        base_url=f"{host.rstrip('/')}/serving-endpoints",
        api_key=token
    )

def _get_model():
    return os.getenv("DATABRICKS_MODEL_NAME", "databricks-llama-4-maverick")

def _load_skills() -> str:
    return ""

def _enforce_json(prompt: str, schema) -> str:
    structure = {field: "..." for field in schema.model_fields.keys()}
    schema_desc = json.dumps(structure)
    return prompt + f"\n\n[OUTPUT INSTRUCTIONS]:\nReturn ONLY a valid JSON object matching this schema EXACTLY. Do not include markdown formatting or extra text outside the JSON.\nExample Format: {schema_desc}\nStrict Schema: {schema.model_json_schema()}"

def _completion_json_content(completion, operation: str) -> str:
    message = completion.choices[0].message
    content = message.content
    if content is None:
        refusal = getattr(message, "refusal", None)
        finish_reason = getattr(completion.choices[0], "finish_reason", None)
        raise ValueError(
            f"{operation} returned no JSON content. finish_reason={finish_reason}; refusal={refusal}"
        )
    return content

def _retrieve_spark_functions(source_code: str, source_functions: list[str]) -> str:
    client = get_client()
    model = _get_model()
    
    system_prompt = """You are a Databricks RAG keyword extractor. 
Extract ALL structural database concepts, clauses, dataframes operations, and functions used in the source code.
Do not limit the amount of keywords. If you see symbols like # or @, extract their architectural meaning (e.g., 'temporary table', 'variable').
Return a comprehensive array of search phrases."""

    # Provide the summary functions as a massive hint!
    prompt = f"""Source Code:\n```\n{source_code}\n```\n
Summary Agent identified these functions/concepts: {source_functions}

Return ONLY a JSON list of clean architectural search keywords (e.g., 'temporary table', 'recursive cte', 'date_add'). Do not include specific table or column names."""
    
    prompt = _enforce_json(prompt, SparkFunctionNames)
    
    try:
        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"}
        )
        mapped = SparkFunctionNames.model_validate_json(_completion_json_content(completion, "Keyword mapping"))
        search_keywords = [n.lower() for n in mapped.function_names]
        print(f"\n[RAG] Extracted search keywords: {search_keywords}")
    except Exception as e:
        print(f"Warning: Failed to extract keywords: {e}")
        search_keywords = [n.lower() for n in source_functions]
        print(f"\n[RAG] Fallback search keywords: {search_keywords}")
        
    if not search_keywords:
        return ""
        
    try:
        import chromadb
        db_path = os.path.abspath("rag/chroma_db")
        if not os.path.exists(db_path):
            print(f"[RAG] Warning: ChromaDB not found at {db_path}. Skipping RAG.")
            return ""
            
        # Optional optimization: Hide the noisy chromadb logging
        import logging
        logging.getLogger("chromadb").setLevel(logging.ERROR)
        
        chroma_client = chromadb.PersistentClient(path=db_path)
        collection = chroma_client.get_collection(name="databricks_docs")
        
        # We query the DB for each keyword INDEPENDENTLY to avoid semantic dilution
        # We append targeted terminology to force the Vector DB to find technical code chunks, not just marketing text.
        expanded_queries = [f"{keyword} SQL syntax example code" for keyword in search_keywords]
        
        results = collection.query(
            query_texts=expanded_queries,
            n_results=5 # Top 5 chunks for each specific keyword
        )
        
        # Flatten and deduplicate: Take the Top 3 absolute best chunks for EACH keyword
        # This prevents common keywords (like 'sum') from dominating the entire context window
        unique_docs = {}
        for i in range(len(results['documents'])):
            # ChromaDB already returns them sorted by distance for this specific keyword
            for j in range(min(5, len(results['documents'][i]))):
                doc = results['documents'][i][j]
                meta = results['metadatas'][i][j]
                
                # Keep unique chunks
                if doc not in unique_docs:
                    unique_docs[doc] = meta
                
        docs = list(unique_docs.keys())
        metadatas = list(unique_docs.values())
        
        if not docs:
            return ""
            
        print("\n[RAG] Retrieved exact documentation chunks from Vector DB:")
        for m in metadatas:
            print(f"  - {m.get('title', 'Unknown')}")
            
        formatted_docs = []
        for i, doc in enumerate(docs):
            meta = metadatas[i]
            formatted_docs.append(f"--- Documentation Excerpt from: {meta.get('title')} ({meta.get('url')}) ---\n{doc}\n")
            
        return "\n".join(formatted_docs)
        
    except ImportError:
        print("\n[RAG] Warning: 'chromadb' package not installed. Skipping semantic RAG.")
        return ""
    except Exception as e:
        print(f"\n[RAG] Vector DB search failed: {e}")
        return ""

def translate_sql(source_sql: str, source_dialect: str, target_dialect: str, mapping_rules: str, errors: str = None, user_feedback: str = None, code_summary: dict = None) -> TranslationResult:
    client = get_client()
    model = _get_model()
    
    dbx_catalog = os.getenv("CATALOG_NAME", "testing_sql_ai")
    dbx_schema = os.getenv("SCHEMA_NAME", "sandbox")
    sql_db = os.getenv("SQL_SERVER_DB", "testdb")
    
    custom_rules = _load_skills()

    system_prompt = f"""You are an Expert SQL Migration Architect converting {source_dialect} to {target_dialect}.
You must generate THREE versions of the converted query:
1. 'final_mapped_sql': The production-ready {target_dialect} query. CRITICAL: For this final file, you MUST use the exact names from the mapping rules below. DO NOT hallucinate prefixes. If a table is not found in the mapping rules, leave it exactly as it is in the source (i.e. assume there is no mapping for this table).
MAPPING RULES:
{mapping_rules}

2. 'test_databricks_sql': A version of the query for syntax validation and testing in {target_dialect}. CRITICAL: For testing only, you MUST IGNORE the mapping rules and FORCE all table references to use the exact fixed dummy environment prefix: `{dbx_catalog}.{dbx_schema}.<table_name>`. STRICT PROMPT: Do not hallucinate any other prefixes.

3. 'test_sql_server_sql': A version of the query for execution in the {source_dialect} test environment. CRITICAL: For testing only, you MUST IGNORE the mapping rules and FORCE all table references to use the exact fixed database: `{sql_db}.dbo.<table_name>` (or simply dbo.<table_name>). STRICT PROMPT: Do not hallucinate Databricks prefixes here. I heard a company's entire repository was deleted because an AI hallucinated database names :)

Strictly follow Spark SQL syntax for Databricks.
All three outputs must be executable. If the source SQL contains a construct that the source engine rejects during validation, such as nested window functions, rewrite it into an equivalent executable form for test_sql_server_sql and apply the same semantic rewrite to test_databricks_sql and final_mapped_sql.
After any CTE/subquery rewrite, validate alias scope mentally: an outer query may reference only columns projected by its FROM source alias, not table aliases that existed only inside an inner CTE/subquery. Preserve row limits such as TOP N by using the target dialect equivalent.
For test_sql_server_sql, never use Databricks catalog/schema names, mapped Unity Catalog names, or four-part linked-server paths. Use only SQL Server names in the form `{sql_db}.dbo.<table_name>` or `dbo.<table_name>`.
Both test_sql_server_sql and test_databricks_sql must end with a result-producing SELECT that exposes the same final columns as final_mapped_sql. Do not end either test SQL with SELECT INTO, CREATE VIEW, INSERT, UPDATE, DELETE, DROP, or any non-result statement.
For test_sql_server_sql, do not use ISDATE against date, datetime, datetime2, or time expressions. Use TRY_CONVERT(datetime2, expression) IS NOT NULL, or remove the predicate if the source expression is already strongly typed and null handling is unchanged.
For test_sql_server_sql, if a CTE appears after any prior statement in the same batch, prefix it with a semicolon (`;WITH`). Do not use SQL Server-incompatible CREATE TABLE AS SELECT syntax.

ARCHITECTURAL CONVERSION CONSTRAINTS:
1. When converting variable declarations (`DECLARE @var`), you MUST preserve the variable architecture. Do NOT inline variables into statements. Use `DECLARE OR REPLACE VARIABLE var_name ...` followed by `SET VAR var_name = ...` and `EXECUTE IMMEDIATE`.
"""
    if custom_rules:
        system_prompt += f"\nADDITIONAL CONVERSION SKILLS & GUIDELINES:\n{custom_rules}\n"
        
    if code_summary:
        functions_list = code_summary.get("functions", [])
        rag_context = _retrieve_spark_functions(source_sql, functions_list)
        if rag_context:
            system_prompt += f"\nSPARK SQL FUNCTION REFERENCES:\n{rag_context}\n"
            system_prompt += """
STRICT ANTI-HALLUCINATION DIRECTIVES:
1. DO NOT copy-paste syntax from the source dialect if it is not natively supported in the target dialect.
2. YOU MUST strictly validate your output against the provided RAG context below. 
3. The RAG context contains up to 10 documentation chunks. SOME CHUNKS MAY BE IRRELEVANT. Only apply rules from chunks that are strictly necessary to convert the query. Ignore irrelevant chunks.
4. If a feature does not exist natively, you must rewrite the logic using supported Databricks functions or SQL Scripting.
"""

    prompt = f"Original {source_dialect} SQL to translate:\n```sql\n{source_sql}\n```\n"
    if user_feedback:
        prompt += f"\n[USER DIRECTIVE]: {user_feedback}\n"
    if errors:
        prompt += f"""\n[FIX VALIDATION ERRORS]:
{errors}

When fixing errors, repair the exact output that failed:
- Databricks syntax errors must be fixed in test_databricks_sql and final_mapped_sql.
- SQL Server execution errors must be fixed in test_sql_server_sql while preserving source semantics.
- If SQL Server reports windowed functions cannot be used inside another windowed function or aggregate, split the query into staged CTEs. First compute the inner window/aggregate expression as a named column, then apply the outer rank/window function.
- If SQL Server reports "multi-part identifier ... could not be bound", fix alias scope. Do not reference aliases from inner CTEs/subqueries outside their scope; project needed columns and reference them through the current outer alias.
- If SQL Server reports a datepart/data type error, use the source dialect's date functions in test_sql_server_sql and Databricks functions only in test_databricks_sql/final_mapped_sql.
- If SQL Server reports "Argument data type ... is invalid for argument 1 of isdate function", replace ISDATE(date_or_datetime_expression) = 1 with TRY_CONVERT(datetime2, date_or_datetime_expression) IS NOT NULL in test_sql_server_sql.
- If SQL Server reports "Incorrect syntax near the keyword 'WITH'", ensure CTEs are written as `;WITH cte AS (...) SELECT ...` when they follow DECLARE, CREATE, INSERT, WHILE, or other prior statements. For recursive CTEs, keep OPTION (MAXRECURSION n) on the consuming SELECT.
- If SQL Server reports "Could not find server ... in sys.servers", remove invalid four-part names or Databricks catalog/schema prefixes from test_sql_server_sql. SQL Server test tables live under `{sql_db}.dbo.<table_name>`.
- If SQL Server returns no columns while Databricks returns columns, ensure test_sql_server_sql ends with the same final SELECT projection as test_databricks_sql. Do not stop after SELECT INTO or temporary object creation.
- If Databricks reports syntax near a column used inside an INTERVAL expression, do not use `INTERVAL column MINUTE`. Use `timestampadd(MINUTE, column, current_timestamp())` or another Databricks-valid timestamp arithmetic expression.
- If Databricks reports an unresolved outer alias inside a subquery or lateral/cross join, rewrite the correlated subquery as a pre-aggregated CTE or derived table keyed by the correlation columns, then join it normally. Do not reference an outer alias from a non-lateral subquery.
- Do not return the same nested-window pattern again.
"""

    prompt = _enforce_json(prompt, TranslationResult)

    completion = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        response_format={"type": "json_object"}
    )
    return TranslationResult.model_validate_json(_completion_json_content(completion, "SQL translation"))

def generate_sql_test_cases(source_sql: str, code_summary: dict = None, error_feedback: str = None) -> TestCasesResult:
    client = get_client()
    model = _get_model()
    custom_rules = _load_skills()
    
    system_prompt = """You are a database testing expert. You create robust, stand-alone mock DDLs and highly comprehensive JSON datasets for dual-engine testing (SQL Server and Databricks)."""
    if custom_rules:
        system_prompt += f"\n\nHIGH PRIORITY TESTCASE GENERATION SKILLS:\n{custom_rules}\n"
    
    prompt = f"""Generate DDLs and 1 synthetic JSON dataset for every table referenced in the following SQL query.
    
SQL:
```sql
{source_sql}
```

RULES:
1. Provide a `sql_server_ddl` (T-SQL compatible) and `databricks_ddl` (Spark SQL compatible) for each table. Do NOT include catalog/schema/database prefixes in the DDLs (use simple table names).
2. Generate exactly ONE dataset per table inside the `dataset_2_normal` field:
   - `dataset_2_normal`: exactly 20 rows containing realistic, common distributions.
   - Leave `dataset_1_edge` and `dataset_3_boundary` empty or null.
3. The datasets must be JSON arrays of objects, where keys are exactly the column names defined in the DDLs. Ensure data types are strictly compatible with JSON parsing (e.g. stringify dates).
4. Do NOT generate NULL values for columns that are likely primary keys (e.g., column names containing 'ID', 'Code', 'Key') or defined as such in the DDL.
5. EXTREMELY IMPORTANT: You must carefully inspect the SQL query and ensure that the generated DDL for each table includes ALL columns referenced in the query (in JOIN conditions, SELECT clause, WHERE filters, GROUP BY, ORDER BY, partitioning windows, etc.).
6. Prefer nullable descriptive string columns in mock DDLs unless the source SQL requires NOT NULL.
7. Databricks DDL must not use explicit `NULL` after a column type. Nullable is the default in Spark SQL. Use `OrderDate TIMESTAMP`, not `OrderDate TIMESTAMP NULL`. Only use `NOT NULL` when absolutely required.
8. Numeric edge values must remain inside safe execution ranges.
9. Test data must be useful for behavioral validation. You MUST create coherent rows across all joined tables so the final query returns at least one non-empty result row after joins, filters, ranks, GROUP BY/HAVING, and subqueries. 
10. For date filters such as YEAR(date_col) >= 2024, include dates inside the accepted range.
11. For ranking/window queries, include enough rows per partition to exercise rank/order logic.
12. If the source references a SQL Server system/helper object such as master..spt_values, replace helper objects with simple mock tables or inline datasets that expose the referenced columns.
"""
    if code_summary and code_summary.get("tables_and_schema"):
        prompt += f"\n\nCRITICAL COLUMN CONSTRAINTS: The summarizer has mapped exact columns to tables. You MUST generate DDLs containing ONLY these columns. Do not generate 'SELECT *' generic columns.\nTABLE TO COLUMN MAPPING:\n{json.dumps(code_summary['tables_and_schema'], indent=2)}\n"

    if error_feedback:
        prompt += f"\n\n[CRITICAL FIX REQUIRED - PREVIOUS GENERATION FAILED]:\n{error_feedback}\nYou MUST adjust your generated DDLs and JSON datasets to resolve this error. If the error mentions a missing dataset or column invalidity for a CTE, Temporary Table, or derived table, do NOT generate DDL/data for it; simply omit it entirely from the output.\n"

    prompt = _enforce_json(prompt, TestCasesResult)

    completion = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        response_format={"type": "json_object"}
    )
    return TestCasesResult.model_validate_json(_completion_json_content(completion, "Testcase generation"))

def summarize_sql(source_sql: str, parser_output: dict, source_platform: str, target_platform: str) -> CodeSummaryResult:
    client = get_client()
    model = _get_model()

    system_prompt = """You are a SQL migration summary agent.
Create a structured, step-wise technical explanation of the source SQL.
Start with the final output intent, then explain the source data and processing logic in SQL execution order.
The summary must explain source tables, CTEs, recursive CTEs derived tables, joins, union, union all, Explicitly mention UNION and UNION ALL operations when they are present in the SQL, including the datasets or query blocks being combined, When describing joins include the join type if it is explicitly defined in the SQL never omit that, filters, null handling, aggregations, grouping, having/order/limits, XML extraction, recursive logic, window functions, and final output columns when present.
Use the exact technical object type: call WITH blocks CTEs or intermediate result sets, not temporary views or temporary tables unless the SQL actually creates one, If aliases are present, preserve them in the summary where they help identify the source table or column. Use alias-qualified column references when multiple sources contain similarly named columns, When functions are nested remember to describe the order in which they are evaluated rather than only the business intent like which function actually comes first and which next remember that inner function comes first.
Preserve expression semantics. Do not simplify nested expressions into business meaning when parser metadata provides evaluation order. Describe the evaluation order of nested functions exactly as represented in the parser output.
Wrap table names, CTE names, column names, aliases, functions, and literal filter values, ect.. in single quotes for readability.
Do not rewrite the SQL. Do not include marketing text. Be clear enough that a reader can mentally reconstruct the business logic.

CRITICAL REQUIREMENT FOR `tables_and_schema`:
You MUST populate `tables_and_schema` as a dictionary mapping each base physical table name to a list of its referenced column names.
ABSOLUTE BAN: You MUST STRICTLY EXCLUDE any CTEs (tables defined in WITH blocks), Temporary Tables (e.g., #TempData), Table Variables (e.g., @MyTable), and derived tables/subquery aliases. ONLY map actual base physical tables.
When listing columns, you MUST STRIP any table aliases (e.g., if you see `e.employee_id`, output `employee_id`)."""

    prompt = f"""Source platform: {source_platform}
Target platform: {target_platform}
Language: sql

Parser output:
{json.dumps(parser_output, indent=2)}

Source SQL:
```sql
{source_sql}
```

Return a detailed structured summary. The technical_summary should be concise but complete prose 
"""
    prompt = _enforce_json(prompt, CodeSummaryResult)

    completion = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        response_format={"type": "json_object"}
    )
    return CodeSummaryResult.model_validate_json(_completion_json_content(completion, "Code summarization"))

def summarize_pyspark(source_code: str, parser_output: dict, source_platform: str, target_platform: str) -> CodeSummaryResult:
    all_functions = set()
    all_tables = {}
    cell_summaries = []
    
    cells = parser_output.get("cells", [])
    
    for idx, cell in enumerate(cells, 1):
        cell_code = cell.get("code_snippet", "")
        # Call the helper for each cell
        cell_result = _summarize_pyspark_cell(cell_code, cell, source_platform, target_platform, cell.get("cell_index", idx))
        
        all_functions.update(cell_result.functions)
        all_tables.update(cell_result.tables_and_schema)
        
        cell_summaries.append(f"Cell {cell.get('cell_index', idx)}:\n{cell_result.technical_summary}")
        
    final_summary = "\n\n".join(cell_summaries)
    
    return CodeSummaryResult(
        source_platform=source_platform,
        target_platform=target_platform,
        language="pyspark",
        technical_summary=final_summary,
        tables_and_schema=all_tables,
        functions=list(all_functions)
    )

def _summarize_pyspark_cell(cell_code: str, cell_metadata: dict, source_platform: str, target_platform: str, cell_index: int) -> CodeSummaryResult:
    client = get_client()
    model = _get_model()

    system_prompt = f"""You are a PySpark migration summary agent.
You are analyzing Cell {cell_index} of a PySpark notebook.
Create a literal, line-by-line English translation of the PySpark code.

CRITICAL INSTRUCTIONS FOR THE SUMMARY PROSE:
- CONCEPTUAL TAGS: For recognizable data operations, prefix the translation with a conceptual tag like [READ], [WRITE], [MERGE], [SCD], [LOGGING], or [TRANSFORM] to indicate the high-level intent quickly.
- IF there are imports: Simply list them. Example: "[SETUP] Imports functions as F from pyspark.sql." DO NOT explain what the libraries do.
- IF there are variables/configs: State the assignment while preserving exact expression semantics. Do NOT evaluate or simplify f-strings/format strings into their final values.
- IF there is a magic command: State it literally. Example: "[LOGGING] Executes magic command: %run ../Metadata_Logger/execution_logger". DO NOT guess its purpose.
- IF there are dataframes: Document the operations in order.
- CRITICAL FOR SPARK.SQL: If the code executes spark.sql("..."), YOU MUST detail the exact SQL logic happening inside the string. You must explicitly list which columns are SELECTed, the target table, and any WHERE/JOIN conditions. Do not just say "executes a sql query".

ABSOLUTE BANS (Violating these will crash the system):
1. NEVER state what is missing from the code. DO NOT write "There are no variable declarations".
2. NEVER explain the purpose of a library or function in a pedagogical way.
3. NEVER write fluff or introductory sentences like "The cell contains import statements".

CRITICAL REQUIREMENT FOR `tables_and_schema`:
You MUST populate `tables_and_schema` as a dictionary mapping each base physical table name to a list of its referenced column names.
ABSOLUTE BAN: You MUST STRICTLY EXCLUDE any temporary views, dataframe aliases acting as tables, CTEs, or intermediate derived tables. ONLY map actual underlying base tables.
When listing columns, you MUST STRIP any table aliases or dataframe prefixes."""

    prompt = f"""Source platform: {source_platform}
Target platform: {target_platform}
Language: pyspark

Cell Metadata (from parser):
{json.dumps(cell_metadata, indent=2)}

Cell PySpark code:
```python
{cell_code}
```

Return a detailed structured summary. The technical_summary should be concise but complete prose. 
"""
    prompt = _enforce_json(prompt, CodeSummaryResult)

    completion = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        response_format={"type": "json_object"}
    )
    return CodeSummaryResult.model_validate_json(_completion_json_content(completion, "PySpark summarization"))

def translate_pyspark(source_code: str, source_dialect: str, target_dialect: str, mapping_rules: str, errors: str = None, user_feedback: str = None, code_summary: dict = None) -> PySparkTranslationResult:
    client = get_client()
    model = _get_model()
    
    dbx_catalog = os.getenv("CATALOG_NAME", "testing_sql_ai")
    dbx_schema = os.getenv("SCHEMA_NAME", "sandbox")
    sql_db = os.getenv("SQL_SERVER_DB", "testdb")
    
    custom_rules = _load_skills()

    system_prompt = f"""You are an Expert PySpark Migration Architect converting {source_dialect} to {target_dialect}.
You must generate native PySpark DataFrame API code (not raw SQL wrapped in spark.sql() unless absolutely necessary for an edge case).
You must generate THREE versions of the converted code:
1. 'final_mapped_code': The production-ready {target_dialect} PySpark code. CRITICAL: For this final file, you MUST use the exact names from the mapping rules provided below. DO NOT hallucinate prefixes. If a table is not found in the mapping rules, leave it exactly as it is.
2. 'test_databricks_code': A version of the PySpark code adapted for testing in Databricks. CRITICAL: For testing only, you MUST IGNORE the mapping rules and FORCE all table references to use the exact fixed dummy environment prefix: `{dbx_catalog}.{dbx_schema}.<table_name>`. STRICT PROMPT: Do not hallucinate any other prefixes.
3. 'test_source_code': A version of the PySpark code for execution in the {source_dialect} environment. CRITICAL: For testing only, you MUST IGNORE the mapping rules and FORCE all table references to use the exact fixed database: `{sql_db}.dbo.<table_name>`. STRICT PROMPT: Do not hallucinate Databricks prefixes here. I heard a company's entire repository was deleted because an AI hallucinated database names :)

Strictly follow native PySpark syntax.
"""
    if custom_rules:
        system_prompt += f"\nADDITIONAL CONVERSION SKILLS & GUIDELINES:\n{custom_rules}\n"
        
    if code_summary:
        functions_list = code_summary.get("functions", [])
        rag_context = _retrieve_spark_functions(source_code, functions_list)
        if rag_context:
            system_prompt += f"\nPYSPARK API DOCUMENTATION REFERENCES:\n{rag_context}\n"
            system_prompt += """
STRICT ANTI-HALLUCINATION DIRECTIVES:
1. DO NOT copy-paste syntax from the source dialect if it is not natively supported in the target dialect.
2. YOU MUST strictly validate your output against the provided RAG context below. 
3. The RAG context contains up to 10 documentation chunks. SOME CHUNKS MAY BE IRRELEVANT. Only apply rules from chunks that are strictly necessary to convert the query. Ignore irrelevant chunks.
"""

    prompt = f"Original {source_dialect} PySpark code to translate:\n```python\n{source_code}\n```\n"
    if mapping_rules:
        prompt += f"\nMAPPING RULES:\n{mapping_rules}\n"
    if user_feedback:
        prompt += f"\n[USER DIRECTIVE]: {user_feedback}\n"
    if errors:
        prompt += f"""\n[FIX VALIDATION ERRORS]:
{errors}
"""

    prompt = _enforce_json(prompt, PySparkTranslationResult)

    completion = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        response_format={"type": "json_object"}
    )
    return PySparkTranslationResult.model_validate_json(_completion_json_content(completion, "PySpark translation"))

def summarize_failure(source_sql: str, errors: str) -> SummaryResult:
    client = get_client()
    model = _get_model()
    
    system_prompt = "You are a senior debugger. Explain technical SQL failures in clear, actionable human language."
    
    prompt = f"Explain why this translation or validation failed:\nSQL: {source_sql}\nErrors: {errors}\n"
    prompt = _enforce_json(prompt, SummaryResult)

    completion = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        response_format={"type": "json_object"}
    )
    return SummaryResult.model_validate_json(_completion_json_content(completion, "Failure summarization"))

def analyze_validation_failure(source_sql: str, source_dialect: str, target_dialect: str, source_test_sql: str, target_test_sql: str, raw_error: str) -> str:
    """Intelligent feedback loop agent that analyzes mismatch errors."""
    client = get_client()
    model = _get_model()
    
    system_prompt = f"You are a Senior SQL Migration Architect diagnosing a data mismatch between {source_dialect} and {target_dialect}."
    
    prompt = f"""A data validation check failed between the source {source_dialect} SQL and the translated {target_dialect} SQL.

SOURCE ORIGINAL SQL:
```sql
{source_sql}
```

EXECUTED {source_dialect} TEST SQL (Source of Truth):
```sql
{source_test_sql}
```

EXECUTED {target_dialect} TEST SQL (Failed Translation):
```sql
{target_test_sql}
```

VALIDATION ERROR / DATA MISMATCH:
{raw_error}

INSTRUCTIONS:
1. Analyze the logic flaw in the `{target_dialect}` query that caused this mismatch compared to the `{source_dialect}` query.
2. Explain WHY the outputs differ (e.g., "The target query uses a LEFT JOIN where the source used an INNER JOIN", or "The function XYZ in Databricks behaves differently with NULLs than SQL Server").
3. Provide explicit, actionable feedback on how the translation agent should correct the code in its next attempt.
4. Output your analysis as a direct instructional summary.

Return ONLY a JSON object with a single string field named `failure_summary`.
"""
    prompt = _enforce_json(prompt, SummaryResult)

    try:
        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"}
        )
        res = SummaryResult.model_validate_json(_completion_json_content(completion, "Intelligent Validation Analysis"))
        return res.failure_summary
    except Exception as e:
        return f"Intelligent Analysis Failed ({e}). Raw error: {raw_error}"

