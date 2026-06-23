import os
import sys
import re
import pandas as pd

# Force unbuffered printing so all prints show up instantly in real-time
def print(*args, **kwargs):
    kwargs['flush'] = True
    import builtins
    builtins.print(*args, **kwargs)

from .mapper import SchemaMapper
from .agent import translate_sql, translate_pyspark, generate_sql_test_cases, summarize_failure, summarize_sql, summarize_pyspark, analyze_validation_failure
from .dual_validator import validate_dual_engine
from .models import TestCasesResult
from parsers.sql_parser import parse_sql
from parsers.pyspark_parser import parse_pyspark
from dataclasses import dataclass

@dataclass
class ConversionResult:
    source_file: str
    sql_server_table_name: str
    databricks_table_name: str
    success: bool
    final_mapped_sql: str | None = None
    errorMessage: str | None = None
    ai_summary: str | None = None
    code_summary: dict | None = None
    parser_output: dict | None = None
    pass_case_1: bool = False
    pass_case_2: bool = False
    pass_case_3: bool = False
    details: str = ""

def extract_cte_names(sql: str) -> set[str]:
    names = set()
    pattern = re.compile(r"(?i)(?:\bWITH|,)\s+([A-Za-z_][\w]*)\s*(?:\([^)]*\))?\s+AS\s*\(")
    for match in pattern.finditer(sql):
        names.add(match.group(1).lower())
    return names

def drop_empty_ddl_cte_tables(test_cases: TestCasesResult, source_sql: str) -> TestCasesResult:
    cte_names = extract_cte_names(source_sql)
    if not cte_names:
        return test_cases

    filtered_tables = []
    dropped = []
    for table in test_cases.tables:
        has_empty_ddl = not table.sql_server_ddl.strip() or not table.databricks_ddl.strip()
        if has_empty_ddl and table.table_name.lower() in cte_names:
            dropped.append(table.table_name)
            continue
        filtered_tables.append(table)

    if dropped:
        print(f"    - [INFO] Ignoring CTE-only testcase entries with empty DDL: {', '.join(dropped)}")
        test_cases.tables = filtered_tables
    return test_cases

def process_file(
    source_file: str,
    source_code: str,
    source_dialect: str = "SQL Server",
    target_dialect: str = "Databricks Unity Catalog SQL",
    input_language: str = "sql",
    output_language: str = "sql",
    user_feedback: str = None,
    test_cases_output_dir: str | None = None,
    skip_validation: bool = False,
) -> ConversionResult:
    print(f"[STEP 1/7] Parsing source {input_language.upper()} and generating structured summary...")
    if input_language == "pyspark":
        parser_output = parse_pyspark(source_code)
    else:
        parser_output = parse_sql(source_code)
        
    code_summary = None
    summary_error = None
    try:
        if input_language == "pyspark":
            code_summary = summarize_pyspark(
                source_code=source_code,
                parser_output=parser_output,
                source_platform=source_dialect,
                target_platform="Databricks Latest DBR",
            ).model_dump()
        else:
            code_summary = summarize_sql(
                source_sql=source_code,
                parser_output=parser_output,
                source_platform=source_dialect,
                target_platform="Databricks Latest DBR",
            ).model_dump()
    except Exception as e:
        summary_error = f"AI Summary Error: {e}"
        print(f"    - [WARN] {summary_error}")

    print("[STEP 2/7] Initializing SchemaMapper and ingesting dialect rules...")
    mapper = SchemaMapper()
    mapping_rules = mapper.get_full_mapping_string()
    
    table_name = os.path.splitext(os.path.basename(source_file))[0]
    
    print("[STEP 3/7] Custom AI conversion skills database loading removed per user request.")

    test_cases = None
    if not skip_validation:
        print("[STEP 4/7] Invoking Databricks LLM agent to generate 3 edge/normal/boundary test datasets...")
        try:
            test_cases = generate_sql_test_cases(source_code, code_summary=code_summary)
            test_cases = drop_empty_ddl_cte_tables(test_cases, source_code)
            print(f"    - [PASS] Generated mock tables: {', '.join(t.table_name for t in test_cases.tables)}")
            for t in test_cases.tables:
                print(f"\n    [DEBUG Databricks DDL] Table {t.table_name}: {t.databricks_ddl}")
                print(f"    [DEBUG SQL Server DDL] Table {t.table_name}: {t.sql_server_ddl}\n")
        except Exception as e:
            return ConversionResult(
                source_file=source_file, 
                sql_server_table_name=table_name,
                databricks_table_name=mapper.get_mapped_name(table_name),
                success=False, 
                errorMessage=f"AI Test Generation Error: {e}",
                ai_summary=summary_error,
                code_summary=code_summary,
                parser_output=parser_output,
            )

        print("[STEP 5/7] Serializing and writing generated datasets to output CSV files...")
        case_output_dir = test_cases_output_dir or os.path.dirname(source_file).replace("input_sql", "output")
        os.makedirs(case_output_dir, exist_ok=True)
        
        for table in test_cases.tables:
            if table.dataset_1_edge: pd.DataFrame(table.dataset_1_edge).to_csv(os.path.join(case_output_dir, f"{table.table_name}_case1.csv"), index=False)
            if table.dataset_2_normal: pd.DataFrame(table.dataset_2_normal).to_csv(os.path.join(case_output_dir, f"{table.table_name}_case2.csv"), index=False)
            if table.dataset_3_boundary: pd.DataFrame(table.dataset_3_boundary).to_csv(os.path.join(case_output_dir, f"{table.table_name}_case3.csv"), index=False)
        print("    - [PASS] CSV mock datasets successfully stored on disk.")
    else:
        print("[STEP 4/7] Skipping Databricks LLM testcase generation (skip_validation=True)...")
        print("[STEP 5/7] Skipping mock datasets serialization...")

    print("[STEP 6/7] Invoking translation agent with comprehensive Schema mapping and Dialect skills...")
    max_retries = 2 if not skip_validation else 0
    attempt = 0
    translation = None
    val_res = None
    latest_error = None
    
    while attempt <= max_retries:
        if attempt > 0:
            print(f"    - [Self-Correction] Attempt {attempt}/{max_retries}...")
        else:
            print(f"    - Translating SQL...")
            
        try:
            if output_language == "pyspark":
                translation = translate_pyspark(
                    source_code=source_code, 
                    source_dialect=source_dialect, 
                    target_dialect=target_dialect, 
                    mapping_rules=mapping_rules, 
                    errors=latest_error, 
                    user_feedback=user_feedback,
                    code_summary=code_summary
                )
                translation.final_mapped_sql = translation.final_mapped_code
                translation.test_databricks_sql = translation.test_databricks_code
                translation.test_sql_server_sql = translation.test_source_code
            else:
                translation = translate_sql(
                    source_sql=source_code, 
                    source_dialect=source_dialect, 
                    target_dialect=target_dialect, 
                    mapping_rules=mapping_rules, 
                    errors=latest_error, 
                    user_feedback=user_feedback,
                    code_summary=code_summary
                )
                translation.test_sql_server_sql = source_code
        except Exception as e:
            return ConversionResult(
                source_file=source_file, 
                sql_server_table_name=table_name,
                databricks_table_name=mapper.get_mapped_name(table_name),
                success=False, 
                errorMessage=f"AI Translation Error: {e}",
                ai_summary=summary_error,
                code_summary=code_summary,
                parser_output=parser_output,
            )
            
        if skip_validation:
            print("[STEP 7/7] Skipping Dual-Engine Validation (skip_validation=True)...")
            success = True
            break
            
        print("[STEP 7/7] Running Dual-Engine Validation against SQL Server & Databricks SQL Warehouse...")
        val_res = validate_dual_engine(translation.test_databricks_sql, translation.test_sql_server_sql, test_cases)
        
        success = val_res.error is None and val_res.pass_case_1 and val_res.pass_case_2 and val_res.pass_case_3
        if success:
            print("    - [PASS] Validation passed successfully!")
            break
            
        raw_error = val_res.error or val_res.details
        latest_error = raw_error
        if translation:
            latest_error = (
                f"{latest_error}\n\n"
                "[CURRENT test_sql_server_sql]\n"
                f"{translation.test_sql_server_sql}\n\n"
                "[CURRENT test_databricks_sql]\n"
                f"{translation.test_databricks_sql}\n"
            )
        print(f"    - [FAIL] Attempt {attempt} failed: {latest_error[:120].strip()}...")
        
        is_setup_error = any(kw in (raw_error or "").upper() for kw in ["INSERT", "CONSTRAINT", "FOREIGN KEY", "PRIMARY KEY", "TABLE NOT FOUND", "TEST_DATA_QUALITY_ERROR"])
        if is_setup_error:
            print("    - [Self-Correction] Setup / test-data quality issue detected. Regenerating mock test cases...")
            try:
                test_cases = generate_sql_test_cases(source_code, code_summary=code_summary, error_feedback=latest_error)
                test_cases = drop_empty_ddl_cte_tables(test_cases, source_code)
            except Exception as ex:
                print(f"    - Warning: test case regeneration failed: {ex}")
        else:
            print("    - [Intelligent Analysis] Analyzing failure reason using AI Summarizer...")
            try:
                intelligent_feedback = analyze_validation_failure(
                    source_sql=source_code,
                    source_dialect=source_dialect,
                    target_dialect=target_dialect,
                    source_test_sql=translation.test_sql_server_sql if translation else "",
                    target_test_sql=translation.test_databricks_sql if translation else "",
                    raw_error=latest_error
                )
                print(f"      * [Feedback Generated] {intelligent_feedback[:100].strip()}...")
                latest_error = f"INTELLIGENT VALIDATION ANALYSIS:\n{intelligent_feedback}\n\nRAW ERROR DETAILS:\n{latest_error}"
            except Exception as e:
                print(f"      * [Warning] Feedback generation failed: {e}")
            
        attempt += 1

    ai_summary = None
    if not success:
        try:
            summary = summarize_failure(source_code, latest_error)
            ai_summary = summary.failure_summary
        except:
            pass
    if summary_error and ai_summary:
        ai_summary = f"{summary_error}\n{ai_summary}"
    elif summary_error:
        ai_summary = summary_error

    error_message = None
    if not success:
        error_message = val_res.error if val_res and val_res.error else "Validation failed"

    return ConversionResult(
        source_file=source_file,
        sql_server_table_name=table_name,
        databricks_table_name=mapper.get_mapped_name(table_name),
        success=success,
        final_mapped_sql=translation.final_mapped_sql if translation and not (val_res and val_res.error) else None,
        errorMessage=error_message,
        ai_summary=ai_summary,
        code_summary=code_summary,
        parser_output=parser_output,
        pass_case_1=val_res.pass_case_1 if val_res else False,
        pass_case_2=val_res.pass_case_2 if val_res else False,
        pass_case_3=val_res.pass_case_3 if val_res else False,
        details=val_res.details if val_res else ""
    )
