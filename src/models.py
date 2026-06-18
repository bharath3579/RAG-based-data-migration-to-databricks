from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class TestTable(BaseModel):
    table_name: str = Field(description="The unqualified table name (e.g., 'customers').")
    sql_server_ddl: str = Field(description="CREATE TABLE statement for SQL Server. Do not include database/schema prefixes.")
    databricks_ddl: str = Field(description="CREATE TABLE statement for Databricks. Do not include catalog/schema prefixes.")
    dataset_1_edge: List[Dict[str, Any]] = Field(description="JSON array of objects representing rows with edge cases (NULLs, empty strings, extremes).")
    dataset_2_normal: List[Dict[str, Any]] = Field(description="JSON array of objects representing realistic, normal data distributions.")
    dataset_3_boundary: List[Dict[str, Any]] = Field(description="JSON array of objects representing boundary conditions or stress tests.")

class TestCasesResult(BaseModel):
    tables: List[TestTable] = Field(description="List of tables needed to execute the query, complete with their DDLs and 3 synthetic datasets.")
    reasoning: str = Field(description="Explanation of the generated data cases.")

class TranslationResult(BaseModel):
    final_mapped_sql: str = Field(description="The final converted Databricks SQL using the provided catalog/schema mapping.")
    test_databricks_sql: str = Field(description="The converted Databricks SQL adapted for testing in the environment-specified catalog/schema.")
    test_sql_server_sql: str = Field(description="The original or adapted SQL Server query formatted to run in the environment-specified database/schema.")
    explanation: str = Field(description="Explanation of the migration and any changes made.")

class SummaryResult(BaseModel):
    failure_summary: str = Field(description="Concise summary explaining why the migration or validation failed.")

class CodeSummaryResult(BaseModel):
    source_platform: str = Field(description="Detected or user-provided source platform.")
    target_platform: str = Field(description="Target platform for the migration.")
    language: str = Field(description="Source language, for MVP this is sql.")
    tables_and_schema: Dict[str, Any] = Field(description="Tables, aliases, and referenced columns or schemas inferred from the SQL.")
    functions: List[str] = Field(description="SQL functions and operators used by the source code.")
    technical_summary: str = Field(description="Structured, step-wise technical explanation of the source code behavior.")

class SparkFunctionNames(BaseModel):
    function_names: List[str] = Field(description="List of Spark SQL function names that are equivalent or relevant to the source functions.")

class PySparkTranslationResult(BaseModel):
    final_mapped_code: str = Field(description="The final native PySpark DataFrame code.")
    test_databricks_code: str = Field(description="The native PySpark DataFrame code adapted for testing in Databricks.")
    test_source_code: str = Field(description="The original source code adapted for testing in the source environment.")
    explanation: str = Field(description="Explanation of the migration and any changes made.")
