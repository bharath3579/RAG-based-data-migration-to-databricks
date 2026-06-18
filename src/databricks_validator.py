import os
import pandas as pd
from databricks import sql
from .models import TestCasesResult
from .validator import ValidationResult

def get_db_connection():
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    
    if not host or not token or not http_path:
        raise ValueError("Missing DATABRICKS_HOST, DATABRICKS_TOKEN, or DATABRICKS_HTTP_PATH")
    
    hostname = host.replace("https://", "").split("/")[0]
    
    return sql.connect(
        server_hostname=hostname,
        http_path=http_path,
        access_token=token
    )

def validate_sql_on_databricks(testable_sql: str, test_cases: TestCasesResult) -> ValidationResult:
    catalog = os.getenv("CATALOG_NAME", "testing_sql_ai")
    schema = os.getenv("SCHEMA_NAME", "sandbox")
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Setup Sandbox
        print(f"      [Databricks] Setting up {catalog}.{schema}...")
        cursor.execute(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        cursor.execute(f"USE CATALOG {catalog}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cursor.execute(f"USE SCHEMA {schema}")
        
        # 2. Cleanup existing tables in sandbox (optional but safer for idempotency)
        # For simplicity in this version, we will just try to create. 
        # If the AI generates 'CREATE TABLE', it might fail if it already exists.
        # We handle this by using 'CREATE OR REPLACE' or letting it fail and retry.
        
        # 3. Create mock tables and insert data
        if test_cases and test_cases.tables:
            for table in test_cases.tables:
                print(f"      [Databricks] Creating table {table.table_name}...")
                # Ensure we are using 'CREATE OR REPLACE' for idempotency in the sandbox
                ddl = table.ddl.replace("CREATE TABLE", "CREATE OR REPLACE TABLE")
                cursor.execute(ddl)
                
                if table.insert_statements:
                    for stmt in table.insert_statements:
                        cursor.execute(stmt)
        
        # 4. Execute the translated SQL
        print(f"      [Databricks] Executing testable SQL...")
        # Since we are in the sandbox, we don't need to strip prefixes, 
        # but we should ensure the SQL targets the local schema.
        cursor.execute(testable_sql)
        
        # 5. Capture results
        try:
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=columns)
            
            csv_output = df.to_csv(index=False)
            test_data_dump = "Validations performed on live Databricks SQL Warehouse."
            
            return ValidationResult(
                error=None,
                test_data_dump=test_data_dump,
                csv_output=csv_output
            )
        except Exception:
            # Query might be DDL/DML with no results
            return ValidationResult(
                error=None,
                test_data_dump="Command executed successfully on Databricks.",
                csv_output="No result set returned (DDL/DML)."
            )

    except Exception as e:
        return ValidationResult(
            error=str(e),
            is_test_data_error=False
        )
    finally:
        if conn:
            conn.close()
