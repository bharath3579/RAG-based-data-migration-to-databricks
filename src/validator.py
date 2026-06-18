import duckdb
import pandas as pd
from dataclasses import dataclass
from .models import TestCasesResult

@dataclass
class ValidationResult:
    error: str | None = None
    is_test_data_error: bool = False
    test_data_dump: str | None = None
    csv_output: str | None = None

def validate_sql(target_sql: str, test_cases: TestCasesResult) -> ValidationResult:
    """
    Executes the generated SQL against a temporary duckdb database populated with test data.
    """
    conn = None
    test_data_dump = ""
    try:
        conn = duckdb.connect(database=':memory:')
        
        # 1. Execute DDL and Insert Data
        for table in test_cases.tables:
            test_data_dump += f"-- DDL for {table.table_name}\n"
            test_data_dump += table.ddl + ";\n\n"
            try:
                conn.execute(table.ddl)
            except Exception as e:
                return ValidationResult(error=f"Test Data DDL Failed: {e}", is_test_data_error=True, test_data_dump=test_data_dump)

            for insert_stmt in table.insert_statements:
                test_data_dump += insert_stmt + ";\n"
                try:
                    conn.execute(insert_stmt)
                except Exception as e:
                    return ValidationResult(error=f"Test Data INSERT Failed: {e}", is_test_data_error=True, test_data_dump=test_data_dump)
            test_data_dump += "\n"
                    
        # 2. Execute Target SQL
        csv_output = None
        target_upper = target_sql.strip().upper()
        if target_upper.startswith("SELECT") or target_upper.startswith("WITH"):
            # It's a query, we can fetch results as CSV
            try:
                df = conn.sql(target_sql).df()
                csv_output = df.to_csv(index=False)
            except Exception as e:
                return ValidationResult(error=str(e), is_test_data_error=False, test_data_dump=test_data_dump)
        else:
            # It's DDL or DML
            try:
                conn.execute(target_sql)
            except Exception as e:
                return ValidationResult(error=str(e), is_test_data_error=False, test_data_dump=test_data_dump)
                
        return ValidationResult(error=None, is_test_data_error=False, test_data_dump=test_data_dump, csv_output=csv_output)
        
    except Exception as e:
        return ValidationResult(error=str(e), is_test_data_error=False, test_data_dump=test_data_dump)
    finally:
        if conn:
            conn.close()
