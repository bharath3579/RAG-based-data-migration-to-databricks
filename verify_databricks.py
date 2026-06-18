import os
import sys
from dotenv import load_dotenv

# Ensure the package is importable
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.converter import process_file

def verify_databricks_connection():
    load_dotenv()
    print("=== Databricks Live Validation Verification ===")
    
    # Test a simple Oracle -> Databricks conversion
    source_sql = "CREATE TABLE employees (id NUMBER PRIMARY KEY, name VARCHAR2(100), hire_date DATE)"
    source_dialect = "Oracle"
    target_dialect = "Databricks Unity Catalog SQL"
    
    print(f"Testing conversion and LIVE validation on Databricks...")
    try:
        result = process_file(
            source_file="verify_test.sql",
            source_sql=source_sql,
            source_dialect=source_dialect,
            target_dialect=target_dialect
        )
        
        if result.success:
            print("\nSUCCESS: VERIFICATION SUCCESSFUL!")
            print(f"Final SQL produced:\n{result.final_sql}")
        else:
            print("\nERROR: VERIFICATION FAILED")
            print(f"Error: {result.errorMessage}")
            if result.ai_summary:
                print(f"AI Summary: {result.ai_summary}")
    except Exception as e:
        print(f"\nFATAL ERROR: {str(e)}")

if __name__ == "__main__":
    verify_databricks_connection()
