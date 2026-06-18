import os
import pandas as pd
from .converter import ConversionResult
from datetime import datetime

class Reporter:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.excel_file = os.path.join(self.output_dir, f"report_{timestamp}.xlsx")
        self.text_file = os.path.join(self.output_dir, f"validation_report_{timestamp}.txt")
        
    def generate_report(self, results: list[ConversionResult]) -> str:
        self._generate_excel(results)
        self._generate_text(results)
        return self.excel_file
        
    def _generate_excel(self, results: list[ConversionResult]):
        data = []
        for r in results:
            migrated = "Y" if not r.errorMessage or "Syntax Error" not in r.errorMessage else "N"
            final_res = "PASS" if (r.success and migrated == "Y") else "FAIL"
            
            data.append({
                "SQL Server Table Name": r.sql_server_table_name,
                "Databricks Table Name": r.databricks_table_name,
                "Migrated (Y/N)": migrated,
                "Test Case 1 Result (PASS/FAIL)": "PASS" if r.pass_case_1 else "FAIL",
                "Test Case 2 Result (PASS/FAIL)": "PASS" if r.pass_case_2 else "FAIL",
                "Test Case 3 Result (PASS/FAIL)": "PASS" if r.pass_case_3 else "FAIL",
                "Final Result": final_res
            })
            
        df = pd.DataFrame(data)
        df.to_excel(self.excel_file, index=False)

    def _generate_text(self, results: list[ConversionResult]):
        with open(self.text_file, mode='w', encoding='utf-8') as f:
            f.write("=== DUAL ENGINE VALIDATION REPORT ===\n")
            f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("Normalization Assumptions:\n")
            f.write("- All columns explicitly CAST to STRING.\n")
            f.write("- Whitespace TRIMMED.\n")
            f.write("- NULL values and empty strings converted to '__NULL__'.\n\n")
            f.write("======================================\n\n")
            
            for r in results:
                f.write(f"TABLE: {r.sql_server_table_name} -> {r.databricks_table_name}\n")
                if r.success:
                    f.write("STATUS: SUCCESS (All validations passed)\n")
                else:
                    f.write("STATUS: FAILED\n")
                    f.write("EXPLANATIONS:\n")
                    if r.errorMessage:
                        f.write(f"  - Error: {r.errorMessage}\n")
                    
                    if r.details:
                        f.write(f"  - Validation Details:\n{r.details}\n")
                        
                    if r.ai_summary:
                        f.write(f"  - AI Root Cause Analysis:\n{r.ai_summary}\n")
                
                f.write("-" * 40 + "\n\n")
