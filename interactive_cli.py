import os
import sys
from dotenv import load_dotenv

# Ensure the package is importable
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.converter import process_file

def get_multiline_input(prompt):
    print(prompt)
    print("(Type '!!!' on a new line to finish, or 'exit' to quit)")
    lines = []
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        stripped = line.strip()
        if stripped.lower() == 'exit':
            sys.exit(0)
        if stripped == '!!!':
            break
        lines.append(line)
    return "".join(lines)

def interactive_loop():
    load_dotenv()
    
    print("=== Agentic SQL Converter: Interactive Mode ===")
    default_dialect = os.getenv("SOURCE_DIALECT", "SQL Server")
    source_dialect = input(f"Enter Source Dialect (e.g., Oracle, Synapse, SQL Server) [{default_dialect}]: ").strip() or default_dialect
    input_language = input("Enter Input Language (sql or pyspark) [sql]: ").strip().lower() or "sql"
    output_language = input("Enter Output Language (sql or pyspark) [sql]: ").strip().lower() or "sql"
    target_dialect = "Databricks Unity Catalog SQL"
    
    while True:
        source_sql = get_multiline_input(f"\nPaste your source {input_language.upper()} code below:")
        if not source_sql.strip():
            continue
            
        user_feedback = None
        while True:
            print("\n--- Translating... ---")
            result = process_file(
                source_file="manual_input",
                source_code=source_sql,
                source_dialect=source_dialect,
                target_dialect=target_dialect,
                input_language=input_language,
                output_language=output_language,
                user_feedback=user_feedback
            )
            
            print("\n" + "="*50)
            if result.success:
                print("✅ SUCCESSFUL CONVERSION")
                print("="*50)
                print(result.final_mapped_sql)
            else:
                print("❌ CONVERSION FAILED")
                print("="*50)
                if result.final_mapped_sql:
                    print("Last Attempt:")
                    print(result.final_mapped_sql)
                print("\nError:")
                print(result.errorMessage)
                print("\nAI Summary:")
                print(result.ai_summary)
            print("="*50)
            
            choice = input("\n[A]pprove & Save, [F]eedback & Retry, [N]ew Query, [Q]uit: ").strip().lower()
            
            if choice == 'a':
                # Save to input_sql/manual/
                save_dir = os.path.join("input_sql", "manual")
                os.makedirs(save_dir, exist_ok=True)
                from datetime import datetime
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                ext = "py" if output_language == "pyspark" else "sql"
                filename = f"manual_{ts}.{ext}"
                with open(os.path.join(save_dir, filename), 'w', encoding='utf-8') as f:
                    f.write(result.final_mapped_sql if result.final_mapped_sql else "-- Failed Conversion\n" + source_sql)
                print(f"Saved to {os.path.join(save_dir, filename)}")
                break
            elif choice == 'f':
                user_feedback = input("Enter your feedback/instructions: ").strip()
                continue
            elif choice == 'n':
                break
            elif choice == 'q':
                return

if __name__ == "__main__":
    interactive_loop()
