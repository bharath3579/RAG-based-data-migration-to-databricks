import os
import sys
import shutil
import argparse
import getpass
import json
import pandas as pd

# Force unbuffered printing so all prints show up instantly in real-time
def print(*args, **kwargs):
    kwargs['flush'] = True
    import builtins
    builtins.print(*args, **kwargs)
from dotenv import load_dotenv

# Ensure the package is importable
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from parsers.sql_parser import parse_sql
from utils.file_utils import discover_files, read_text, relative_output_dir
from utils.report_utils import (
    append_summary,
    append_validation_report,
    build_databricks_notebook_py,
    build_databricks_notebook_scala,
)

OUTPUT_FOLDERS = {
    "test_cases": "test_cases",
    "parser_outputs": "parser_outputs",
    "summary_outputs": "summary_outputs",
    "final_result": "final_result",
    "validation_excels": "validation_excels",
    "summarizer_excels": "summarizer_excels",
}

def cleanup_dirs(dirs):
    for d in dirs:
        if os.path.exists(d):
            print(f"Cleaning up {d}...")
            try:
                shutil.rmtree(d)
            except Exception as e:
                print(f'Failed to delete {d}. Reason: {e}')

def prompt_if_missing(var_name: str, secret: bool = False) -> str:
    value = os.getenv(var_name)
    if value:
        return value
    prompt = f"Enter {var_name}: "
    value = getpass.getpass(prompt) if secret else input(prompt).strip()
    os.environ[var_name] = value
    return value

def ensure_connection_env():
    required = [
        ("DATABRICKS_HOST", False),
        ("DATABRICKS_TOKEN", True),
        ("DATABRICKS_HTTP_PATH", False),
        ("SQL_SERVER_HOST", False),
        ("SQL_SERVER_DB", False),
    ]
    print("Checking validation connection configuration...")
    for key, secret in required:
        prompt_if_missing(key, secret=secret)

def ensure_llm_env():
    required = [
        ("DATABRICKS_HOST", False),
        ("DATABRICKS_TOKEN", True),
    ]
    print("Checking LLM endpoint configuration...")
    for key, secret in required:
        prompt_if_missing(key, secret=secret)

def output_category_dir(output_base_dir: str, category: str) -> str:
    return os.path.join(output_base_dir, OUTPUT_FOLDERS[category])

def mirrored_output_file(input_dir: str, output_base_dir: str, category: str, filepath, suffix: str):
    out_dir = relative_output_dir(input_dir, output_category_dir(output_base_dir, category), filepath)
    os.makedirs(out_dir, exist_ok=True)
    return out_dir / f"{filepath.stem}{suffix}"

def write_json(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)



def build_summary_payload(rel_input_path: str, source_dialect: str, target_dialect: str, parser_output: dict, code_summary: dict | None, error: str | None = None):
    code_summary = code_summary or {}
    return {
        "file_path": rel_input_path,
        "source_platform": source_dialect,
        "target_platform": target_dialect,
        "language": "sql",
        "tables_and_schema": code_summary.get("tables_and_schema") or {
            "tables": parser_output.get("tables", []),
            "aliases": parser_output.get("aliases", {}),
        },
        "functions": code_summary.get("functions") or parser_output.get("functions", []),
        "technical_summary": code_summary.get("technical_summary", ""),
        "summary_error": error,
    }

def write_summary_excel(path: str, rows: list[dict]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_excel(path, index=False)

def main():
    parser = argparse.ArgumentParser(description="Agentic SQL Converter")
    parser.add_argument("--interactive", action="store_true", help="Launch interactive CLI mode")
    parser.add_argument("--source-platform", help="Source platform for all input files, e.g. SQL Server or Azure Synapse")
    parser.add_argument("--input-dir", help="Input directory containing SQL or PySpark files")
    parser.add_argument(
        "--run",
        choices=["parser", "summary", "convert", "full"],
        help="parser = parse only, summary = parse and summarize only, convert = translate without validation, full = convert, test, and validate",
    )
    parser.add_argument("--output-lang", choices=["py", "pyspark", "sql", "scala"], help="Output format for the final result (py, pyspark, sql, scala)")
    parser.add_argument("--skip-validation-config-prompt", action="store_true", help="Do not prompt for missing validation connection values")
    args = parser.parse_args()

    if args.interactive:
        import interactive_cli
        interactive_cli.interactive_loop()
        return

    load_dotenv()
    
    input_dir = args.input_dir
    while not input_dir:
        input_dir = input("Enter input directory path (e.g., input_sql): ").strip()

    output_base_dir = "output"
    validation_report_dir = output_category_dir(output_base_dir, "validation_excels")
    summarizer_report_dir = output_category_dir(output_base_dir, "summarizer_excels")
    target_dialect = "Databricks Unity Catalog SQL"
    
    source_dialect = args.source_platform or os.getenv("SOURCE_PLATFORM") or os.getenv("SOURCE_DIALECT")
    while not source_dialect:
        print("\nSelect source platform for SQL files:")
        print("1. SQL Server (sql files)")
        print("2. Azure Synapse (sql, scala, python)")
        print("3. Others (Single-Engine Validation)")
        choice = input("Enter choice (1-3, or type name) [1]: ").strip()
        if not choice or choice == "1" or choice.lower() == "sql server":
            source_dialect = "SQL Server"
        elif choice == "2" or choice.lower() == "azure synapse":
            source_dialect = "Azure Synapse"
        elif choice == "3" or choice.lower() == "others":
            source_dialect = "Others"
        else:
            source_dialect = choice # Allow custom typing
    os.environ["SOURCE_DIALECT"] = source_dialect

    output_lang = args.output_lang
    while not output_lang or output_lang not in ["py", "pyspark", "sql", "scala"]:
        print("\nSelect output language format:")
        print("1. sql     - Raw SQL file")
        print("2. pyspark - Native PySpark DataFrame code")
        print("3. py      - Databricks Notebook Python wrapper (spark.sql)")
        print("4. scala   - Databricks Notebook Scala wrapper")
        choice = input("Enter choice (1-4, or type name) [1]: ").strip().lower()
        if not choice or choice in ("1", "sql"):
            output_lang = "sql"
        elif choice in ("2", "pyspark"):
            output_lang = "pyspark"
        elif choice in ("3", "py"):
            output_lang = "py"
        elif choice in ("4", "scala"):
            output_lang = "scala"
        else:
            print("Invalid choice. Please try again.")
    args.output_lang = output_lang

    run_mode = args.run
    while not run_mode or run_mode not in ["parser", "summary", "convert", "full"]:
        print("\nSelect how far you want to run the pipeline:")
        print("1. parser  - Parse only (AST extraction)")
        print("2. summary - Parse and summarize only")
        print("3. convert - Translate without validation")
        print("4. full    - Convert, test, and validate (end-to-end)")
        choice = input("Enter choice (1-4, or type name) [4]: ").strip().lower()
        
        if not choice or choice in ("4", "full"):
            run_mode = "full"
        elif choice in ("1", "parser"):
            run_mode = "parser"
        elif choice in ("2", "summary"):
            run_mode = "summary"
        elif choice in ("3", "convert"):
            run_mode = "convert"
        else:
            print("Invalid choice. Please try again.")
    args.run = run_mode

    if args.run in ("summary", "convert"):
        ensure_llm_env()
    elif args.run == "full" and not args.skip_validation_config_prompt:
        ensure_connection_env()
    
    # Cleanup for a fresh run
    cleanup_dirs([output_base_dir])
    os.makedirs(output_base_dir, exist_ok=True)
    for folder_name in OUTPUT_FOLDERS.values():
        os.makedirs(os.path.join(output_base_dir, folder_name), exist_ok=True)

    if not os.path.exists(input_dir):
        os.makedirs(input_dir)
        print(f"Created '{input_dir}' directory.")
        return

    results = []
    summary_rows = []

    input_base_dir = input_dir if os.path.isdir(input_dir) else os.path.dirname(input_dir)

    for filepath in discover_files(input_dir, {".sql", ".py", ".ipynb"}):
        item_id = filepath.stem
        input_language = "pyspark" if filepath.suffix.lower() in (".py", ".ipynb") else "sql"
        print(f"\nProcessing: {item_id} (Source Platform: {source_dialect}, Input Lang: {input_language})")

        source_sql = read_text(filepath)
        rel_input_path = str(filepath)
        if input_language == "pyspark":
            from parsers.pyspark_parser import parse_pyspark
            parser_output = parse_pyspark(source_sql)
        else:
            parser_output = parse_sql(source_sql)
        parser_output_path = mirrored_output_file(input_base_dir, output_base_dir, "parser_outputs", filepath, ".json")
        write_json(parser_output_path, parser_output)
        print(f"    - Saved parser output to: {parser_output_path}")

        if args.run == "parser":
            continue

        if args.run == "summary":
            code_summary = None
            summary_error = None
            try:
                if input_language == "pyspark":
                    from src.agent import summarize_pyspark
                    code_summary = summarize_pyspark(
                        source_code=source_sql,
                        parser_output=parser_output,
                        source_platform=source_dialect,
                        target_platform=target_dialect,
                    ).model_dump()
                else:
                    from src.agent import summarize_sql
                    code_summary = summarize_sql(
                        source_sql=source_sql,
                        parser_output=parser_output,
                        source_platform=source_dialect,
                        target_platform=target_dialect,
                    ).model_dump()
            except Exception as e:
                summary_error = f"AI Summary Error: {e}"
                print(f"    - [WARN] {summary_error}")

            summary_payload = build_summary_payload(
                rel_input_path,
                source_dialect,
                target_dialect,
                parser_output,
                code_summary,
                summary_error,
            )
            summary_output_path = mirrored_output_file(input_base_dir, output_base_dir, "summary_outputs", filepath, ".json")
            write_json(summary_output_path, summary_payload)
            print(f"    - Saved summary output to: {summary_output_path}")
            raw_tables = summary_payload.get("tables_and_schema", {}).get("tables", [])
            extracted_tables = []
            if isinstance(raw_tables, list):
                for t in raw_tables:
                    if isinstance(t, str):
                        extracted_tables.append(t)
                    elif isinstance(t, dict):
                        extracted_tables.append(t.get("name", str(t)))
                    else:
                        extracted_tables.append(str(t))
            
            summary_rows.append({
                "file_path": rel_input_path,
                "source_platform": source_dialect,
                "target_platform": target_dialect,
                "tables": ", ".join(extracted_tables),
                "functions": ", ".join(summary_payload.get("functions", [])),
                "technical_summary": summary_payload.get("technical_summary", ""),
                "summary_error": summary_error,
            })
            continue

        test_cases_dir = relative_output_dir(input_base_dir, output_category_dir(output_base_dir, "test_cases"), filepath)
        os.makedirs(test_cases_dir, exist_ok=True)
        from agents.conversion_agent import convert_file

        result = convert_file(
            source_file=str(filepath),
            source_code=source_sql,
            source_platform=source_dialect,
            target_platform=target_dialect,
            input_language=input_language,
            output_language=args.output_lang,
            test_cases_output_dir=str(test_cases_dir),
            skip_validation=(args.run == "convert"),
        )
        results.append(result)

        summary_payload = build_summary_payload(
            rel_input_path,
            source_dialect,
            target_dialect,
            result.parser_output or parser_output,
            result.code_summary,
            result.ai_summary if result.ai_summary and not result.code_summary else None,
        )
        summary_output_path = mirrored_output_file(input_base_dir, output_base_dir, "summary_outputs", filepath, ".json")
        write_json(summary_output_path, summary_payload)
        print(f"    - Saved summary output to: {summary_output_path}")
        summary_rows.append({
            "file_path": rel_input_path,
            "source_platform": source_dialect,
            "target_platform": target_dialect,
            "tables": ", ".join(summary_payload.get("tables_and_schema", {}).get("tables", [])),
            "functions": ", ".join(summary_payload.get("functions", [])),
            "technical_summary": summary_payload.get("technical_summary", ""),
            "summary_error": summary_payload.get("summary_error"),
        })

        if result.final_mapped_sql:
            ext = ".py" if args.output_lang in ("py", "pyspark") else f".{args.output_lang}"
            output_file_path = mirrored_output_file(input_base_dir, output_base_dir, "final_result", filepath, ext)
            with open(output_file_path, 'w', encoding='utf-8') as f:
                if args.output_lang == "py":
                    f.write(build_databricks_notebook_py(result.final_mapped_sql))
                elif args.output_lang == "scala":
                    f.write(build_databricks_notebook_scala(result.final_mapped_sql))
                else:
                    f.write(result.final_mapped_sql)
            print(f"    - Saved translated output to: {output_file_path}")
            print("\nConverted SQL:")
            print("-" * 40)
            print(result.final_mapped_sql)
            print("-" * 40)

        if result.details:
            details_path = mirrored_output_file(input_base_dir, output_base_dir, "validation_excels", filepath, "_validation_details.txt")
            with open(details_path, 'w', encoding='utf-8') as f:
                f.write(result.details)

        append_summary(
            os.path.join(output_category_dir(output_base_dir, "summary_outputs"), "summary.json"),
            result,
            rel_input_path,
            source_dialect,
        )
        append_validation_report(
            os.path.join(output_category_dir(output_base_dir, "validation_excels"), "validation_report.csv"),
            rel_input_path,
            result,
        )

    if summary_rows:
        summary_excel_path = os.path.join(summarizer_report_dir, "summarizer_report.xlsx")
        write_summary_excel(summary_excel_path, summary_rows)
        print(f"\nSummarizer Excel written to: {summary_excel_path}")

    if args.run != "full":
        print(f"\nExecution Complete! Run mode: {args.run}. Outputs are under '{output_base_dir}'.")
        return

    print(f"\nAll items processed. Generating validation report in '{validation_report_dir}'...")
    from src.reporter import Reporter

    reporter = Reporter(validation_report_dir)
    md_report_path = reporter.generate_report(results)
    
    print(f"\nExecution Complete! Report: {md_report_path}")

if __name__ == "__main__":
    main()
