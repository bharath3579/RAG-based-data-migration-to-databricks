import csv
import json
import os


def build_databricks_notebook_py(databricks_sql: str) -> str:
    safe_sql = databricks_sql.replace('"""', '\\"\\"\\"').strip()
    return (
        "# Databricks notebook source\n"
        "# Converted SQL for Databricks Latest DBR\n\n"
        "converted_df = spark.sql(\"\"\"\n"
        f"{safe_sql}\n"
        "\"\"\")\n\n"
        "display(converted_df)\n"
    )

def build_databricks_notebook_scala(databricks_sql: str) -> str:
    safe_sql = databricks_sql.replace('"""', '\\"\\"\\"').strip()
    return (
        "// Databricks notebook source\n"
        "// Converted SQL for Databricks Latest DBR\n\n"
        "val converted_df = spark.sql(\"\"\"\n"
        f"{safe_sql}\n"
        "\"\"\")\n\n"
        "display(converted_df)\n"
    )


def append_summary(summary_path: str, result, rel_input_path: str, source_platform: str):
    os.makedirs(os.path.dirname(summary_path), exist_ok=True)
    if os.path.exists(summary_path):
        with open(summary_path, "r", encoding="utf-8") as f:
            summary = json.load(f)
    else:
        summary = {"files": []}

    code_summary = result.code_summary or {}
    functions = code_summary.get("functions") or (result.parser_output or {}).get("functions", [])
    summary["files"].append({
        "file_path": rel_input_path,
        "source_platform": source_platform,
        "target_platform": "databricks",
        "language": "sql",
        "tables_and_schema": code_summary.get("tables_and_schema") or {
            "tables": (result.parser_output or {}).get("tables", []),
            "aliases": (result.parser_output or {}).get("aliases", {}),
        },
        "functions": functions,
        "technical_summary": code_summary.get("technical_summary") or result.ai_summary or "",
        "conversion_status": "SUCCESS" if result.final_mapped_sql else "FAILED",
        "validation_status": "PASS" if result.success else ("FAIL" if result.final_mapped_sql else "NOT_EXECUTED"),
        "total_tokens": result.total_tokens,
        "retries": result.retries,
        "metrics": result.metrics,
        "error_message": result.errorMessage,
        "details": result.details,
    })

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


def append_validation_report(report_path: str, rel_input_path: str, result):
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    file_exists = os.path.exists(report_path)
    conversion_status = "SUCCESS" if result.final_mapped_sql else "FAILED"
    validation_status = "PASS" if result.success else ("FAIL" if result.final_mapped_sql else "NOT_EXECUTED")
    details = result.details or result.errorMessage or result.ai_summary or ""

    with open(report_path, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["file_path", "conversion_status", "validation_status", "total_tokens", "retries", "details"])
        writer.writerow([rel_input_path, conversion_status, validation_status, result.total_tokens, result.retries, details])
