import ast
from typing import Any


def parse_python(source_code: str) -> dict[str, Any]:
    tree = ast.parse(source_code)
    imports = []
    classes = []
    functions = []
    spark_operations = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            imports.extend(f"{module}.{alias.name}".strip(".") for alias in node.names)
        elif isinstance(node, ast.ClassDef):
            classes.append(node.name)
        elif isinstance(node, ast.FunctionDef):
            functions.append(node.name)
        elif isinstance(node, ast.Attribute) and node.attr in {"sql", "table", "read", "write", "select", "filter", "join", "groupBy", "agg"}:
            spark_operations.append(node.attr)

    return {
        "imports": sorted(set(imports)),
        "classes": classes,
        "functions": functions,
        "spark_operations": sorted(set(spark_operations)),
    }
