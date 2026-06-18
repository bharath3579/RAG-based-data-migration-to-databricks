import re
from typing import Any


def parse_scala(source_code: str) -> dict[str, Any]:
    imports = re.findall(r"(?m)^\s*import\s+([^\n]+)", source_code)
    classes = re.findall(r"\bclass\s+([A-Za-z_][\w]*)", source_code)
    functions = re.findall(r"\bdef\s+([A-Za-z_][\w]*)\s*\(", source_code)
    spark_transformations = re.findall(
        r"\.(select|filter|where|join|groupBy|agg|withColumn|orderBy|sql|table)\b",
        source_code,
    )
    return {
        "imports": imports,
        "classes": classes,
        "functions": functions,
        "spark_transformations": sorted(set(spark_transformations)),
    }
