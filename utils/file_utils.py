import os
from pathlib import Path


SUPPORTED_EXTENSIONS = {".sql", ".py", ".scala"}


def discover_files(input_dir: str, supported_extensions: set[str] | None = None) -> list[Path]:
    extensions = supported_extensions or SUPPORTED_EXTENSIONS
    root = Path(input_dir)
    if not root.exists():
        return []
    if root.is_file():
        if root.suffix.lower() in extensions:
            return [root]
        return []

    return [
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in extensions
    ]


def relative_output_dir(input_dir: str, output_dir: str, source_file: Path) -> Path:
    rel_parent = source_file.parent.relative_to(input_dir)
    return Path(output_dir) / rel_parent


import json

def read_text(path: Path) -> str:
    if path.suffix.lower() == ".ipynb":
        content = path.read_text(encoding="utf-8")
        try:
            nb = json.loads(content)
            cells = nb.get("cells", [])
            code_blocks = []
            for cell in cells:
                if cell.get("cell_type") == "code":
                    source = cell.get("source", [])
                    if isinstance(source, list):
                        code_blocks.append("".join(source))
                    else:
                        code_blocks.append(str(source))
            # Stitch cells together using Databricks notebook separators
            return "\n\n# COMMAND ----------\n\n".join(code_blocks)
        except Exception as e:
            print(f"Warning: Failed to parse .ipynb {path}: {e}")
            return content
    return path.read_text(encoding="utf-8")


def ensure_dir(path: str | Path):
    os.makedirs(path, exist_ok=True)
