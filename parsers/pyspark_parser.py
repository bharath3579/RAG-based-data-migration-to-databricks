import re
import ast
from typing import Any

def _extract_cells(code: str) -> list[dict[str, Any]]:
    # Split by Databricks `# COMMAND ----------` or Jupyter/Synapse `# %%`
    cells = []
    pattern = re.compile(r'^(#\s*COMMAND\s*-+|^#\s*%%)\s*$', re.MULTILINE)
    
    start_idx = 0
    cell_idx = 1
    
    for match in pattern.finditer(code):
        end_idx = match.start()
        cell_code = code[start_idx:end_idx].strip()
        if cell_code:
            cells.append({"cell_index": cell_idx, "code": cell_code})
            cell_idx += 1
        start_idx = match.end()
        
    final_cell_code = code[start_idx:].strip()
    if final_cell_code:
        cells.append({"cell_index": cell_idx, "code": final_cell_code})
        
    # If no cells found, treat whole file as one cell
    if not cells:
        cells.append({"cell_index": 1, "code": code.strip()})
        
    return cells

class PySparkVisitor(ast.NodeVisitor):
    def __init__(self, known_dfs: set):
        self.functions = set()
        self.dataframes = set()
        self.variables = set()
        self.imports = set()
        self.known_dfs = known_dfs
        
    def visit_Import(self, node):
        for alias in node.names:
            self.imports.add(alias.name)
        self.generic_visit(node)
        
    def visit_ImportFrom(self, node):
        module = node.module or ''
        for alias in node.names:
            self.imports.add(f"{module}.{alias.name}")
        self.generic_visit(node)

    def visit_Call(self, node):
        if isinstance(node.func, ast.Attribute):
            func_name = node.func.attr
            self.functions.add(func_name)
        elif isinstance(node.func, ast.Name):
            self.functions.add(node.func.id)
            
        self.generic_visit(node)
        
    def is_dataframe_expression(self, node) -> bool:
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                if isinstance(node.func.value, ast.Name) and node.func.value.id == "spark":
                    return True
                if isinstance(node.func.value, ast.Name) and node.func.value.id in self.known_dfs:
                    return True
                if self.is_dataframe_expression(node.func.value):
                    return True
        elif isinstance(node, ast.Attribute):
            return self.is_dataframe_expression(node.value)
        return False

    def visit_Assign(self, node):
        is_df_assignment = self.is_dataframe_expression(node.value)
        
        for target in node.targets:
            if isinstance(target, ast.Name):
                var_name = target.id
                if is_df_assignment or var_name.endswith('_df') or var_name.endswith('Df') or var_name.startswith('df'):
                    self.dataframes.add(var_name)
                    self.known_dfs.add(var_name)
                else:
                    self.variables.add(var_name)
        self.generic_visit(node)

def parse_pyspark(source_code: str) -> dict[str, Any]:
    cells = _extract_cells(source_code)
    
    all_functions = set()
    all_dfs = set()
    all_variables = set()
    all_imports = set()
    all_magics = set()
    
    global_known_dfs = set()
    
    parsed_cells = []
    
    for cell in cells:
        cell_code = cell["code"]
        visitor = PySparkVisitor(global_known_dfs)
        try:
            tree = ast.parse(cell_code)
            visitor.visit(tree)
        except SyntaxError:
            pass
            
        # Also extract magic commands (e.g. %run, %sql)
        magic_matches = re.findall(r"^%([a-zA-Z_]\w*)", cell_code, re.MULTILINE)
        
        cell_functions = list(visitor.functions)
        cell_dfs = list(visitor.dataframes)
        cell_variables = list(visitor.variables)
        cell_imports = list(visitor.imports)
        cell_magics = list(set(magic_matches))
        
        all_functions.update(cell_functions)
        all_dfs.update(cell_dfs)
        all_variables.update(cell_variables)
        all_imports.update(cell_imports)
        all_magics.update(cell_magics)
        
        parsed_cells.append({
            "cell_index": cell["cell_index"],
            "imports": cell_imports,
            "variables": cell_variables,
            "dataframes": cell_dfs,
            "functions": cell_functions,
            "magic_commands": cell_magics,
            "code_snippet": cell_code
        })
        
    # Add regex fallback for functions if ast failed on some cells
    regex_functions = set(re.findall(r"\.([a-zA-Z_]\w*)\s*\(", source_code))
    all_functions.update(regex_functions)
    
    # Filter out common non-pyspark python builtins to keep the keyword list clean
    ignore_funcs = {"print", "len", "format", "append", "extend", "isinstance", "int", "str"}
    clean_functions = [f for f in all_functions if f not in ignore_funcs]

    return {
        "cells": parsed_cells,
        "imports": list(all_imports),
        "variables": list(all_variables),
        "dataframes": list(all_dfs),
        "functions": clean_functions,
        "magic_commands": list(all_magics),
        "total_cells": len(cells)
    }
