import re
from typing import Any


SQL_FUNCTION_RE = re.compile(r"\b([A-Za-z_][\w]*)\s*\(", re.IGNORECASE)
TABLE_RE = re.compile(
    r"\b(?:FROM|JOIN|INTO|UPDATE)\s+([A-Za-z_#\[][\w\].#\[\]]*)(?:\s+(?:AS\s+)?([A-Za-z_][\w]*))?",
    re.IGNORECASE,
)
JOIN_RE = re.compile(
    r"\b((?:LEFT|RIGHT|FULL|INNER|CROSS)\s+(?:OUTER\s+)?JOIN|JOIN)\s+([A-Za-z_#\[][\w\].#\[\]]*)(?:\s+(?:AS\s+)?([A-Za-z_][\w]*))?\s+ON\s+(.*?)(?=\b(?:LEFT|RIGHT|FULL|INNER|CROSS)?\s*JOIN\b|\bWHERE\b|\bGROUP\s+BY\b|\bHAVING\b|\bORDER\s+BY\b|$)",
    re.IGNORECASE | re.DOTALL,
)
CLAUSE_PATTERNS = {
    "select": r"\bSELECT\b\s+(.*?)\s+\bFROM\b",
    "where": r"\bWHERE\b\s+(.*?)(?=\bGROUP\s+BY\b|\bHAVING\b|\bORDER\s+BY\b|$)",
    "group_by": r"\bGROUP\s+BY\b\s+(.*?)(?=\bHAVING\b|\bORDER\s+BY\b|$)",
    "having": r"\bHAVING\b\s+(.*?)(?=\bORDER\s+BY\b|$)",
    "order_by": r"\bORDER\s+BY\b\s+(.*?)$",
}
AGGREGATIONS = {"SUM", "AVG", "COUNT", "MIN", "MAX", "STDDEV", "VARIANCE"}
KEYWORDS = {
    "AS", "CASE", "WHEN", "THEN", "ELSE", "END", "SELECT",
    "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "OVER", "PARTITION", "BY",
}
CONTROL_WORDS = {
    "AS", "CASE", "WHEN", "THEN", "ELSE", "END", "SELECT", "FROM", "WHERE",
    "GROUP", "HAVING", "ORDER", "OVER", "PARTITION", "BY", "ON", "JOIN",
}


def _clean_sql(sql: str) -> str:
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return " ".join(sql.split())


def _extract_clause(sql: str, name: str) -> str | None:
    match = re.search(CLAUSE_PATTERNS[name], sql, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    return match.group(1).strip().rstrip(";")


def _find_matching_paren(text: str, open_index: int) -> int | None:
    depth = 0
    quote = None
    i = open_index
    while i < len(text):
        char = text[i]
        if quote:
            if char == quote:
                if i + 1 < len(text) and text[i + 1] == quote:
                    i += 1
                else:
                    quote = None
        elif char in {"'", '"'}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return None


def _split_top_level(text: str, separator: str = ",") -> list[str]:
    parts = []
    start = 0
    depth = 0
    quote = None
    for i, char in enumerate(text):
        if quote:
            if char == quote:
                if i + 1 < len(text) and text[i + 1] == quote:
                    continue
                quote = None
        elif char in {"'", '"'}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif char == separator and depth == 0:
            parts.append(text[start:i].strip())
            start = i + 1
    tail = text[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def _function_calls(expression: str) -> list[dict[str, Any]]:
    calls = []
    i = 0
    while i < len(expression):
        match = re.search(r"\b([A-Za-z_][\w]*)\s*\(", expression[i:])
        if not match:
            break

        name = match.group(1)
        upper_name = name.upper()
        name_start = i + match.start(1)
        open_index = i + match.end(0) - 1
        close_index = _find_matching_paren(expression, open_index)
        if close_index is None:
            i = open_index + 1
            continue

        if upper_name in CONTROL_WORDS:
            i = open_index + 1
            continue

        argument_text = expression[open_index + 1:close_index]
        arguments = []
        nested_calls = []
        for argument in _split_top_level(argument_text):
            child_calls = _function_calls(argument)
            arguments.append({
                "expression": argument,
                "function_calls": child_calls,
            })
            nested_calls.extend(child_calls)

        calls.append({
            "function": upper_name,
            "expression": expression[name_start:close_index + 1].strip(),
            "arguments": arguments,
            "nested_functions": nested_calls,
            "evaluation_order": _evaluation_order(nested_calls) + [upper_name],
        })
        i = close_index + 1
    return calls


def _evaluation_order(calls: list[dict[str, Any]]) -> list[str]:
    order = []
    for call in calls:
        for function_name in call.get("evaluation_order", [call["function"]]):
            if function_name not in order:
                order.append(function_name)
    return order


def _top_level_as_index(expression: str) -> int | None:
    depth = 0
    quote = None
    matches = list(re.finditer(r"\s+AS\s+([A-Za-z_][\w]*)\s*$", expression, flags=re.IGNORECASE))
    if not matches:
        return None
    candidate = matches[-1]
    for i, char in enumerate(expression[:candidate.start()]):
        if quote:
            if char == quote:
                if i + 1 < len(expression) and expression[i + 1] == quote:
                    continue
                quote = None
        elif char in {"'", '"'}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
    return candidate.start() if depth == 0 else None


def _describe_expression(expression: str) -> dict[str, Any]:
    alias = None
    body = expression.strip()
    as_index = _top_level_as_index(body)
    if as_index is not None:
        alias_match = re.search(r"\s+AS\s+([A-Za-z_][\w]*)\s*$", body, flags=re.IGNORECASE)
        alias = alias_match.group(1) if alias_match else None
        body = body[:as_index].strip()

    calls = _function_calls(body)
    return {
        "expression": expression,
        "source_expression": body,
        "alias": alias,
        "function_calls": calls,
        "evaluation_order": _evaluation_order(calls),
    }


def _describe_clause_expressions(clause: str | None) -> list[dict[str, Any]]:
    if not clause:
        return []
    return [_describe_expression(part) for part in _split_top_level(clause)]


def _extract_top_limit(select_clause: str | None) -> tuple[str | None, str | None]:
    if not select_clause:
        return None, select_clause
    match = re.match(r"TOP\s+(?:\(([^)]+)\)|(\d+|[A-Za-z_][\w.]*))\s+(.*)$", select_clause, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None, select_clause
    return (match.group(1) or match.group(2)).strip(), match.group(3).strip()


def parse_sql(source_sql: str) -> dict[str, Any]:
    normalized = _clean_sql(source_sql)
    select_clause = _extract_clause(normalized, "select")
    row_limit, select_expressions_clause = _extract_top_limit(select_clause)
    where_clause = _extract_clause(normalized, "where")
    group_by_clause = _extract_clause(normalized, "group_by")
    having_clause = _extract_clause(normalized, "having")
    order_by_clause = _extract_clause(normalized, "order_by")
    tables = []
    seen_tables = set()
    aliases = {}

    for table, alias in TABLE_RE.findall(normalized):
        table_name = table.strip()
        table_key = table_name.lower()
        if table_key not in seen_tables:
            tables.append(table_name)
            seen_tables.add(table_key)
        if alias and alias.upper() not in KEYWORDS:
            aliases[alias] = table_name

    joins = []
    for join_type, table, alias, condition in JOIN_RE.findall(normalized):
        joins.append({
            "join_type": " ".join(join_type.upper().split()),
            "table": table.strip(),
            "alias": alias or None,
            "condition": condition.strip(),
        })

    function_calls = _function_calls(normalized)
    functions = []
    for function_name in _evaluation_order(function_calls):
        if function_name not in functions:
            functions.append(function_name)

    aggregations = [name for name in functions if name in AGGREGATIONS]
    clause_expressions = {
        "select": _describe_clause_expressions(select_expressions_clause),
        "where": _describe_clause_expressions(where_clause),
        "group_by": _describe_clause_expressions(group_by_clause),
        "having": _describe_clause_expressions(having_clause),
        "order_by": _describe_clause_expressions(order_by_clause),
    }

    return {
        "tables": tables,
        "aliases": aliases,
        "joins": joins,
        "filters": where_clause,
        "select": select_clause,
        "group_by": group_by_clause,
        "having": having_clause,
        "order_by": order_by_clause,
        "row_limit": row_limit,
        "functions": functions,
        "aggregations": aggregations,
        "function_calls": function_calls,
        "clause_expressions": clause_expressions,
    }
