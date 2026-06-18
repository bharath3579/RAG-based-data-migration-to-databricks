from src.agent import summarize_sql, summarize_pyspark

def generate_summary(source_code: str, parser_output: dict, source_platform: str, target_platform: str, input_language: str = "sql"):
    if input_language == "sql":
        return summarize_sql(
            source_sql=source_code,
            parser_output=parser_output,
            source_platform=source_platform,
            target_platform=target_platform,
        )
    elif input_language == "pyspark":
        return summarize_pyspark(
            source_code=source_code,
            parser_output=parser_output,
            source_platform=source_platform,
            target_platform=target_platform,
        )
    else:
        raise ValueError(f"Unsupported input language: {input_language}")
