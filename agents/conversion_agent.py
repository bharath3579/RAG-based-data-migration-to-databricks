from src.converter import process_file

def convert_file(
    source_file: str,
    source_code: str,
    source_platform: str,
    target_platform: str,
    input_language: str = "sql",
    output_language: str = "sql",
    test_cases_output_dir: str | None = None,
    skip_validation: bool = False,
):
    return process_file(
        source_file=source_file,
        source_code=source_code,
        source_dialect=source_platform,
        target_dialect=target_platform,
        input_language=input_language,
        output_language=output_language,
        test_cases_output_dir=test_cases_output_dir,
        skip_validation=skip_validation,
    )
