from src.dual_validator import validate_dual_engine


def validate_conversion(test_databricks_sql, test_source_sql, test_cases):
    return validate_dual_engine(test_databricks_sql, test_source_sql, test_cases)
