"""
Error handling utilities for the Data Lakehouse Ingest framework.
Provides helper functions to generate structured error entries
for inclusion in ingestion reports and logs.

This module standardizes how exceptions are represented during
table-level processing, ensuring consistent error reporting across
all stages of the ingestion pipeline.
"""

def error_entry_for_exception(table: dict, exc: Exception) -> dict[str, str]:
    """
    Create a standardized error entry dictionary for a failed table operation.

    Converts an exception into a serializable error record suitable for
    inclusion in the ingestion report or structured log output. Ensures
    consistent representation of failure details across all ingestion steps.

    Args:
        table (dict): Table configuration dictionary from the ingestion config.
            Expected to contain a "name" field identifying the table.
        exc (Exception): The caught exception instance representing the failure.

    Returns:
        dict[str, str]: A dictionary containing:
            - "name": Table name (or "<unknown>" if missing).
            - "error": Exception message as a string.
            - "status": Always set to "failed".

    Example:
        >>> try:
        ...     raise ValueError("Missing required column")
        ... except Exception as e:
        ...     error_entry_for_exception({"name": "gene_table"}, e)
        {'name': 'gene_table', 'error': 'Missing required column', 'status': 'failed'}

    Notes:
        - Keeps error serialization minimal and human-readable.
        - Does not include stack traces; those are logged separately.
        - Used in orchestration layers to populate both `error_list` and
          table-level entries in the final ingestion report.
    """
    name = table.get("name", "<unknown>")
    return {"name": name, "error": str(exc), "status": "failed"}
