"""
Batch table processing utilities for the Data Lakehouse Ingest framework.

This module provides the `process_tables()` function, which coordinates the
end-to-end processing of all tables defined in the ingestion context. It acts
as a higher-level orchestrator above the per-table `process_table()` function,
managing logging context, error capture, and aggregation of table-level
reports.

Responsibilities:
    - Iterate through configured tables in the ingestion run.
    - Apply dynamic logger context so logs include table identifiers.
    - Delegate per-table work to `process_table()`.
    - Capture and normalize any exceptions encountered during table processing.
    - Produce two lists: successful/failed table reports and top-level errors.

This module improves modularity by separating multi-table orchestration from
the top-level `ingest()` function, resulting in cleaner orchestration flow.
"""

from typing import Any
from .error_utils import error_entry_for_exception
from .table_processor import process_table
from .models import ProcessStatus
import logging
from pyspark.sql import SparkSession, DataFrame
from minio import Minio
from dataclasses import asdict


def process_tables(
    spark: SparkSession,
    logger: logging.Logger,
    loader: Any,
    ctx: dict,
    started_at: str,
    minio_client: Minio | None = None,
    dataframes: dict[str, DataFrame] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Process all tables defined in the ingestion context.

    Iterates through the list of tables extracted from the ingestion context
    (`ctx["tables"]`), applies per-table logging context, and delegates table-
    level ingestion work to `process_table()`. All exceptions raised during
    table processing are captured and converted into standardized error
    entries using `error_entry_for_exception()`.

    Optionally supports DataFrame overrides for individual tables. When
    provided, a DataFrame mapped to the table's name is passed to
    `process_table()` instead of reading the table from the configured
    Bronze source.

    Args:
        spark (SparkSession):
            Active Spark session used for reading/writing data.
        logger (logging.Logger):
            Structured logger used for emitting JSON-formatted logs.
        loader (Any):
            The configuration loader containing resolved configuration and
            schema information.
        ctx (dict):
            Ingestion context produced by `init_run_context()`. Must include
            a `"tables"` key containing the list of table definitions.
        started_at (str):
            ISO-8601 timestamp marking when the ingestion run began.
        minio_client (Minio | None, optional):
            MinIO client used for reading data from S3-compatible sources.
            Passed through to `process_table()`.
        dataframes (dict[str, DataFrame] | None, optional):
            Optional mapping of table names to pre-loaded Spark DataFrames.
            If a table name exists in this dictionary, the corresponding
            DataFrame will be passed to `process_table()` as an override
            (`df_override`) instead of loading the table from Bronze storage.

    Returns:
        tuple[list[dict[str, Any]], list[dict[str, Any]]]:
            A tuple containing:
                - `table_reports`: List of per-table result dictionaries,
                  each containing status, metrics, and, for failures, diagnostic
                  details such as error message and optional traceback.
                - `error_list`: List of normalized error entries for all
                  tables that encountered exceptions.

    Notes:
        - Each table is processed independently; a failure in one table does
          not prevent others from being ingested.
        - When `dataframes` is provided, the DataFrame associated with the
          table name will override the default data-loading behavior in
          `process_table()`.
        - DataFrame overrides enable notebook-based ingestion, testing, or
          upstream pipelines that already produce Spark DataFrames.
        - The returned lists are used by the top-level orchestrator to build
          the final ingestion run report.
    """
    tables = ctx["tables"]
    table_reports: list[dict[str, Any]] = []
    error_list: list[dict[str, Any]] = []

    for table in tables:
        try:
            report_row = process_table(
                spark=spark,
                logger=logger,
                loader=loader,
                ctx=ctx,
                table=table,
                run_started_at_iso=started_at,
                minio_client=minio_client,
                df_override=(dataframes or {}).get(table.get("name", "")),
            )

            report_dict = asdict(report_row)
            table_reports.append(report_dict)

            if report_row.status == ProcessStatus.FAILED:
                error_entry = {
                    "phase": report_dict.get("phase", "table_processing"),
                    "table": report_dict.get("name"),
                    "error": report_dict.get("error", "Unknown table processing error"),
                }

                if report_dict.get("traceback"):
                    error_entry["traceback"] = report_dict["traceback"]

                error_list.append(error_entry)

        except Exception as e:
            entry = error_entry_for_exception(table, e)
            table_reports.append(entry)
            error_list.append(entry)

    return table_reports, error_list
