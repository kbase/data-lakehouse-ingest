"""
Result models for table-level ingestion operations in the Data Lakehouse Ingest framework.

This module defines structured dataclasses used to represent the outcome of
processing a single table during an ingestion run. These models provide a
typed alternative to loosely structured dictionaries and make the contract
between orchestration components explicit.

Two result types are defined:

- TableProcessSuccess: Returned when a table is successfully processed and
  written to the Silver Delta layer. Includes metrics such as rows read,
  rows written, elapsed time, and optional comment application results.

- TableProcessFailure: Returned when a table fails during processing. Contains
  diagnostic information including the error message, processing phase,
  input metadata, and optional traceback information.

A union alias `TableProcessResult` is provided for convenience when type
hinting functions that may return either success or failure results.

Several bounded fields (such as status, input_source, and write mode)
are represented using enums to improve type safety and prevent invalid
values from propagating through the ingestion pipeline.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Union

from data_lakehouse_ingest.orchestrator.schema_utils import SchemaSource


class ProcessStatus(Enum):
    """
    Represents the outcome of a table processing operation.
    """
    SUCCESS = "success"
    FAILED = "failed"


class InputSource(Enum):
    """
    Indicates where the input data originated from.
    """
    BRONZE = "bronze"
    DATAFRAME = "dataframe"


class WriteMode(Enum):
    """
    Supported write modes when writing Delta tables.
    """
    OVERWRITE = "overwrite"
    APPEND = "append"


@dataclass
class TableProcessSuccess:
    """
    Represents a successful table ingestion result.

    This object captures metadata and metrics produced when a table is
    successfully processed and written to the Silver Delta layer.

    Attributes:
        name: Table name.
        tenant: Tenant identifier associated with the ingestion run.
        target_table: Fully qualified target table name in the Silver layer.
        mode: Write mode used when writing to Delta. Represented by the WriteMode enum.
        format: Detected input file format when reading from Bronze storage.
        schema_source: Origin of the resolved schema. Represented by the SchemaSource enum.
        input_source: Indicates whether input was read from Bronze storage or provided
            as a Spark DataFrame override. Represented by the InputSource enum.
        bronze_path: Source path in Bronze storage if applicable.
        silver_path: Target storage path where the Delta table is written.
        rows_in: Number of input rows read.
        rows_written: Number of rows written to the Silver Delta table.
        rows_rejected: Number of rows rejected during processing.
        extra_columns_dropped: Columns dropped because they were not present in the schema.
        partitions_written: List of partitions written (if partitioning is used).
        quarantine_path: Location where rejected records would be stored.
        elapsed_sec: Processing time in seconds.
        status: Processing status represented by the ProcessStatus enum.
        comments_report: Result of applying Delta column comments when structured
            schema metadata includes column comments.
    """

    name: str
    tenant: str | None
    target_table: str | None
    mode: WriteMode
    format: str | None
    schema_source: SchemaSource
    input_source: InputSource
    bronze_path: str | None
    silver_path: str | None
    rows_in: int | None
    rows_written: int | None
    rows_rejected: int | None
    extra_columns_dropped: list[str]
    partitions_written: list[str] | None
    quarantine_path: str | None
    elapsed_sec: float | None
    status: ProcessStatus
    comments_report: dict[str, Any] | None


@dataclass
class TableProcessFailure:
    """
    Represents a failed table ingestion result.

    This object captures diagnostic information when a table cannot be
    processed successfully.

    Attributes:
        name: Table name.
        error: Error message describing the failure.
        phase: Processing phase where the failure occurred
            (e.g., data_loading, schema_resolution).
        bronze_path: Input Bronze path if known.
        format: Detected input format if known.
        input_source: Indicates whether input was read from Bronze storage or provided
            as a Spark DataFrame override. Represented by the InputSource enum.
        status: Processing status represented by the ProcessStatus enum.
        traceback: Optional formatted stack trace captured at the point of failure,
            when available.
    """

    name: str
    error: str
    phase: str
    bronze_path: str | None
    format: str | None
    input_source: InputSource
    status: ProcessStatus
    traceback: str | None = None


TableProcessResult = Union[TableProcessSuccess, TableProcessFailure]
