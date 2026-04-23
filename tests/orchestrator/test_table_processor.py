import pytest
import logging
from unittest.mock import MagicMock, patch
from data_lakehouse_ingest.orchestrator.table_processor import process_table
from data_lakehouse_ingest.orchestrator.schema_utils import SchemaSource
from data_lakehouse_ingest.orchestrator.models import ProcessStatus, InputSource, WriteMode
from types import SimpleNamespace
from dataclasses import asdict


@pytest.fixture
def mock_spark():
    spark = MagicMock()
    df = MagicMock()
    df.count.return_value = 10
    spark.read.format.return_value.load.return_value = df
    return spark


@pytest.fixture
def mock_logger():
    return MagicMock(spec=logging.Logger)


@pytest.fixture
def mock_loader():
    loader = MagicMock()
    loader.get_bronze_path.return_value = "s3a://bronze/test_table/"
    loader.get_silver_path.return_value = "s3a://silver/test_table/"
    loader.get_defaults_for.return_value = {"header": True, "delimiter": ",", "inferSchema": False}
    return loader


@pytest.fixture
def table_config():
    return {"name": "test_table", "format": "csv", "mode": "overwrite"}


# ---------------------------------------------------------------------
# Regular ingestion path
# ---------------------------------------------------------------------
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_table_comment",
    autospec=True,
    return_value={"status": "skipped", "reason": "no table comment"},
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    autospec=True,
    return_value=SimpleNamespace(
        schema_defs="CREATE TABLE ...",
        schema_source=SchemaSource.SCHEMA_SQL,
        comment_metadata=None,
    ),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.detect_format",
    autospec=True,
    return_value="csv",
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.load_table_data",
    autospec=True,
    return_value=(MagicMock(), 100),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    autospec=True,
    side_effect=lambda *args, **kwargs: (kwargs["df"], {"dropped_columns": []}),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.write_to_delta",
    autospec=True,
    return_value=95,
)
# Verifies the standard bronze→silver happy path produces a success report and calls the expected helpers.
def test_process_table_success(
    mock_write_to_delta,
    mock_apply_schema,
    mock_load_data,
    mock_detect_format,
    mock_resolve_schema,
    mock_apply_table_comment,
    mock_spark,
    mock_logger,
    mock_loader,
    table_config,
):
    ctx = {
        "tenant": "tenant_alpha",
        "namespace": "tenant_alpha__dataset",
        "namespace_base_path": "s3a://silver/",
    }
    result = process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=mock_loader,
        ctx=ctx,
        table=table_config,
        run_started_at_iso="2025-10-31T12:00:00Z",
    )

    # Assertions
    result_dict = asdict(result)
    elapsed = result_dict.pop("elapsed_sec")

    assert isinstance(elapsed, float)
    assert elapsed >= 0

    assert result_dict == {
        "name": "test_table",
        "tenant": "tenant_alpha",
        "target_table": "tenant_alpha__dataset.test_table",
        "mode": WriteMode.OVERWRITE,
        "format": "csv",
        "schema_source": SchemaSource.SCHEMA_SQL,
        "input_source": InputSource.BRONZE,
        "bronze_path": "s3a://bronze/test_table/",
        "silver_path": "s3a://silver/",
        "rows_in": 100,
        "rows_written": 95,
        "rows_rejected": 0,
        "extra_columns_dropped": [],
        "partitions_written": None,
        "quarantine_path": "s3a://silver//quarantine/2025-10-31T12-00-00Z/",
        "status": ProcessStatus.SUCCESS,
        "table_comment_report": None,
        "column_comments_report": None,
    }

    mock_apply_table_comment.assert_not_called()
    mock_detect_format.assert_called_once_with("s3a://bronze/test_table/", "csv")
    mock_resolve_schema.assert_called_once_with(
        spark=mock_spark,
        table=table_config,
        logger=mock_logger,
        minio_client=None,
    )
    mock_load_data.assert_called_once()
    mock_apply_schema.assert_called_once()
    mock_write_to_delta.assert_called_once()
    mock_logger.info.assert_any_call("Processing table: test_table")

    mock_loader.get_bronze_path.assert_called_once_with("test_table")
    mock_loader.get_defaults_for.assert_called_once_with("csv")

    _, args, _ = mock_load_data.mock_calls[0]
    assert args[0] == mock_spark
    assert args[1] == "s3a://bronze/test_table/"
    assert args[2] == "csv"
    assert args[3] == {
        "header": "true",
        "delimiter": ",",
        "inferSchema": "false",
        "recursiveFileLookup": "true",
    }
    assert args[4] == mock_logger

    assert result.status == ProcessStatus.SUCCESS
    assert result.input_source == InputSource.BRONZE
    assert result.mode == WriteMode.OVERWRITE


# ---------------------------------------------------------------------
# Delta comment application path (structured schema only)
# ---------------------------------------------------------------------
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_table_comment",
    autospec=True,
    return_value={"status": "skipped", "reason": "no table comment"},
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_comments_from_table_schema",
    autospec=True,
    return_value={"applied": 1, "skipped": 0, "missing_in_table": []},
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    autospec=True,
    return_value=SimpleNamespace(
        schema_defs=[{"column": "gene_id", "type": "string"}],
        schema_source=SchemaSource.SCHEMA_STRUCTURED,
        comment_metadata=[{"column": "gene_id", "type": "string", "comment": "Gene identifier"}],
    ),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.detect_format",
    autospec=True,
    return_value="csv",
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.load_table_data",
    autospec=True,
    return_value=(MagicMock(), 100),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    autospec=True,
    side_effect=lambda *args, **kwargs: (kwargs["df"], {"dropped_columns": []}),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.write_to_delta",
    autospec=True,
    return_value=95,
)
# Verifies structured schemas trigger Delta column comment application and the correct args are passed through.
def test_process_table_applies_delta_comments_for_structured_schema(
    mock_write_to_delta,
    mock_apply_schema,
    mock_load_data,
    mock_detect_format,
    mock_resolve_schema,
    mock_apply_comments,
    mock_apply_table_comment,
    mock_spark,
    mock_logger,
    mock_loader,
    table_config,
):
    ctx = {
        "tenant": "tenant_alpha",
        "namespace": "tenant_alpha__dataset",
        "namespace_base_path": "s3a://silver/",
    }

    table_with_schema = {
        **table_config,
        "schema": [{"column": "gene_id", "type": "string", "comment": "Gene identifier"}],
    }

    result = process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=mock_loader,
        ctx=ctx,
        table=table_with_schema,
        run_started_at_iso="2025-10-31T12:00:00Z",
    )

    result_dict = asdict(result)
    elapsed = result_dict.pop("elapsed_sec")

    assert isinstance(elapsed, float)
    assert elapsed >= 0

    assert result_dict == {
        "name": "test_table",
        "tenant": "tenant_alpha",
        "target_table": "tenant_alpha__dataset.test_table",
        "mode": WriteMode.OVERWRITE,
        "format": "csv",
        "schema_source": SchemaSource.SCHEMA_STRUCTURED,
        "input_source": InputSource.BRONZE,
        "bronze_path": "s3a://bronze/test_table/",
        "silver_path": "s3a://silver/",
        "rows_in": 100,
        "rows_written": 95,
        "rows_rejected": 0,
        "extra_columns_dropped": [],
        "partitions_written": None,
        "quarantine_path": "s3a://silver//quarantine/2025-10-31T12-00-00Z/",
        "status": ProcessStatus.SUCCESS,
        "table_comment_report": None,
        "column_comments_report": {"applied": 1, "skipped": 0, "missing_in_table": []},
    }

    mock_apply_comments.assert_called_once()
    _, kwargs = mock_apply_comments.call_args
    assert kwargs["full_table_name"] == "tenant_alpha__dataset.test_table"
    assert kwargs["table_schema"] == [
        {"column": "gene_id", "type": "string", "comment": "Gene identifier"}
    ]
    assert kwargs["require_existing_table"] is True

    mock_resolve_schema.assert_called_once_with(
        spark=mock_spark,
        table=table_with_schema,
        logger=mock_logger,
        minio_client=None,
    )

    mock_loader.get_bronze_path.assert_called_once_with("test_table")
    mock_detect_format.assert_called_once_with("s3a://bronze/test_table/", "csv")
    mock_loader.get_defaults_for.assert_called_once_with("csv")

    _, args, _ = mock_load_data.mock_calls[0]
    assert args[0] == mock_spark
    assert args[1] == "s3a://bronze/test_table/"
    assert args[2] == "csv"
    assert args[3] == {
        "header": "true",
        "delimiter": ",",
        "inferSchema": "false",
        "recursiveFileLookup": "true",
    }
    assert args[4] == mock_logger

    _, kwargs = mock_apply_schema.call_args
    assert kwargs["schema_defs"] == [{"column": "gene_id", "type": "string"}]
    assert kwargs["logger"] == mock_logger

    _, kwargs = mock_write_to_delta.call_args
    assert kwargs["spark"] == mock_spark
    assert kwargs["namespace"] == "tenant_alpha__dataset"
    assert kwargs["namespace_base_path"] == "s3a://silver/"
    assert kwargs["name"] == "test_table"
    assert kwargs["silver_path"] == "s3a://silver/"
    assert kwargs["partition_by"] is None
    assert kwargs["mode"] == "overwrite"
    assert kwargs["logger"] == mock_logger


# ---------------------------------------------------------------------
# Delta comment skip path (non-structured schema)
# ---------------------------------------------------------------------
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_table_comment",
    autospec=True,
    return_value={"status": "skipped", "reason": "no table comment"},
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_comments_from_table_schema",
    autospec=True,
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    autospec=True,
    return_value=SimpleNamespace(
        schema_defs="CREATE TABLE ...",
        schema_source=SchemaSource.SCHEMA_SQL,
        comment_metadata=None,
    ),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.detect_format",
    autospec=True,
    return_value="csv",
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.load_table_data",
    autospec=True,
    return_value=(MagicMock(), 100),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    autospec=True,
    side_effect=lambda *args, **kwargs: (kwargs["df"], {"dropped_columns": []}),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.write_to_delta",
    autospec=True,
    return_value=95,
)
# Verifies non-structured schemas skip Delta comment application entirely.
def test_process_table_does_not_apply_delta_comments_for_non_structured_schema(
    mock_write_to_delta,
    mock_apply_schema,
    mock_load_data,
    mock_detect_format,
    mock_resolve_schema,
    mock_apply_comments,
    mock_apply_table_comment,
    mock_spark,
    mock_logger,
    mock_loader,
    table_config,
):
    ctx = {
        "tenant": "tenant_alpha",
        "namespace": "tenant_alpha__dataset",
        "namespace_base_path": "s3a://silver/",
    }
    result = process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=mock_loader,
        ctx=ctx,
        table=table_config,
        run_started_at_iso="2025-10-31T12:00:00Z",
    )

    result_dict = asdict(result)
    elapsed = result_dict.pop("elapsed_sec")

    assert isinstance(elapsed, float)
    assert elapsed >= 0

    assert result_dict == {
        "name": "test_table",
        "tenant": "tenant_alpha",
        "target_table": "tenant_alpha__dataset.test_table",
        "mode": WriteMode.OVERWRITE,
        "format": "csv",
        "schema_source": SchemaSource.SCHEMA_SQL,
        "input_source": InputSource.BRONZE,
        "bronze_path": "s3a://bronze/test_table/",
        "silver_path": "s3a://silver/",
        "rows_in": 100,
        "rows_written": 95,
        "rows_rejected": 0,
        "extra_columns_dropped": [],
        "partitions_written": None,
        "quarantine_path": "s3a://silver//quarantine/2025-10-31T12-00-00Z/",
        "status": ProcessStatus.SUCCESS,
        "table_comment_report": None,
        "column_comments_report": None,
    }

    mock_apply_comments.assert_not_called()

    mock_resolve_schema.assert_called_once_with(
        spark=mock_spark,
        table=table_config,
        logger=mock_logger,
        minio_client=None,
    )

    mock_loader.get_bronze_path.assert_called_once_with("test_table")
    mock_detect_format.assert_called_once_with("s3a://bronze/test_table/", "csv")
    mock_loader.get_defaults_for.assert_called_once_with("csv")

    _, args, _ = mock_load_data.mock_calls[0]
    assert args[0] == mock_spark
    assert args[1] == "s3a://bronze/test_table/"
    assert args[2] == "csv"
    assert args[3] == {
        "header": "true",
        "delimiter": ",",
        "inferSchema": "false",
        "recursiveFileLookup": "true",
    }
    assert args[4] == mock_logger

    _, kwargs = mock_apply_schema.call_args
    assert kwargs["schema_defs"] == "CREATE TABLE ..."
    assert kwargs["logger"] == mock_logger

    _, kwargs = mock_write_to_delta.call_args
    assert kwargs["spark"] == mock_spark
    assert kwargs["namespace"] == "tenant_alpha__dataset"
    assert kwargs["namespace_base_path"] == "s3a://silver/"
    assert kwargs["name"] == "test_table"
    assert kwargs["silver_path"] == "s3a://silver/"
    assert kwargs["partition_by"] is None
    assert kwargs["mode"] == "overwrite"
    assert kwargs["logger"] == mock_logger


# ---------------------------------------------------------------------
# Data load failure path
# ---------------------------------------------------------------------
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    autospec=True,
    return_value=SimpleNamespace(
        schema_defs="CREATE TABLE ...",
        schema_source=SchemaSource.SCHEMA_SQL,
        comment_metadata=None,
    ),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.detect_format",
    autospec=True,
    return_value="csv",
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.load_table_data",
    autospec=True,
    side_effect=Exception("Simulated load failure"),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    autospec=True,
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.write_to_delta",
    autospec=True,
)
# Verifies data loading exceptions are caught and returned as a failed report with phase="data_loading".
def test_process_table_data_load_failure(
    mock_write_to_delta,
    mock_apply_schema,
    mock_load,
    mock_detect_format,
    mock_resolve_schema,
    mock_spark,
    mock_logger,
    mock_loader,
    table_config,
):
    ctx = {
        "tenant": "tenant_alpha",
        "namespace": "tenant_alpha__dataset",
        "namespace_base_path": "s3a://silver/",
    }

    result = process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=mock_loader,
        ctx=ctx,
        table=table_config,
        run_started_at_iso="2025-10-31T12:00:00Z",
    )

    result_dict = asdict(result)
    assert result_dict["name"] == "test_table"
    assert result_dict["error"] == "Simulated load failure"
    assert result_dict["phase"] == "data_loading"
    assert result_dict["bronze_path"] == "s3a://bronze/test_table/"
    assert result_dict["format"] == "csv"
    assert result_dict["input_source"] == InputSource.BRONZE
    assert result_dict["status"] == ProcessStatus.FAILED
    assert isinstance(result_dict["traceback"], str)
    assert "Simulated load failure" in result_dict["traceback"]

    mock_resolve_schema.assert_called_once_with(
        spark=mock_spark,
        table=table_config,
        logger=mock_logger,
        minio_client=None,
    )
    mock_loader.get_bronze_path.assert_called_once_with("test_table")
    mock_detect_format.assert_called_once_with("s3a://bronze/test_table/", "csv")
    mock_loader.get_defaults_for.assert_called_once_with("csv")

    _, args, _ = mock_load.mock_calls[0]
    assert args[0] == mock_spark
    assert args[1] == "s3a://bronze/test_table/"
    assert args[2] == "csv"
    assert args[3] == {
        "header": "true",
        "delimiter": ",",
        "inferSchema": "false",
        "recursiveFileLookup": "true",
    }
    assert args[4] == mock_logger

    mock_apply_schema.assert_not_called()
    mock_write_to_delta.assert_not_called()
    mock_logger.error.assert_called_once()


# ---------------------------------------------------------------------
# logger.context_filter branch
# ---------------------------------------------------------------------
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_table_comment",
    autospec=True,
    return_value={"status": "skipped", "reason": "no table comment"},
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    autospec=True,
    return_value=SimpleNamespace(
        schema_defs="CREATE TABLE ...",
        schema_source=SchemaSource.SCHEMA_SQL,
        comment_metadata=None,
    ),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.detect_format",
    autospec=True,
    return_value="csv",
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.load_table_data",
    autospec=True,
    return_value=(MagicMock(), 100),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    autospec=True,
    side_effect=lambda *args, **kwargs: (kwargs["df"], {"dropped_columns": []}),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.write_to_delta",
    autospec=True,
    return_value=95,
)
# Verifies the logger context filter (if present) is updated with the table name.
def test_process_table_sets_logger_table_context_when_context_filter_present(
    mock_write_to_delta,
    mock_apply_schema,
    mock_load_data,
    mock_detect_format,
    mock_resolve_schema,
    mock_apply_table_comment,
    mock_spark,
    mock_logger,
    mock_loader,
    table_config,
):
    # Add the attribute that your production logger has
    mock_logger.context_filter = MagicMock()

    ctx = {
        "tenant": "tenant_alpha",
        "namespace": "tenant_alpha__dataset",
        "namespace_base_path": "s3a://silver/",
    }

    result = process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=mock_loader,
        ctx=ctx,
        table=table_config,
        run_started_at_iso="2025-10-31T12:00:00Z",
    )

    result_dict = asdict(result)
    elapsed = result_dict.pop("elapsed_sec")

    assert isinstance(elapsed, float)
    assert elapsed >= 0

    assert result_dict == {
        "name": "test_table",
        "tenant": "tenant_alpha",
        "target_table": "tenant_alpha__dataset.test_table",
        "mode": WriteMode.OVERWRITE,
        "format": "csv",
        "schema_source": SchemaSource.SCHEMA_SQL,
        "input_source": InputSource.BRONZE,
        "bronze_path": "s3a://bronze/test_table/",
        "silver_path": "s3a://silver/",
        "rows_in": 100,
        "rows_written": 95,
        "rows_rejected": 0,
        "extra_columns_dropped": [],
        "partitions_written": None,
        "quarantine_path": "s3a://silver//quarantine/2025-10-31T12-00-00Z/",
        "status": ProcessStatus.SUCCESS,
        "table_comment_report": None,
        "column_comments_report": None,
    }

    mock_apply_table_comment.assert_not_called()
    mock_logger.context_filter.set_table.assert_called_once_with("test_table")

    mock_resolve_schema.assert_called_once_with(
        spark=mock_spark,
        table=table_config,
        logger=mock_logger,
        minio_client=None,
    )

    mock_loader.get_bronze_path.assert_called_once_with("test_table")
    mock_detect_format.assert_called_once_with("s3a://bronze/test_table/", "csv")
    mock_loader.get_defaults_for.assert_called_once_with("csv")

    _, args, _ = mock_load_data.mock_calls[0]
    assert args[0] == mock_spark
    assert args[1] == "s3a://bronze/test_table/"
    assert args[2] == "csv"
    assert args[3] == {
        "header": "true",
        "delimiter": ",",
        "inferSchema": "false",
        "recursiveFileLookup": "true",
    }
    assert args[4] == mock_logger

    _, kwargs = mock_apply_schema.call_args
    assert kwargs["schema_defs"] == "CREATE TABLE ..."
    assert kwargs["logger"] == mock_logger

    _, kwargs = mock_write_to_delta.call_args
    assert kwargs["spark"] == mock_spark
    assert kwargs["namespace"] == "tenant_alpha__dataset"
    assert kwargs["namespace_base_path"] == "s3a://silver/"
    assert kwargs["name"] == "test_table"
    assert kwargs["silver_path"] == "s3a://silver/"
    assert kwargs["partition_by"] is None
    assert kwargs["mode"] == "overwrite"
    assert kwargs["logger"] == mock_logger


# ---------------------------------------------------------------------
# loader fallback defaults branch (no get_defaults_for)
# and the TSV delimiter logic ("\t")
# ---------------------------------------------------------------------
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    autospec=True,
    return_value=SimpleNamespace(
        schema_defs="CREATE TABLE ...",
        schema_source=SchemaSource.SCHEMA_SQL,
        comment_metadata=None,
    ),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.detect_format",
    autospec=True,
    return_value="tsv",
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.load_table_data",
    autospec=True,
    return_value=(MagicMock(), 100),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    autospec=True,
    side_effect=lambda *args, **kwargs: (kwargs["df"], {"dropped_columns": []}),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.write_to_delta",
    autospec=True,
    return_value=95,
)
# Verifies fallback reader options are used when loader lacks get_defaults_for(), including TSV delimiter and recursive lookup.
def test_process_table_uses_fallback_reader_options_when_loader_has_no_get_defaults_for(
    mock_write_to_delta,
    mock_apply_schema,
    mock_load_data,
    mock_detect_format,
    mock_resolve_schema,
    mock_spark,
    mock_logger,
    table_config,
):
    # Minimal loader that intentionally lacks get_defaults_for()
    class LoaderNoDefaults:
        def get_bronze_path(self, name):
            return "s3a://bronze/test_table/"

        def get_silver_path(self, name):
            return "s3a://silver/test_table/"

    loader = LoaderNoDefaults()

    ctx = {
        "tenant": "tenant_alpha",
        "namespace": "tenant_alpha__dataset",
        "namespace_base_path": "s3a://silver/",
    }

    result = process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=loader,
        ctx=ctx,
        table=table_config,
        run_started_at_iso="2025-10-31T12:00:00Z",
    )

    assert result.status == ProcessStatus.SUCCESS
    assert result.target_table == "tenant_alpha__dataset.test_table"

    # Assert fallback delimiter for TSV was used
    _, args, _ = mock_load_data.mock_calls[0]
    # load_table_data(spark, bronze_path, fmt, opts, logger)
    opts = args[3]
    assert opts["delimiter"] == "\t"
    assert opts["header"] == "true"
    assert opts["inferSchema"] == "false"
    assert opts["recursiveFileLookup"] == "true"


# ---------------------------------------------------------------------
# DataFrame override bypasses bronze loading
# ---------------------------------------------------------------------
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_table_comment",
    autospec=True,
    return_value={"status": "skipped", "reason": "no table comment"},
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    autospec=True,
    return_value=SimpleNamespace(
        schema_defs="CREATE TABLE ...",
        schema_source=SchemaSource.SCHEMA_SQL,
        comment_metadata=None,
    ),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    autospec=True,
    side_effect=lambda *args, **kwargs: (kwargs["df"], {"dropped_columns": []}),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.write_to_delta",
    autospec=True,
    return_value=10,
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.load_table_data",
    autospec=True,
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.detect_format",
    autospec=True,
)
# Verifies df_override ingestion bypasses bronze path loading and uses df.count() for rows_in.
def test_process_table_dataframe_override_skips_bronze_loading(
    mock_detect_format,
    mock_load_table_data,
    mock_write_to_delta,
    mock_apply_schema_columns,
    mock_resolve_schema,
    mock_apply_table_comment,
    mock_spark,
    mock_logger,
    mock_loader,
    table_config,
):
    ctx = {
        "tenant": "tenant_alpha",
        "namespace": "tenant_alpha__dataset",
        "namespace_base_path": "s3a://silver/",
    }

    df_override = MagicMock()
    df_override.count.return_value = 123

    result = process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=mock_loader,
        ctx=ctx,
        table=table_config,
        run_started_at_iso="2025-10-31T12:00:00Z",
        df_override=df_override,
    )

    result_dict = asdict(result)
    elapsed = result_dict.pop("elapsed_sec")

    assert isinstance(elapsed, float)
    assert elapsed >= 0

    assert result_dict == {
        "name": "test_table",
        "tenant": "tenant_alpha",
        "target_table": "tenant_alpha__dataset.test_table",
        "mode": WriteMode.OVERWRITE,
        "format": None,
        "schema_source": SchemaSource.SCHEMA_SQL,
        "input_source": InputSource.DATAFRAME,
        "bronze_path": None,
        "silver_path": "s3a://silver/",
        "rows_in": 123,
        "rows_written": 10,
        "rows_rejected": 0,
        "extra_columns_dropped": [],
        "partitions_written": None,
        "quarantine_path": "s3a://silver//quarantine/2025-10-31T12-00-00Z/",
        "status": ProcessStatus.SUCCESS,
        "table_comment_report": None,
        "column_comments_report": None,
    }

    mock_apply_table_comment.assert_not_called()

    mock_resolve_schema.assert_called_once_with(
        spark=mock_spark,
        table=table_config,
        logger=mock_logger,
        minio_client=None,
    )

    df_override.count.assert_called_once()

    _, kwargs = mock_apply_schema_columns.call_args
    assert kwargs["df"] == df_override
    assert kwargs["schema_defs"] == "CREATE TABLE ..."
    assert kwargs["logger"] == mock_logger
    _, kwargs = mock_write_to_delta.call_args
    assert kwargs["df"] == df_override
    assert kwargs["spark"] == mock_spark
    assert kwargs["namespace"] == "tenant_alpha__dataset"
    assert kwargs["namespace_base_path"] == "s3a://silver/"
    assert kwargs["name"] == "test_table"
    assert kwargs["silver_path"] == "s3a://silver/"
    assert kwargs["partition_by"] is None
    assert kwargs["mode"] == "overwrite"
    assert kwargs["logger"] == mock_logger

    # No bronze-path loading should happen
    mock_loader.get_bronze_path.assert_not_called()
    mock_detect_format.assert_not_called()
    mock_load_table_data.assert_not_called()

    # still should apply schema + write
    mock_apply_schema_columns.assert_called_once()
    mock_write_to_delta.assert_called_once()


# ---------------------------------------------------------------------
# DataFrame override format selection
# ---------------------------------------------------------------------
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    autospec=True,
    return_value=SimpleNamespace(
        schema_defs="CREATE TABLE ...",
        schema_source=SchemaSource.SCHEMA_SQL,
        comment_metadata=None,
    ),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    autospec=True,
    side_effect=lambda *args, **kwargs: (kwargs["df"], {"dropped_columns": []}),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.write_to_delta",
    autospec=True,
    return_value=1,
)
# Verifies df_override reports format=None because Bronze file format is not applicable for DataFrame-based ingestion.
def test_process_table_dataframe_override_sets_format_from_table_or_default(
    mock_write_to_delta,
    mock_apply_schema_columns,
    mock_resolve_schema,
    mock_spark,
    mock_logger,
    mock_loader,
):
    ctx = {
        "tenant": "tenant_alpha",
        "namespace": "tenant_alpha__dataset",
        "namespace_base_path": "s3a://silver/",
    }

    df_override = MagicMock()
    df_override.count.return_value = 1

    # Case A: format provided
    result_a = process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=mock_loader,
        ctx=ctx,
        table={"name": "test_table", "format": "csv", "mode": "overwrite"},
        run_started_at_iso="2025-10-31T12:00:00Z",
        df_override=df_override,
    )
    assert result_a.format is None

    # Case B: format missing -> "<dataframe>"
    result_b = process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=mock_loader,
        ctx=ctx,
        table={"name": "test_table", "mode": "overwrite"},
        run_started_at_iso="2025-10-31T12:00:00Z",
        df_override=df_override,
    )
    assert result_b.format is None

    assert mock_resolve_schema.call_count == 2
    assert df_override.count.call_count == 2
    mock_loader.get_bronze_path.assert_not_called()
    assert mock_apply_schema_columns.call_count == 2
    assert mock_write_to_delta.call_count == 2

    first_call = mock_resolve_schema.call_args_list[0]
    second_call = mock_resolve_schema.call_args_list[1]

    assert first_call.kwargs["table"] == {
        "name": "test_table",
        "format": "csv",
        "mode": "overwrite",
    }
    assert second_call.kwargs["table"] == {"name": "test_table", "mode": "overwrite"}

    for call in mock_write_to_delta.call_args_list:
        kwargs = call.kwargs
        assert kwargs["name"] == "test_table"
        assert kwargs["mode"] == "overwrite"
        assert kwargs["namespace"] == "tenant_alpha__dataset"


# ---------------------------------------------------------------------
# DataFrame override applies Delta comments
# ---------------------------------------------------------------------
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_table_comment",
    autospec=True,
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_comments_from_table_schema",
    autospec=True,
    return_value={"applied": 1, "skipped": 0, "missing_in_table": []},
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    autospec=True,
    return_value=SimpleNamespace(
        schema_defs=[{"column": "gene_id", "type": "string"}],
        schema_source=SchemaSource.SCHEMA_STRUCTURED,
        comment_metadata=[{"column": "gene_id", "type": "string", "comment": "Gene identifier"}],
    ),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    autospec=True,
    side_effect=lambda *args, **kwargs: (kwargs["df"], {"dropped_columns": []}),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.write_to_delta",
    autospec=True,
    return_value=10,
)
# Verifies df_override still applies Delta comments when structured comment metadata is present.
def test_process_table_dataframe_override_applies_delta_comments_when_available(
    mock_write_to_delta,
    mock_apply_schema_columns,
    mock_resolve_schema,
    mock_apply_comments,
    mock_apply_table_comment,
    mock_spark,
    mock_logger,
    mock_loader,
):
    ctx = {
        "tenant": "tenant_alpha",
        "namespace": "tenant_alpha__dataset",
        "namespace_base_path": "s3a://silver/",
    }

    df_override = MagicMock()
    df_override.count.return_value = 10

    result = process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=mock_loader,
        ctx=ctx,
        table={"name": "test_table", "mode": "overwrite"},
        run_started_at_iso="2025-10-31T12:00:00Z",
        df_override=df_override,
    )

    assert result.status == ProcessStatus.SUCCESS
    assert result.input_source == InputSource.DATAFRAME
    assert result.bronze_path is None
    assert result.format is None
    assert result.table_comment_report is None
    assert result.column_comments_report == {
        "applied": 1,
        "skipped": 0,
        "missing_in_table": [],
    }

    mock_apply_table_comment.assert_not_called()

    mock_resolve_schema.assert_called_once_with(
        spark=mock_spark,
        table={"name": "test_table", "mode": "overwrite"},
        logger=mock_logger,
        minio_client=None,
    )

    df_override.count.assert_called_once()

    mock_loader.get_bronze_path.assert_not_called()

    _, kwargs = mock_apply_schema_columns.call_args
    assert kwargs["df"] == df_override
    assert kwargs["schema_defs"] == [{"column": "gene_id", "type": "string"}]
    assert kwargs["logger"] == mock_logger

    _, kwargs = mock_write_to_delta.call_args
    assert kwargs["df"] == df_override
    assert kwargs["spark"] == mock_spark
    assert kwargs["namespace"] == "tenant_alpha__dataset"
    assert kwargs["namespace_base_path"] == "s3a://silver/"
    assert kwargs["name"] == "test_table"
    assert kwargs["silver_path"] == "s3a://silver/"
    assert kwargs["partition_by"] is None
    assert kwargs["mode"] == "overwrite"
    assert kwargs["logger"] == mock_logger

    mock_apply_comments.assert_called_once()
    _, kwargs = mock_apply_comments.call_args
    assert kwargs["spark"] == mock_spark
    assert kwargs["full_table_name"] == "tenant_alpha__dataset.test_table"
    assert kwargs["table_schema"] == [
        {"column": "gene_id", "type": "string", "comment": "Gene identifier"}
    ]
    assert kwargs["logger"] == mock_logger
    assert kwargs["require_existing_table"] is True


# ---------------------------------------------------------------------
# Dropped columns reported in result
# ---------------------------------------------------------------------
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_table_comment",
    autospec=True,
    return_value={"status": "skipped", "reason": "no table comment"},
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    autospec=True,
    return_value=SimpleNamespace(
        schema_defs="CREATE TABLE ...",
        schema_source=SchemaSource.SCHEMA_SQL,
        comment_metadata=None,
    ),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.detect_format",
    autospec=True,
    return_value="csv",
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.load_table_data",
    autospec=True,
    return_value=(MagicMock(), 100),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    autospec=True,
    return_value=(MagicMock(), {"dropped_columns": ["colA", "colB"]}),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.write_to_delta",
    autospec=True,
    return_value=95,
)
# Verifies dropped columns returned by apply_schema_columns are surfaced in extra_columns_dropped in the report.
def test_process_table_reports_dropped_columns(
    mock_write_to_delta,
    mock_apply_schema,
    mock_load_data,
    mock_detect_format,
    mock_resolve_schema,
    mock_apply_table_comment,
    mock_spark,
    mock_logger,
    mock_loader,
):
    ctx = {
        "tenant": "tenant_alpha",
        "namespace": "tenant_alpha__dataset",
        "namespace_base_path": "s3a://silver/",
    }

    result = process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=mock_loader,
        ctx=ctx,
        table={"name": "test_table", "format": "csv", "mode": "overwrite"},
        run_started_at_iso="2025-10-31T12:00:00Z",
    )

    result_dict = asdict(result)
    elapsed = result_dict.pop("elapsed_sec")

    assert isinstance(elapsed, float)
    assert elapsed >= 0

    assert result_dict == {
        "name": "test_table",
        "tenant": "tenant_alpha",
        "target_table": "tenant_alpha__dataset.test_table",
        "mode": WriteMode.OVERWRITE,
        "format": "csv",
        "schema_source": SchemaSource.SCHEMA_SQL,
        "input_source": InputSource.BRONZE,
        "bronze_path": "s3a://bronze/test_table/",
        "silver_path": "s3a://silver/",
        "rows_in": 100,
        "rows_written": 95,
        "rows_rejected": 0,
        "extra_columns_dropped": ["colA", "colB"],
        "partitions_written": None,
        "quarantine_path": "s3a://silver//quarantine/2025-10-31T12-00-00Z/",
        "status": ProcessStatus.SUCCESS,
        "table_comment_report": None,
        "column_comments_report": None,
    }

    mock_resolve_schema.assert_called_once_with(
        spark=mock_spark,
        table={"name": "test_table", "format": "csv", "mode": "overwrite"},
        logger=mock_logger,
        minio_client=None,
    )

    mock_loader.get_bronze_path.assert_called_once_with("test_table")
    mock_detect_format.assert_called_once_with("s3a://bronze/test_table/", "csv")
    mock_loader.get_defaults_for.assert_called_once_with("csv")

    _, args, _ = mock_load_data.mock_calls[0]
    assert args[0] == mock_spark
    assert args[1] == "s3a://bronze/test_table/"
    assert args[2] == "csv"
    assert args[3] == {
        "header": "true",
        "delimiter": ",",
        "inferSchema": "false",
        "recursiveFileLookup": "true",
    }
    assert args[4] == mock_logger

    _, kwargs = mock_apply_schema.call_args
    assert kwargs["schema_defs"] == "CREATE TABLE ..."
    assert kwargs["logger"] == mock_logger

    _, kwargs = mock_write_to_delta.call_args
    assert kwargs["spark"] == mock_spark
    assert kwargs["namespace"] == "tenant_alpha__dataset"
    assert kwargs["namespace_base_path"] == "s3a://silver/"
    assert kwargs["name"] == "test_table"
    assert kwargs["silver_path"] == "s3a://silver/"
    assert kwargs["partition_by"] is None
    assert kwargs["mode"] == "overwrite"
    assert kwargs["logger"] == mock_logger


# ---------------------------------------------------------------------
# Delta table level comment
# ---------------------------------------------------------------------


@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_table_comment",
    autospec=True,
    return_value={"status": "success", "applied": True},
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    autospec=True,
    return_value=SimpleNamespace(
        schema_defs="CREATE TABLE ...",
        schema_source=SchemaSource.SCHEMA_SQL,
        comment_metadata=None,
    ),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.detect_format",
    autospec=True,
    return_value="csv",
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.load_table_data",
    autospec=True,
    return_value=(MagicMock(), 100),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    autospec=True,
    side_effect=lambda *args, **kwargs: (kwargs["df"], {"dropped_columns": []}),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.write_to_delta",
    autospec=True,
    return_value=95,
)
def test_process_table_applies_table_level_string_comment(
    mock_write_to_delta,
    mock_apply_schema,
    mock_load_data,
    mock_detect_format,
    mock_resolve_schema,
    mock_apply_table_comment,
    mock_spark,
    mock_logger,
    mock_loader,
):
    ctx = {
        "tenant": "tenant_alpha",
        "namespace": "tenant_alpha__dataset",
        "namespace_base_path": "s3a://silver/",
    }

    table = {
        "name": "test_table",
        "format": "csv",
        "mode": "overwrite",
        "comment": "Test table comment",
    }

    result = process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=mock_loader,
        ctx=ctx,
        table=table,
        run_started_at_iso="2025-10-31T12:00:00Z",
    )

    assert result.status == ProcessStatus.SUCCESS
    assert result.table_comment_report == {"status": "success", "applied": True}
    assert result.column_comments_report is None

    mock_apply_table_comment.assert_called_once_with(
        spark=mock_spark,
        full_table_name="tenant_alpha__dataset.test_table",
        table_comment="Test table comment",
        logger=mock_logger,
        require_existing_table=True,
    )


@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_table_comment",
    autospec=True,
    return_value={"status": "success", "applied": True},
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    autospec=True,
    return_value=SimpleNamespace(
        schema_defs="CREATE TABLE ...",
        schema_source=SchemaSource.SCHEMA_SQL,
        comment_metadata=None,
    ),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.detect_format",
    autospec=True,
    return_value="csv",
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.load_table_data",
    autospec=True,
    return_value=(MagicMock(), 100),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    autospec=True,
    side_effect=lambda *args, **kwargs: (kwargs["df"], {"dropped_columns": []}),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.write_to_delta",
    autospec=True,
    return_value=95,
)
def test_process_table_applies_table_level_dict_comment(
    mock_write_to_delta,
    mock_apply_schema,
    mock_load_data,
    mock_detect_format,
    mock_resolve_schema,
    mock_apply_table_comment,
    mock_spark,
    mock_logger,
    mock_loader,
):
    ctx = {
        "tenant": "tenant_alpha",
        "namespace": "tenant_alpha__dataset",
        "namespace_base_path": "s3a://silver/",
    }

    table_comment = {
        "description": "Test table comment",
        "owner": "arkinlab",
    }

    table = {
        "name": "test_table",
        "format": "csv",
        "mode": "overwrite",
        "comment": table_comment,
    }

    result = process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=mock_loader,
        ctx=ctx,
        table=table,
        run_started_at_iso="2025-10-31T12:00:00Z",
    )

    assert result.status == ProcessStatus.SUCCESS
    assert result.table_comment_report == {"status": "success", "applied": True}
    assert result.column_comments_report is None

    mock_apply_table_comment.assert_called_once_with(
        spark=mock_spark,
        full_table_name="tenant_alpha__dataset.test_table",
        table_comment=table_comment,
        logger=mock_logger,
        require_existing_table=True,
    )


@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_comments_from_table_schema",
    autospec=True,
    return_value={"applied": 1, "skipped": 0, "missing_in_table": []},
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_table_comment",
    autospec=True,
    return_value={"status": "success", "applied": True},
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    autospec=True,
    return_value=SimpleNamespace(
        schema_defs=[{"column": "gene_id", "type": "string"}],
        schema_source=SchemaSource.SCHEMA_STRUCTURED,
        comment_metadata=[{"column": "gene_id", "type": "string", "comment": "Gene identifier"}],
    ),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.detect_format",
    autospec=True,
    return_value="csv",
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.load_table_data",
    autospec=True,
    return_value=(MagicMock(), 100),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    autospec=True,
    side_effect=lambda *args, **kwargs: (kwargs["df"], {"dropped_columns": []}),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.write_to_delta",
    autospec=True,
    return_value=95,
)
def test_process_table_applies_table_and_column_comments(
    mock_write_to_delta,
    mock_apply_schema,
    mock_load_data,
    mock_detect_format,
    mock_resolve_schema,
    mock_apply_table_comment,
    mock_apply_column_comments,
    mock_spark,
    mock_logger,
    mock_loader,
):
    ctx = {
        "tenant": "tenant_alpha",
        "namespace": "tenant_alpha__dataset",
        "namespace_base_path": "s3a://silver/",
    }

    table = {
        "name": "test_table",
        "format": "csv",
        "mode": "overwrite",
        "comment": {"description": "table comment"},
        "schema": [{"column": "gene_id", "type": "string", "comment": "Gene identifier"}],
    }

    result = process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=mock_loader,
        ctx=ctx,
        table=table,
        run_started_at_iso="2025-10-31T12:00:00Z",
    )

    assert result.status == ProcessStatus.SUCCESS
    assert result.table_comment_report == {"status": "success", "applied": True}
    assert result.column_comments_report == {
        "applied": 1,
        "skipped": 0,
        "missing_in_table": [],
    }

    mock_apply_table_comment.assert_called_once()
    mock_apply_column_comments.assert_called_once()
