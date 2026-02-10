import pytest
import logging
from unittest.mock import MagicMock, patch
from data_lakehouse_ingest.orchestrator.table_processor import process_table
from data_lakehouse_ingest.orchestrator.schema_utils import SchemaSource
from types import SimpleNamespace


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
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    return_value=SimpleNamespace(
        schema_defs="CREATE TABLE ...",
        schema_source=SchemaSource.SCHEMA_SQL,
        comments_schema=None,
    ),
)
@patch("data_lakehouse_ingest.orchestrator.table_processor.detect_format", return_value="csv")
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.load_table_data",
    return_value=(MagicMock(), 100),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    side_effect=lambda df, **_: (df, {"dropped_columns": []}),
)
@patch("data_lakehouse_ingest.orchestrator.table_processor.write_to_delta", return_value=95)
def test_process_table_success(
    mock_write_to_delta,
    mock_apply_schema,
    mock_load_data,
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

    # Assertions
    assert result["status"] == "success"
    assert result["rows_in"] == 100
    assert result["rows_written"] == 95
    assert "elapsed_sec" in result
    mock_detect_format.assert_called_once()
    mock_resolve_schema.assert_called_once()
    mock_load_data.assert_called_once()
    mock_apply_schema.assert_called_once()
    mock_write_to_delta.assert_called_once()
    mock_logger.info.assert_any_call("Processing table: test_table")


# ---------------------------------------------------------------------
# Delta comment application path (structured schema only)
# ---------------------------------------------------------------------
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_comments_from_table_schema",
    return_value={"applied": 1, "skipped": 0, "missing_in_table": []},
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    return_value=SimpleNamespace(
        schema_defs=[{"column": "gene_id", "type": "string"}],
        schema_source=SchemaSource.SCHEMA_STRUCTURED,
        comments_schema=[{"column": "gene_id", "type": "string", "comment": "Gene identifier"}],
    ),
)
@patch("data_lakehouse_ingest.orchestrator.table_processor.detect_format", return_value="csv")
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.load_table_data",
    return_value=(MagicMock(), 100),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    side_effect=lambda df, **_: (df, {"dropped_columns": []}),
)
@patch("data_lakehouse_ingest.orchestrator.table_processor.write_to_delta", return_value=95)
def test_process_table_applies_delta_comments_for_structured_schema(
    mock_write_to_delta,
    mock_apply_schema,
    mock_load_data,
    mock_detect_format,
    mock_resolve_schema,
    mock_apply_comments,
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

    assert result["status"] == "success"
    assert result["comments_report"] == {"applied": 1, "skipped": 0, "missing_in_table": []}

    mock_apply_comments.assert_called_once()
    _, kwargs = mock_apply_comments.call_args
    assert kwargs["full_table_name"] == "tenant_alpha__dataset.test_table"
    assert kwargs["table_schema"] == [
        {"column": "gene_id", "type": "string", "comment": "Gene identifier"}
    ]
    assert kwargs["require_existing_table"] is True


# ---------------------------------------------------------------------
# Delta comment skip path (non-structured schema)
# ---------------------------------------------------------------------
@patch("data_lakehouse_ingest.orchestrator.table_processor.apply_comments_from_table_schema")
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    return_value=SimpleNamespace(
        schema_defs="CREATE TABLE ...",
        schema_source=SchemaSource.SCHEMA_SQL,
        comments_schema=None,
    ),
)
@patch("data_lakehouse_ingest.orchestrator.table_processor.detect_format", return_value="csv")
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.load_table_data",
    return_value=(MagicMock(), 100),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    side_effect=lambda df, **_: (df, {"dropped_columns": []}),
)
@patch("data_lakehouse_ingest.orchestrator.table_processor.write_to_delta", return_value=95)
def test_process_table_does_not_apply_delta_comments_for_non_structured_schema(
    mock_write_to_delta,
    mock_apply_schema,
    mock_load_data,
    mock_detect_format,
    mock_resolve_schema,
    mock_apply_comments,
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

    assert result["status"] == "success"
    assert result["comments_report"] is None
    mock_apply_comments.assert_not_called()


# ---------------------------------------------------------------------
# Data load failure path
# ---------------------------------------------------------------------
@patch("data_lakehouse_ingest.orchestrator.table_processor.detect_format", return_value="csv")
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.load_table_data",
    side_effect=Exception("Simulated load failure"),
)
def test_process_table_data_load_failure(
    mock_load, mock_detect_format, mock_spark, mock_logger, mock_loader, table_config
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

    assert result["status"] == "failed"
    assert result["phase"] == "data_loading"
    assert "Simulated load failure" in result["error"]
    mock_logger.error.assert_called_once()


# ---------------------------------------------------------------------
# logger.context_filter branch
# ---------------------------------------------------------------------
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    return_value=SimpleNamespace(
        schema_defs="CREATE TABLE ...",
        schema_source=SchemaSource.SCHEMA_SQL,
        comments_schema=None,
    ),
)
@patch("data_lakehouse_ingest.orchestrator.table_processor.detect_format", return_value="csv")
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.load_table_data",
    return_value=(MagicMock(), 100),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    side_effect=lambda df, **_: (df, {"dropped_columns": []}),
)
@patch("data_lakehouse_ingest.orchestrator.table_processor.write_to_delta", return_value=95)
def test_process_table_sets_logger_table_context_when_context_filter_present(
    mock_write_to_delta,
    mock_apply_schema,
    mock_load_data,
    mock_detect_format,
    mock_resolve_schema,
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

    process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=mock_loader,
        ctx=ctx,
        table=table_config,
        run_started_at_iso="2025-10-31T12:00:00Z",
    )

    mock_logger.context_filter.set_table.assert_called_once_with("test_table")


# ---------------------------------------------------------------------
# loader fallback defaults branch (no get_defaults_for)
# and the TSV delimiter logic ("\t")
# ---------------------------------------------------------------------
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.resolve_schema",
    return_value=SimpleNamespace(
        schema_defs="CREATE TABLE ...",
        schema_source=SchemaSource.SCHEMA_SQL,
        comments_schema=None,
    ),
)
@patch("data_lakehouse_ingest.orchestrator.table_processor.detect_format", return_value="tsv")
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.load_table_data",
    return_value=(MagicMock(), 100),
)
@patch(
    "data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns",
    side_effect=lambda df, **_: (df, {"dropped_columns": []}),
)
@patch("data_lakehouse_ingest.orchestrator.table_processor.write_to_delta", return_value=95)
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

    process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=loader,
        ctx=ctx,
        table=table_config,
        run_started_at_iso="2025-10-31T12:00:00Z",
    )

    # Assert fallback delimiter for TSV was used
    _, args, _ = mock_load_data.mock_calls[0]
    # load_table_data(spark, bronze_path, fmt, opts, logger)
    opts = args[3]
    assert opts["delimiter"] == "\t"
    assert opts["header"] == "true"
    assert opts["inferSchema"] == "false"
    assert opts["recursiveFileLookup"] == "true"
