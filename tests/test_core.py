import pytest
import json
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame
from data_lakehouse_ingest.core import ingest


# ---------------------------------------------------------------------
# Helper: create a dummy SparkSession mock
# ---------------------------------------------------------------------
@pytest.fixture
def mock_spark():
    spark = MagicMock()

    # --- Fake DataFrame returned by read.json ---
    mock_df = MagicMock()
    mock_df.columns = ["id", "name"]
    mock_df.select.return_value = mock_df
    mock_df.count.return_value = 5
    spark.read.option.return_value.options.return_value.json.return_value = mock_df

    # --- Mock DESCRIBE NAMESPACE EXTENDED ---
    fake_row = MagicMock()
    fake_row.info_name = "location"
    fake_row.info_value = "s3a://bucket/silver/"  # MUST NOT be None

    fake_result_df = MagicMock()
    fake_result_df.collect.return_value = [fake_row]

    spark.sql.return_value = fake_result_df

    # --- Prevent failures in setCurrentDatabase ---
    spark.catalog.setCurrentDatabase.return_value = None

    return spark


@pytest.fixture
def mock_logger():
    return MagicMock()


# ---------------------------------------------------------------------
# data_lakehouse_ingest_config tests
# ---------------------------------------------------------------------


# Test that ingest() automatically initializes Spark and MinIO when both are None
# and successfully proceeds to process tables.
@patch("data_lakehouse_ingest.core.init_run_context")
@patch("data_lakehouse_ingest.core.process_tables")
@patch("data_lakehouse_ingest.core.ConfigLoader")
@patch("data_lakehouse_ingest.core.get_minio_client")
@patch("data_lakehouse_ingest.core.get_spark_session")
def test_ingest_autoinit_spark_and_minio_success(
    mock_get_spark,
    mock_get_minio,
    mock_configloader,
    mock_process_tables,
    mock_init_ctx,
):
    mock_get_spark.return_value = MagicMock()
    mock_get_minio.return_value = MagicMock()

    mock_init_ctx.return_value = {"tables": [{"name": "table1"}]}
    mock_process_tables.return_value = ([{"name": "table1", "status": "success"}], [])

    # ConfigLoader should be constructible
    mock_configloader.return_value = MagicMock()

    result = ingest(config={}, spark=None, logger=MagicMock(), minio_client=None)

    assert result["success"] is True
    mock_get_spark.assert_called_once()
    mock_get_minio.assert_called_once()


# Test that ingest() returns a failure report when Spark auto-initialization raises an exception.
@patch("data_lakehouse_ingest.core.get_spark_session")
def test_ingest_autoinit_spark_failure(mock_get_spark, mock_logger):
    mock_get_spark.side_effect = Exception("spark failed")

    result = ingest(config={}, spark=None, logger=mock_logger, minio_client=None)

    assert result["success"] is False
    assert "spark_initialization" in json.dumps(result)


# Test that ingest() returns a failure report when MinIO auto-initialization raises an exception.
@patch("data_lakehouse_ingest.core.get_minio_client")
@patch("data_lakehouse_ingest.core.get_spark_session")
def test_ingest_autoinit_minio_failure(mock_get_spark, mock_get_minio, mock_logger):
    mock_get_spark.return_value = MagicMock()
    mock_get_minio.side_effect = Exception("minio failed")

    result = ingest(config={}, spark=None, logger=mock_logger, minio_client=None)

    mock_get_spark.assert_called_once()
    mock_get_minio.assert_called_once()

    assert result["success"] is False
    assert "minio_initialization" in json.dumps(result)


# Test that ingest() returns a validation failure when ConfigLoader raises an exception
# while parsing or validating the configuration.
@patch("data_lakehouse_ingest.core.ConfigLoader")
@patch("data_lakehouse_ingest.core.get_minio_client")
def test_ingest_config_configloader_failure(
    mock_get_minio, mock_configloader, mock_spark, mock_logger
):
    mock_get_minio.return_value = MagicMock()
    mock_configloader.side_effect = Exception("invalid config")

    result = ingest(config={}, spark=mock_spark, logger=mock_logger, minio_client=None)

    assert result["success"] is False
    assert "config_validation" in json.dumps(result)


# Test that ingest() successfully processes a valid configuration and returns
# a successful table processing report.
@patch("data_lakehouse_ingest.orchestrator.init_utils.create_namespace_if_not_exists")
@patch("data_lakehouse_ingest.core.get_minio_client")
@patch("data_lakehouse_ingest.core.process_tables")
@patch("data_lakehouse_ingest.core.ConfigLoader")
def test_ingest_config_valid_json(
    mock_configloader,
    mock_process,
    mock_minio,
    mock_spark,
    mock_logger,
):
    mock_minio.return_value = MagicMock()

    mock_process.return_value = ([{"name": "table1", "status": "success"}], [])

    loader = mock_configloader.return_value

    loader.config = {"tenant": "tenant_demo", "dataset": "demo_dataset"}

    loader.get_full_config.return_value = {}
    loader.get_tenant.return_value = "tenant_demo"

    loader.get_namespace.return_value = "tenant_demo__dataset"
    loader.get_namespace_base_path.return_value = "s3a://bucket/silver/"

    loader.get_all_defaults.return_value = {"json": {"header": True}}
    loader.get_tables.return_value = [
        {
            "name": "table1",
            "format": "json",
            "schema_sql": "id STRING, name STRING",
            "bronze_path": "s3a://bucket/file.json",
            "linkml_schema": None,
            "silver_path": "s3a://bucket/silver/table1",
        }
    ]

    loader.get_bronze_path.return_value = "s3a://bucket/file.json"
    loader.get_silver_path.return_value = "s3a://bucket/silver/table1"

    result = ingest(config={}, spark=mock_spark, logger=mock_logger)

    assert result["success"] is True
    assert len(result["tables"]) == 1
    assert result["tables"][0]["status"] == "success"


# ---------------------------------------------------------------------
# Defensive MinIO init branch: get_minio_client returns None without raising
# ---------------------------------------------------------------------


# Test that ingest() defensively fails when get_minio_client() returns None
# instead of a valid MinIO client instance.
@patch("data_lakehouse_ingest.core.get_minio_client")
def test_ingest_minio_autoinit_returns_none_triggers_defensive_failure(
    mock_get_minio,
    mock_spark,
    mock_logger,
):
    mock_get_minio.return_value = None  # no exception, but still invalid

    result = ingest(config={}, spark=mock_spark, logger=mock_logger, minio_client=None)

    assert result["success"] is False
    assert "minio_initialization" in json.dumps(result)
    mock_get_minio.assert_called_once()


# ---------------------------------------------------------------------
# DataFrame override validation tests (core.ingest)
# ---------------------------------------------------------------------


# Test multiple invalid `dataframes` override cases using parameterization to ensure proper dataframe_validation errors.
@pytest.mark.parametrize(
    "dataframes, ctx_tables, expected_error_substrings",
    [
        pytest.param(
            ["not-a-dict"],
            [{"name": "table1"}],
            ["Invalid 'dataframes' argument.", "Expected dict[str, pyspark.sql.DataFrame]"],
            id="dataframes_not_a_dict",
        ),
        pytest.param(
            {123: object()},
            [{"name": "table1"}],
            ["Invalid key in 'dataframes': expected str table name"],
            id="dataframes_key_not_string",
        ),
        pytest.param(
            {"table1": object()},
            [{"name": "table1"}],
            ["Invalid value for table 'table1' in 'dataframes': expected pyspark.sql.DataFrame"],
            id="dataframes_value_not_dataframe",
        ),
        pytest.param(
            {"table2": "DF"},
            [{"name": "table1"}],
            ["DataFrame override provided for unknown table(s):", "Valid tables:"],
            id="dataframes_unknown_table",
        ),
    ],
)
@patch("data_lakehouse_ingest.core.init_run_context")
@patch("data_lakehouse_ingest.core.process_tables")
@patch("data_lakehouse_ingest.core.ConfigLoader")
@patch("data_lakehouse_ingest.core.get_minio_client")
def test_ingest_dataframes_invalid_inputs_return_specific_validation_error(
    mock_get_minio,
    mock_configloader,
    mock_process_tables,
    mock_init_ctx,
    mock_spark,
    mock_logger,
    dataframes,
    ctx_tables,
    expected_error_substrings,
):
    """
    Test that ingest() rejects invalid dataframe overrides and returns a dataframe_validation
    error with a case-specific message. Parameterized to avoid repetitive test bodies.
    """
    mock_get_minio.return_value = MagicMock()
    mock_configloader.return_value = MagicMock()
    mock_init_ctx.return_value = {"tables": ctx_tables}

    # Patch core.DataFrame so isinstance(value, DataFrame) can be tested deterministically
    class DummyDataFrame:
        pass

    with patch("data_lakehouse_ingest.core.DataFrame", DummyDataFrame):
        # For the "unknown table" case, we need the value to pass isinstance(value, DataFrame),
        # otherwise we fail earlier in the "value_not_dataframe" branch.
        if isinstance(dataframes, dict) and "table2" in dataframes and dataframes["table2"] == "DF":
            dataframes = {"table2": DummyDataFrame()}

        result = ingest(
            config={},
            spark=mock_spark,
            logger=mock_logger,
            minio_client=None,
            dataframes=dataframes,
        )

    assert result["success"] is False
    assert result["tables"] == []
    assert result["errors"], "Expected at least one error entry"

    err0 = result["errors"][0]
    assert err0["phase"] == "dataframe_validation"
    for s in expected_error_substrings:
        assert s in err0["error"]

    mock_process_tables.assert_not_called()


# Test that ingest() accepts valid dataframe overrides and forwards them
# unchanged to process_tables().
@patch("data_lakehouse_ingest.core.init_run_context")
@patch("data_lakehouse_ingest.core.process_tables")
@patch("data_lakehouse_ingest.core.ConfigLoader")
@patch("data_lakehouse_ingest.core.get_minio_client")
def test_ingest_dataframes_valid_overrides_passed_to_process_tables(
    mock_get_minio,
    mock_configloader,
    mock_process_tables,
    mock_init_ctx,
    mock_spark,
    mock_logger,
):
    mock_get_minio.return_value = MagicMock()
    mock_configloader.return_value = MagicMock()

    mock_init_ctx.return_value = {"tables": [{"name": "table1"}, {"name": "table2"}]}
    mock_process_tables.return_value = (
        [{"name": "table1", "status": "success"}, {"name": "table2", "status": "success"}],
        [],
    )

    df1 = MagicMock(spec=DataFrame)
    df2 = MagicMock(spec=DataFrame)
    overrides = {"table1": df1, "table2": df2}

    result = ingest(
        config={},
        spark=mock_spark,
        logger=mock_logger,
        minio_client=None,
        dataframes=overrides,
    )

    assert result["success"] is True
    mock_process_tables.assert_called_once()
    # Verify the overrides dict is passed through unchanged
    assert mock_process_tables.call_args.kwargs["dataframes"] is overrides
