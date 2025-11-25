import logging
from unittest.mock import MagicMock
from data_lakehouse_ingest.orchestrator.init_utils import init_logger, init_run_context

def test_init_logger_creates_default_logger():
    logger = init_logger(None)
    assert isinstance(logger, logging.Logger)
    assert logger.name == "data_lakehouse_ingest"

def test_init_run_context_creates_tenant_and_tables():
    from data_lakehouse_ingest.orchestrator import init_utils

    init_utils.create_namespace_if_not_exists = MagicMock(return_value="demo_dataset")

    # Mock Spark
    mock_spark = MagicMock()
    mock_spark.sql.return_value.collect.return_value = [
        MagicMock(info_name="location", info_value="s3a://dummy-path")
    ]
    mock_spark.catalog.setCurrentDatabase = MagicMock()


    mock_loader = MagicMock()
    mock_loader.config = {
        "tenant": "demo_tenant",
        "dataset": "demo_dataset",
    }
    mock_loader.get_tables.return_value = [{"name": "table1"}]
    mock_logger = MagicMock()

    # Run function
    ctx = init_run_context(mock_spark, mock_logger, mock_loader)

    # Validate context
    assert ctx["tenant"] == "demo_tenant"
    assert ctx["dataset"] == "demo_dataset"
    assert ctx["namespace"] == "demo_dataset"
    assert ctx["tables"] == [{"name": "table1"}]
    assert ctx["namespace_base_path"] == "s3a://dummy-path"

    # Validate side effects
    init_utils.create_namespace_if_not_exists.assert_called_once()
    mock_spark.catalog.setCurrentDatabase.assert_called_once_with("demo_dataset")

