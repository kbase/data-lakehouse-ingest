import logging
from unittest.mock import MagicMock
from data_lakehouse_ingest.orchestrator.init_utils import init_logger, init_run_context

def test_init_logger_creates_default_logger():
    logger = init_logger(None)
    assert isinstance(logger, logging.Logger)
    assert logger.name == "data_lakehouse_ingest"

def test_init_run_context_creates_tenant_and_tables():
    mock_spark = MagicMock()
    mock_loader = MagicMock()
    mock_loader.get_tenant.return_value = "demo_tenant"
    mock_loader.get_tables.return_value = [{"name": "table1"}]
    mock_loader.get_all_defaults.return_value = {"csv": {"header": True}}
    mock_logger = MagicMock()

    ctx = init_run_context(mock_spark, mock_logger, mock_loader)
    assert ctx["tenant"] == "demo_tenant"
    assert "tables" in ctx
    mock_spark.sql.assert_called_with("CREATE DATABASE IF NOT EXISTS `demo_tenant`")
