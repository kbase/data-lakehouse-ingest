import logging
from unittest.mock import MagicMock
from data_lakehouse_ingest.orchestrator.init_utils import init_logger, init_run_context


def test_init_logger_creates_default_logger():
    logger = init_logger(None)
    assert isinstance(logger, logging.Logger)


def test_init_run_context_creates_tenant_namespace_with_iceberg():
    from data_lakehouse_ingest.orchestrator import init_utils

    init_utils.create_namespace_if_not_exists = MagicMock(return_value="demo_tenant.demo_dataset")

    mock_spark = MagicMock()
    mock_loader = MagicMock()
    mock_loader.config = {
        "tenant": "demo_tenant",
        "dataset": "demo_dataset",
    }
    mock_loader.get_tables.return_value = [{"name": "table1"}]
    mock_logger = MagicMock()

    ctx = init_run_context(mock_spark, mock_logger, mock_loader)

    assert ctx["tenant"] == "demo_tenant"
    assert ctx["dataset"] == "demo_dataset"
    assert ctx["namespace"] == "demo_tenant.demo_dataset"
    assert ctx["tables"] == [{"name": "table1"}]
    assert "namespace_base_path" not in ctx

    init_utils.create_namespace_if_not_exists.assert_called_once_with(
        mock_spark,
        namespace="demo_dataset",
        tenant_name="demo_tenant",
        iceberg=True,
    )
    mock_spark.sql.assert_any_call("USE demo_tenant.demo_dataset")


def test_init_run_context_creates_personal_namespace_with_iceberg():
    from data_lakehouse_ingest.orchestrator import init_utils

    init_utils.create_namespace_if_not_exists = MagicMock(return_value="my.demo_dataset")

    mock_spark = MagicMock()
    mock_loader = MagicMock()
    mock_loader.config = {
        "tenant": None,
        "dataset": "demo_dataset",
    }
    mock_loader.get_tables.return_value = [{"name": "table1"}]
    mock_logger = MagicMock()

    ctx = init_run_context(mock_spark, mock_logger, mock_loader)

    assert ctx["tenant"] is None
    assert ctx["namespace"] == "my.demo_dataset"

    init_utils.create_namespace_if_not_exists.assert_called_once_with(
        mock_spark,
        "demo_dataset",
        iceberg=True,
    )
    mock_spark.sql.assert_any_call("USE my.demo_dataset")
