import sys
import logging
import pytest
from unittest.mock import MagicMock


def fake_create_namespace_if_not_exists(spark, namespace=None, tenant_name=None, **kwargs):
    # return a fake namespace string like Spark would
    return f"{tenant_name}__{namespace}"


# IMPORTANT: install the berdl_notebook_utils mocks BEFORE importing any
# data_lakehouse_ingest module. data_lakehouse_ingest.core has a top-level
# `from berdl_notebook_utils.clients import get_s3_client`, so if we let the
# real package load from the venv first, sys.modules.setdefault below becomes
# a no-op and the test suite breaks any time the upstream API surface drifts.
sys.modules.setdefault("berdl_notebook_utils", MagicMock())
sys.modules.setdefault("berdl_notebook_utils.spark", MagicMock())
sys.modules.setdefault(
    "berdl_notebook_utils.spark.database",
    MagicMock(create_namespace_if_not_exists=fake_create_namespace_if_not_exists),
)

# mock for get_spark_session
mock_setup_spark = MagicMock()
mock_setup_spark.get_spark_session = MagicMock(
    side_effect=ImportError("berdl_notebook_utils not installed")
)
sys.modules.setdefault("berdl_notebook_utils.setup_spark_session", mock_setup_spark)

# mock for get_s3_client
mock_clients = MagicMock()
mock_clients.get_s3_client = MagicMock(return_value=MagicMock())
sys.modules.setdefault("berdl_notebook_utils.clients", mock_clients)

# Safe to import data_lakehouse_ingest now that the upstream mocks are in place.
import data_lakehouse_ingest.logger as logger_module  # noqa: E402


@pytest.fixture(autouse=True)
def reset_logger_singleton():
    # Reset our singleton
    logger_module._logger_instance = None

    # Also reset the underlying logging.Logger state
    logger = logging.getLogger("pipeline_logger")

    # Remove handlers
    for h in logger.handlers[:]:
        h.close()
        logger.removeHandler(h)

    # Remove filters
    logger.filters.clear()

    # Remove custom attributes if present
    if hasattr(logger, "context_filter"):
        del logger.context_filter
    if hasattr(logger, "log_file_path"):
        del logger.log_file_path

    yield

    logger_module._logger_instance = None
