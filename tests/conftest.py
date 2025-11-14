import sys
from unittest.mock import MagicMock

def fake_create_namespace_if_not_exists(spark, namespace=None, tenant_name=None, **kwargs):
    # return a fake namespace string like Spark would
    return f"{tenant_name}__{namespace}"

sys.modules.setdefault("berdl_notebook_utils", MagicMock())
sys.modules.setdefault("berdl_notebook_utils.spark", MagicMock())
sys.modules.setdefault(
    "berdl_notebook_utils.spark.database",
    MagicMock(create_namespace_if_not_exists=fake_create_namespace_if_not_exists)
)

# mock for get_spark_session
mock_setup_spark = MagicMock()
mock_setup_spark.get_spark_session = MagicMock(side_effect=ImportError("berdl_notebook_utils not installed"))
sys.modules.setdefault("berdl_notebook_utils.setup_spark_session", mock_setup_spark)