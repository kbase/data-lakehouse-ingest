import pytest
from unittest.mock import MagicMock

@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.read.options.return_value.format.return_value.load.return_value.count.return_value = 3
    return spark

@pytest.fixture
def mock_logger():
    return MagicMock()