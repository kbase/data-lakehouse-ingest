import pytest
from data_lakehouse_ingest.loaders.xml_loader import load_xml_data

# TODO: Add more comprehensive tests to cover valid and invalid XML data cases.


def test_load_xml_data_success(mock_spark, mock_logger):
    opts = {"rowTag": "record"}
    df = load_xml_data(mock_spark, "s3a://bucket/data.xml", opts, mock_logger)
    assert df is not None
    mock_logger.info.assert_any_call("📂 Reading XML data from: s3a://bucket/data.xml")


def test_load_xml_data_missing_rowtag(mock_spark, mock_logger):
    with pytest.raises(ValueError):
        load_xml_data(mock_spark, "data.xml", {}, mock_logger)
    mock_logger.error.assert_called_once()


def test_load_xml_data_failure(mock_spark, mock_logger):
    opts = {"rowTag": "entry"}
    mock_spark.read.format.return_value.options.side_effect = Exception("parse fail")
    with pytest.raises(Exception):
        load_xml_data(mock_spark, "bad.xml", opts, mock_logger)
    mock_logger.error.assert_called()
