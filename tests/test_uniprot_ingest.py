import pytest
import tempfile
import os
from unittest.mock import MagicMock, patch
from data_lakehouse_ingest.parsers.uniprot_ingest import process_uniprot_to_delta, copy_s3a_to_local


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------
@pytest.fixture
def mock_logger():
    return MagicMock()


@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark._jsc.hadoopConfiguration.return_value = "hadoop_conf"
    spark._jvm.java.net.URI.return_value = "uri"
    spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value = MagicMock()
    spark._jvm.org.apache.hadoop.fs.Path.side_effect = lambda x: x
    return spark


# ---------------------------------------------------------------------
# copy_s3a_to_local tests
# ---------------------------------------------------------------------
def test_copy_s3a_to_local_non_s3_path(mock_spark):
    local_path = "/tmp/local.xml"
    result = copy_s3a_to_local(mock_spark, local_path)
    assert result == local_path  # should return as-is


def test_copy_s3a_to_local_s3_path(mock_spark, tmp_path):
    s3_path = "s3a://test-bucket/path/to/file.xml"
    result = copy_s3a_to_local(mock_spark, s3_path)
    assert os.path.exists(result)
    assert result.endswith(".xml")
    # Verify Hadoop API calls
    mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.assert_called_once()
    mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value.copyToLocalFile.assert_called_once()


# ---------------------------------------------------------------------
# process_uniprot_to_delta tests
# ---------------------------------------------------------------------
@patch("data_lakehouse_ingest.parsers.uniprot_ingest.write_uniprot_to_delta")
@patch("data_lakehouse_ingest.parsers.uniprot_ingest.parse_entries")
@patch("data_lakehouse_ingest.parsers.uniprot_ingest.get_spark_session")
def test_process_uniprot_to_delta_local_path(
    mock_get_spark_session,
    mock_parse_entries,
    mock_write_to_delta,
    tmp_path,
):
    # Mock Spark
    mock_spark = MagicMock()
    mock_get_spark_session.return_value = mock_spark
    mock_parse_entries.return_value = (100, 5)

    # Run ingestion for local XML
    xml_path = str(tmp_path / "uniprot_sample.xml")
    open(xml_path, "w").write("<root></root>")

    process_uniprot_to_delta(
        xml_path=xml_path,
        namespace="uniprot_ns",
        s3_silver_base="s3a://silver-dest",
        batch_size=10,
    )

    mock_get_spark_session.assert_called_once_with("uniprot_ns")
    mock_parse_entries.assert_called_once()
    mock_write_to_delta.assert_called_once()
    # Should drop/create tables and stop Spark
    mock_spark.sql.assert_any_call("DROP TABLE IF EXISTS `uniprot_ns`.`entities`")
    mock_spark.stop.assert_called_once()


@patch("data_lakehouse_ingest.parsers.uniprot_ingest.copy_s3a_to_local")
@patch("data_lakehouse_ingest.parsers.uniprot_ingest.write_uniprot_to_delta")
@patch("data_lakehouse_ingest.parsers.uniprot_ingest.parse_entries")
@patch("data_lakehouse_ingest.parsers.uniprot_ingest.get_spark_session")
def test_process_uniprot_to_delta_s3_path(
    mock_get_spark_session,
    mock_parse_entries,
    mock_write_to_delta,
    mock_copy_s3a_to_local,
    tmp_path,
):
    mock_spark = MagicMock()
    mock_get_spark_session.return_value = mock_spark
    mock_parse_entries.return_value = (50, 2)
    mock_copy_s3a_to_local.return_value = "/tmp/local_copy.xml"

    process_uniprot_to_delta(
        xml_path="s3a://my-bucket/uniprot.xml",
        namespace="uniprot_ns",
        s3_silver_base="s3a://silver-dest",
    )

    # Ensure copy was invoked for s3a path
    mock_copy_s3a_to_local.assert_called_once_with(mock_spark, "s3a://my-bucket/uniprot.xml")
    mock_write_to_delta.assert_called_once()
    mock_spark.sql.assert_any_call("DROP TABLE IF EXISTS `uniprot_ns`.`entities`")
    mock_spark.stop.assert_called_once()
