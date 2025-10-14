import pytest
from unittest.mock import MagicMock
from data_lakehouse_ingest.utils.uniprot_writer import write_uniprot_to_delta


@pytest.fixture
def mock_spark():
    spark = MagicMock()
    df = MagicMock()
    writer = MagicMock()
    reader = MagicMock()

    # CreateDataFrame -> df
    spark.createDataFrame.return_value = df

    # df.write.format("delta").mode("overwrite")... chain
    df.write.format.return_value = writer
    writer.mode.return_value = writer
    writer.option.return_value = writer
    writer.save.return_value = None

    # spark.read.format("delta").load(...) -> reader
    spark.read.format.return_value = reader
    reader.load.return_value = reader
    reader.count.return_value = 123

    return spark


@pytest.fixture
def mock_logger():
    return MagicMock()


# ---------------------------------------------------------------------
# 1️⃣ Skip empty tables
# ---------------------------------------------------------------------
def test_write_uniprot_to_delta_skips_empty_table(mock_spark, mock_logger):
    tables = {"entities": ([], ["id", "name"])}  # empty records
    write_uniprot_to_delta(mock_spark, tables, "ns", "s3a://bucket/base", mock_logger)
    mock_logger.info.assert_any_call("Skipping empty UniProt table: entities")
    mock_spark.createDataFrame.assert_not_called()


# ---------------------------------------------------------------------
# 2️⃣ Write to s3a:// path
# ---------------------------------------------------------------------
def test_write_uniprot_to_delta_s3_path(mock_spark, mock_logger):
    records = [{"id": 1, "name": "protein1"}]
    schema = ["id", "name"]
    tables = {"entities": (records, schema)}

    write_uniprot_to_delta(mock_spark, tables, "ns", "s3a://bucket/silver", mock_logger)

    # Ensure correct S3 path used
    expected_path = "s3a://bucket/silver/entities"
    mock_logger.info.assert_any_call("💾 Writing UniProt table ns.entities to s3a://bucket/silver/entities")
    mock_spark.createDataFrame.assert_called_once_with(records, schema)
    mock_spark.sql.assert_any_call(f"""
            CREATE TABLE IF NOT EXISTS `ns`.`entities`
            USING DELTA
            LOCATION '{expected_path}'
        """)
    mock_logger.info.assert_any_call("✅ Table ns.entities written successfully with 123 rows.")


# ---------------------------------------------------------------------
# 3️⃣ Write to local filesystem path (non-s3a)
# ---------------------------------------------------------------------
def test_write_uniprot_to_delta_local_path(mock_spark, mock_logger, tmp_path):
    local_dir = tmp_path / "silver"
    local_dir.mkdir()
    records = [{"id": 1, "name": "protein2"}]
    schema = ["id", "name"]
    tables = {"proteins": (records, schema)}

    write_uniprot_to_delta(mock_spark, tables, "ns", str(local_dir), mock_logger)

    expected_path = f"{local_dir}/proteins"
    mock_logger.info.assert_any_call(f"💾 Writing UniProt table ns.proteins to {expected_path}")
    mock_spark.createDataFrame.assert_called_once_with(records, schema)
    mock_spark.sql.assert_any_call(f"""
            CREATE TABLE IF NOT EXISTS `ns`.`proteins`
            USING DELTA
            LOCATION '{expected_path}'
        """)
    mock_logger.info.assert_any_call("✅ Table ns.proteins written successfully with 123 rows.")


# ---------------------------------------------------------------------
# 4️⃣ Multiple tables written
# ---------------------------------------------------------------------
def test_write_uniprot_to_delta_multiple_tables(mock_spark, mock_logger):
    records = [{"id": 1}]
    schema = ["id"]
    tables = {
        "entities": (records, schema),
        "names": (records, schema),
    }

    write_uniprot_to_delta(mock_spark, tables, "ns", "s3a://bucket/base", mock_logger)

    # Should call DataFrame creation twice (for 2 non-empty tables)
    assert mock_spark.createDataFrame.call_count == 2
    mock_logger.info.assert_any_call("✅ Table ns.entities written successfully with 123 rows.")
    mock_logger.info.assert_any_call("✅ Table ns.names written successfully with 123 rows.")
