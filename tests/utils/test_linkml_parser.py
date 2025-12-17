import os
import tempfile
import logging
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from data_lakehouse_ingest.utils.linkml_parser import load_linkml_schema


@pytest.fixture(scope="module")
def spark():
    """Create a simple Spark session for testing."""
    return SparkSession.builder.master("local[1]").appName("linkml-test").getOrCreate()


@pytest.fixture
def logger():
    """Basic logger for unit tests."""
    logger = logging.getLogger("test_linkml_parser")
    logger.setLevel(logging.INFO)
    return logger


# ---------------------------------------------------------------------
# Test: Local YAML file
# ---------------------------------------------------------------------
def test_load_linkml_schema_local(spark, logger):
    """
    Verify that load_linkml_schema correctly parses a simple local YAML schema.
    """

    yaml_content = """
name: TestSchema
id: http://example.org/TestSchema

classes:
  Person:
    slots:
      - id
      - name
      - age

slots:
  id:
    range: integer
  name:
    range: string
  age:
    range: float
"""

    # Create temp YAML file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".yaml") as tmp:
        tmp.write(yaml_content.encode("utf-8"))
        tmp_path = tmp.name

    try:
        schema = load_linkml_schema(
            spark=spark,
            path=tmp_path,
            logger=logger,
            minio_client=None,
        )
    finally:
        os.remove(tmp_path)

    assert isinstance(schema, dict)
    assert schema == {
        "id": "INT",
        "name": "STRING",
        "age": "DOUBLE",
    }


# ---------------------------------------------------------------------
# Test: S3A + MinIO fetch
# ---------------------------------------------------------------------
def test_load_linkml_schema_s3a_minio(spark, logger):
    """
    Ensure that S3A paths use the MinIO client and that the YAML file is parsed.
    """

    yaml_content = """
id: http://example.org/TestSchema
name: TestSchema
classes:
  Sample:
    slots:
      - value

slots:
  value:
    range: boolean
"""

    # Fake MinIO client and fake get_object response
    fake_obj = MagicMock()
    fake_obj.read.return_value = yaml_content.encode("utf-8")
    fake_obj.close = MagicMock()

    minio_client = MagicMock()
    minio_client.get_object.return_value = fake_obj

    schema = load_linkml_schema(
        spark=spark,
        path="s3a://mybucket/path/schema.yaml",
        logger=logger,
        minio_client=minio_client,
    )

    # Verify schema mapping
    assert schema == {"value": "BOOLEAN"}

    # Ensure MinIO client was used
    minio_client.get_object.assert_called_once_with("mybucket", "path/schema.yaml")

    # Ensure fake object was closed
    fake_obj.close.assert_called_once()


# ---------------------------------------------------------------------
# Test: No classes -> error
# ---------------------------------------------------------------------
def test_load_linkml_schema_invalid_schema_raises(spark, logger):
    """
    Test that a schema with no classes raises a ValueError.
    """

    yaml_content = """
id: http://example.org/BadSchema
name: BadSchema
slots:
  id:
    range: integer
"""

    with tempfile.NamedTemporaryFile(delete=False, suffix=".yaml") as tmp:
        tmp.write(yaml_content.encode("utf-8"))
        tmp_path = tmp.name

    try:
        with pytest.raises(ValueError) as excinfo:
            load_linkml_schema(
                spark=spark,
                path=tmp_path,
                logger=logger,
                minio_client=None,
            )
    finally:
        os.remove(tmp_path)

    assert "No classes found" in str(excinfo.value)
