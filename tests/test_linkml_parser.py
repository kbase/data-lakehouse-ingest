import pytest
import tempfile
import os
from unittest.mock import MagicMock, patch
from data_lakehouse_ingest.utils.linkml_parser import load_linkml_schema


@pytest.fixture
def mock_logger():
    return MagicMock()


# ---------------------------------------------------------------------
# Helper to make fake slot
# ---------------------------------------------------------------------
class FakeSlot:
    def __init__(self, name, range):
        self.name = name
        self.range = range


# ---------------------------------------------------------------------
@patch("linkml_runtime.utils.schemaview.SchemaView")
def test_load_linkml_schema_local_success(mock_schema_view, mock_logger):
    mock_view = MagicMock()
    mock_view.all_classes.return_value = {"Genome": "class_def"}
    mock_view.get_class.return_value = MagicMock(slots=["id", "name"])
    mock_view.induced_slot.side_effect = [
        FakeSlot("id", "string"),
        FakeSlot("name", "integer"),
    ]
    mock_schema_view.return_value = mock_view

    result = load_linkml_schema(None, "local/path/schema.yaml", mock_logger)
    assert result == {"id": "STRING", "name": "INT"}
    mock_logger.info.assert_any_call("🧩 Parsing LinkML schema from local/path/schema.yaml")
    mock_logger.info.assert_any_call("✅ Derived schema_sql from LinkML: id STRING, name INT")


# ---------------------------------------------------------------------
@patch("linkml_runtime.utils.schemaview.SchemaView")
def test_load_linkml_schema_s3_success(mock_schema_view, mock_logger):
    fake_yaml = "classes:\n  Example:\n    slots: [id]\nslots:\n  id:\n    range: string\n"

    fake_response = MagicMock()
    fake_response.read.return_value = fake_yaml.encode("utf-8")
    fake_response.close.return_value = None

    mock_minio = MagicMock()
    mock_minio.get_object.return_value = fake_response

    mock_view = MagicMock()
    mock_view.all_classes.return_value = {"Example": "class_def"}
    mock_view.get_class.return_value = MagicMock(slots=["id"])
    mock_view.induced_slot.return_value = FakeSlot("id", "string")
    mock_schema_view.return_value = mock_view

    result = load_linkml_schema(None, "s3a://mybucket/schema.yaml", mock_logger, minio_client=mock_minio)
    assert result == {"id": "STRING"}
    mock_logger.info.assert_any_call("🧩 Parsing LinkML schema from s3a://mybucket/schema.yaml")

    tmpfiles = [f for f in os.listdir(tempfile.gettempdir()) if f.endswith(".yaml")]
    assert not tmpfiles


# ---------------------------------------------------------------------
@patch("linkml_runtime.utils.schemaview.SchemaView", side_effect=Exception("cannot open file"))
def test_load_linkml_schema_failure(mock_schema_view, mock_logger):
    with pytest.raises(Exception):
        load_linkml_schema(None, "local/path/schema.yaml", mock_logger)
    mock_logger.error.assert_called_once()
    assert "❌ Failed to load SchemaView" in mock_logger.error.call_args[0][0]


# ---------------------------------------------------------------------
@patch("linkml_runtime.utils.schemaview.SchemaView")
def test_load_linkml_schema_no_classes(mock_schema_view, mock_logger):
    mock_view = MagicMock()
    mock_view.all_classes.return_value = {}
    mock_schema_view.return_value = mock_view

    with pytest.raises(ValueError, match="No classes found in LinkML schema"):
        load_linkml_schema(None, "local/path/schema.yaml", mock_logger)


# ---------------------------------------------------------------------
@patch("linkml_runtime.utils.schemaview.SchemaView")
def test_load_linkml_schema_unknown_types(mock_schema_view, mock_logger):
    mock_view = MagicMock()
    mock_view.all_classes.return_value = {"Genome": "class_def"}
    mock_view.get_class.return_value = MagicMock(slots=["mystery"])
    mock_view.induced_slot.return_value = FakeSlot("mystery", "weirdtype")
    mock_schema_view.return_value = mock_view

    result = load_linkml_schema(None, "local/schema.yaml", mock_logger)
    assert result == {"mystery": "STRING"}
