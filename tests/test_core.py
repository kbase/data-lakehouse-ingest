import json
import tempfile
from data_lakehouse_ingest import ingest_from_config

def test_ingest_from_config_with_dict():
    cfg = {"input": {"uri": "s3a://bucket/path"}, "target": {"table": "demo"}}
    assert ingest_from_config(cfg) is True

def test_ingest_from_config_with_json_file():
    cfg = {"input": {"uri": "s3a://bucket/path"}, "target": {"table": "demo"}}
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(cfg, f)
        f.flush()
        result = ingest_from_config(f.name)
    assert result is True
