import json
from pathlib import Path
import pytest
from unittest.mock import MagicMock
from data_lakehouse_ingest.config_loader import ConfigLoader
from minio.error import S3Error


# ---------------------------------------------------------------------
# Helper fixture: minimal valid config dict
# ---------------------------------------------------------------------
@pytest.fixture
def minimal_config():
    return {
        "tenant": "genomedepot",
        "dataset": "arkinlab",
        "paths": {"bronze_base": "s3a://bucket/bronze", "silver_base": "s3a://bucket/silver"},
        "tables": [
            {
                "name": "browser_cazy_family",
                "schema_sql": "id STRING",
                "bronze_path": "s3a://bucket/bronze/browser_cazy_family.csv",
            }
        ],
        "defaults": {"csv": {"header": True, "delimiter": ",", "inferSchema": False}},
    }


@pytest.fixture
def mock_logger():
    return MagicMock()


# ---------------------------------------------------------------------
# Basic initialization tests
# ---------------------------------------------------------------------
def test_load_from_dict(minimal_config, mock_logger):
    loader = ConfigLoader(minimal_config, logger=mock_logger)
    assert loader.get_tenant() == minimal_config["tenant"]
    mock_logger.info.assert_any_call("Using inline dict configuration")
    mock_logger.info.assert_any_call("Minimal config validation passed")


def test_load_from_inline_json(minimal_config, mock_logger):
    cfg_str = json.dumps(minimal_config)
    loader = ConfigLoader(cfg_str, logger=mock_logger)
    assert isinstance(loader.get_full_config(), dict)
    mock_logger.info.assert_any_call("Parsing inline JSON configuration string")


def test_load_from_local_file(minimal_config, mock_logger):
    safe_file = Path.home() / ".data_lakehouse" / "configs" / "local_test_config.json"
    safe_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(safe_file, "w", encoding="utf-8") as f:
            json.dump(minimal_config, f)
        loader = ConfigLoader(str(safe_file), logger=mock_logger)
        assert loader.get_dataset() == "arkinlab"
        mock_logger.info.assert_any_call(
            f"Loading configuration from local file: {safe_file.resolve()}"
        )
    finally:
        if safe_file.exists():
            safe_file.unlink()


def test_load_from_invalid_json(mock_logger):
    bad_json = "{ invalid json }"
    with pytest.raises(ValueError):
        ConfigLoader(bad_json, logger=mock_logger)
    mock_logger.error.assert_called()


def test_load_from_s3_requires_minio_client(mock_logger):
    with pytest.raises(ValueError):
        ConfigLoader("s3a://bucket/key.json", logger=mock_logger, minio_client=None)
    mock_logger.error.assert_called_once()


# ---------------------------------------------------------------------
# MinIO read success & failure paths
# ---------------------------------------------------------------------
def test_load_from_s3_success(minimal_config, mock_logger):
    fake_json = json.dumps(minimal_config).encode("utf-8")
    fake_response = MagicMock()
    fake_response.read.return_value = fake_json

    mock_minio = MagicMock()
    mock_minio.get_object.return_value.__enter__.return_value = fake_response

    loader = ConfigLoader(
        "s3a://test-bucket/config.json", logger=mock_logger, minio_client=mock_minio
    )
    assert loader.get_tenant().startswith("genomedepot")
    mock_logger.info.assert_any_call(
        "Fetching config from MinIO: bucket=test-bucket, key=config.json"
    )


def test_load_from_s3_failure_s3error(mock_logger):
    mock_minio = MagicMock()
    mock_minio.get_object.side_effect = S3Error(
        code="NoSuchKey",
        message="err",
        resource="bucket/key",
        request_id="123",
        host_id="abc",
        response=None,
    )
    with pytest.raises(RuntimeError):
        ConfigLoader("s3a://test-bucket/config.json", logger=mock_logger, minio_client=mock_minio)
    mock_logger.error.assert_called()


# ---------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------
def test_missing_required_top_level_keys(mock_logger):
    bad_cfg = {"tenant": "t"}  # missing many keys
    with pytest.raises(ValueError):
        ConfigLoader(bad_cfg, logger=mock_logger)
    mock_logger.error.assert_any_call("Missing required top-level keys: ['dataset', 'tables']")


def test_missing_paths_section(mock_logger, minimal_config):
    cfg = dict(minimal_config)
    cfg["paths"] = dict(cfg["paths"])
    del cfg["paths"]["bronze_base"]

    with pytest.raises(ValueError):
        ConfigLoader(cfg, logger=mock_logger)
    mock_logger.error.assert_called()


def test_missing_table_key_no_schema_is_allowed(mock_logger, minimal_config):
    cfg = dict(minimal_config)
    cfg["tables"] = [dict(cfg["tables"][0])]
    del cfg["tables"][0]["schema_sql"]

    loader = ConfigLoader(cfg, logger=mock_logger)
    assert loader.get_table("browser_cazy_family") is not None

    mock_logger.info.assert_any_call(
        "Table 'browser_cazy_family' has no explicit schema ('schema_sql'/'schema'); "
        "schema will be inferred by the loader."
    )


# ---------------------------------------------------------------------
# Accessor tests
# ---------------------------------------------------------------------
def test_accessors(minimal_config, mock_logger):
    loader = ConfigLoader(minimal_config, logger=mock_logger)
    assert loader.get_dataset() == "arkinlab"
    assert "bronze" in loader.get_paths()["bronze_base"]
    assert isinstance(loader.get_tables(), list)
    assert loader.get_table("browser_cazy_family")["name"] == "browser_cazy_family"
    assert loader.get_table("missing_table") is None
    assert loader.get_bronze_path("browser_cazy_family").endswith(".csv")
    assert loader.get_silver_path("abc") == "s3a://bucket/silver/abc"
    assert "delimiter" in loader.get_defaults_for("csv")
    assert loader.summarize()["num_tables"] == 1


def test_get_defaults_for_tsv_inherits_from_csv(minimal_config, mock_logger):
    loader = ConfigLoader(minimal_config, logger=mock_logger)
    defaults = loader.get_defaults_for("tsv")
    assert defaults["delimiter"] == "\t"


def test_get_defaults_for_unknown_format_warns(minimal_config, mock_logger):
    loader = ConfigLoader(minimal_config, logger=mock_logger)
    df = loader.get_defaults_for("parquet")
    assert df["delimiter"] == ","
    mock_logger.warning.assert_any_call(
        "No defaults found for format 'parquet', using safe fallback."
    )


def test_paths_section_can_be_omitted(mock_logger, minimal_config):
    # Remove paths entirely (now allowed)
    cfg = dict(minimal_config)
    cfg.pop("paths", None)

    loader = ConfigLoader(cfg, logger=mock_logger)
    assert loader.get_dataset() == "arkinlab"
    assert loader.get_paths() == {}
    mock_logger.info.assert_any_call(
        "No 'paths' section found in config — skipping base path validation."
    )


def test_get_silver_path_requires_silver_base(mock_logger, minimal_config):
    # Paths present but silver_base missing -> get_silver_path should raise
    cfg = dict(minimal_config)
    cfg["paths"] = {"bronze_base": "s3a://bucket/bronze"}  # omit silver_base

    loader = ConfigLoader(cfg, logger=mock_logger)
    with pytest.raises(ValueError):
        loader.get_silver_path("any_table")


def test_get_bronze_path_substitutes_bronze_base(mock_logger, minimal_config):
    cfg = dict(minimal_config)
    cfg["tables"] = [
        {
            "name": "browser_cazy_family",
            "schema_sql": "id STRING",
            "bronze_path": "${bronze_base}/browser_cazy_family.csv",
        }
    ]

    loader = ConfigLoader(cfg, logger=mock_logger)
    assert loader.get_bronze_path("browser_cazy_family") == (
        "s3a://bucket/bronze/browser_cazy_family.csv"
    )


def test_get_bronze_path_joins_relative_filename_with_bronze_base(mock_logger, minimal_config):
    cfg = dict(minimal_config)
    cfg["tables"] = [
        {
            "name": "browser_cazy_family",
            "schema_sql": "id STRING",
            "bronze_path": "browser_cazy_family.csv",
        }
    ]

    loader = ConfigLoader(cfg, logger=mock_logger)
    assert loader.get_bronze_path("browser_cazy_family") == (
        "s3a://bucket/bronze/browser_cazy_family.csv"
    )


def test_structured_schema_is_accepted(mock_logger, minimal_config):
    cfg = dict(minimal_config)
    cfg["tables"] = [
        {
            "name": "browser_cazy_family",
            "schema": [
                {"column": "id", "type": "STRING", "nullable": True, "comment": "primary id"},
                {"name": "name", "type": "STRING"},
            ],
            "bronze_path": "s3a://bucket/bronze/browser_cazy_family.csv",
        }
    ]

    loader = ConfigLoader(cfg, logger=mock_logger)
    schema = loader.get_table_schema("browser_cazy_family")
    assert isinstance(schema, list)
    assert schema[0]["column"] == "id"


def test_structured_schema_missing_type_raises(mock_logger, minimal_config):
    cfg = dict(minimal_config)
    cfg["tables"] = [
        {
            "name": "browser_cazy_family",
            "schema": [{"column": "id"}],  # missing type
            "bronze_path": "s3a://bucket/bronze/browser_cazy_family.csv",
        }
    ]

    with pytest.raises(ValueError):
        ConfigLoader(cfg, logger=mock_logger)
