import json
import logging
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


# ---------------------------------------------------------------------
# Basic initialization tests
# ---------------------------------------------------------------------
def test_load_from_dict(minimal_config, caplog):
    """Loads configuration from an inline dict and logs successful validation."""

    with caplog.at_level(logging.INFO):
        loader = ConfigLoader(minimal_config)

    assert loader.get_tenant() == minimal_config["tenant"]
    assert "Using inline dict configuration" in caplog.text
    assert "Minimal config validation passed" in caplog.text


def test_load_from_inline_json(minimal_config, caplog):
    """Parses an inline JSON string configuration and logs the parse step."""
    cfg_str = json.dumps(minimal_config)

    with caplog.at_level(logging.INFO):
        loader = ConfigLoader(cfg_str)

    assert isinstance(loader.get_full_config(), dict)
    assert "Parsing inline JSON configuration string" in caplog.text


def test_load_from_local_file(minimal_config, caplog):
    """Loads configuration from a local file inside the safe config directory."""
    safe_file = Path.home() / ".data_lakehouse" / "configs" / "local_test_config.json"
    safe_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(safe_file, "w", encoding="utf-8") as f:
            json.dump(minimal_config, f)

        with caplog.at_level(logging.INFO):
            loader = ConfigLoader(str(safe_file))

        assert loader.get_dataset() == "arkinlab"
        assert f"Loading configuration from local file: {safe_file.resolve()}" in caplog.text
    finally:
        if safe_file.exists():
            safe_file.unlink()


def test_load_from_invalid_json(caplog):
    """Rejects invalid inline JSON and logs a configuration validation error."""
    bad_json = "{ invalid json }"
    with (
        caplog.at_level(logging.ERROR),
        pytest.raises(ValueError, match=r"Invalid configuration source"),
    ):
        ConfigLoader(bad_json)

    assert "Invalid inline JSON configuration" in caplog.text


def test_load_from_s3_requires_minio_client(caplog):
    """Fails fast when loading an s3a:// config without a MinIO client."""
    with (
        caplog.at_level(logging.ERROR),
        pytest.raises(ValueError, match=r"MinIO client must be provided.*s3a://"),
    ):
        ConfigLoader("s3a://bucket/key.json", minio_client=None)

    assert "MinIO client must be provided for s3a:// paths." in caplog.text


# ---------------------------------------------------------------------
# MinIO read success & failure paths
# ---------------------------------------------------------------------
def test_load_from_s3_success(minimal_config, caplog):
    """Successfully loads configuration from MinIO when a valid client is provided."""
    fake_json = json.dumps(minimal_config).encode("utf-8")
    fake_response = MagicMock()
    fake_response.read.return_value = fake_json

    mock_minio = MagicMock()
    mock_minio.get_object.return_value.__enter__.return_value = fake_response

    with caplog.at_level(logging.INFO):
        loader = ConfigLoader("s3a://test-bucket/config.json", minio_client=mock_minio)

    assert loader.get_tenant().startswith("genomedepot")
    assert "Fetching config from MinIO: bucket=test-bucket, key=config.json" in caplog.text


def test_load_from_s3_failure_s3error(caplog):
    """Raises and logs a runtime error when MinIO fails to read the config object."""
    mock_minio = MagicMock()
    mock_minio.get_object.side_effect = S3Error(
        code="NoSuchKey",
        message="err",
        resource="bucket/key",
        request_id="123",
        host_id="abc",
        response=None,
    )

    with (
        caplog.at_level(logging.ERROR),
        pytest.raises(
            RuntimeError, match=r"Failed to read s3a://test-bucket/config\.json from MinIO"
        ),
    ):
        ConfigLoader("s3a://test-bucket/config.json", minio_client=mock_minio)

    assert "Failed to read s3a://test-bucket/config.json from MinIO" in caplog.text


# ---------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------
def test_missing_required_top_level_keys():
    """Raises a validation error when required top-level config keys are missing."""
    bad_cfg = {"tenant": "t"}  # missing many keys
    with pytest.raises(ValueError) as excinfo:
        ConfigLoader(bad_cfg)

    msg = str(excinfo.value)
    assert "Missing required top-level keys: ['dataset', 'tables']" in msg
    assert "Config must contain a non-empty 'tables' list" in msg


def test_missing_paths_section_logs_and_raises(minimal_config, caplog):
    """Logs and raises a validation error when paths.bronze_base is missing."""
    cfg = minimal_config.copy()  # avoid mutating fixture
    cfg["paths"] = cfg["paths"].copy()
    del cfg["paths"]["bronze_base"]

    with caplog.at_level(logging.ERROR), pytest.raises(ValueError) as excinfo:
        ConfigLoader(cfg)

    assert "Missing required path keys: ['bronze_base']" in str(excinfo.value)
    assert "Config validation failed" in caplog.text
    assert "Missing required path keys: ['bronze_base']" in caplog.text


def test_missing_table_key_no_schema_is_allowed(minimal_config, caplog):
    """Allows tables without explicit schema definitions and logs schema inference."""
    cfg = minimal_config.copy()
    cfg["tables"] = [cfg["tables"][0].copy()]
    del cfg["tables"][0]["schema_sql"]

    with caplog.at_level(logging.INFO):
        loader = ConfigLoader(cfg)

    assert loader.get_table("browser_cazy_family") is not None
    assert (
        "Table 'browser_cazy_family' has no explicit schema ('schema_sql'/'schema'); "
        "schema will be inferred by the loader."
    ) in caplog.text


def test_paths_must_be_object_map_when_provided(minimal_config):
    """Rejects configs where 'paths' is provided but is not an object/map."""
    cfg = minimal_config.copy()
    cfg["paths"] = "not-a-dict"

    with pytest.raises(ValueError, match=r"'paths' must be an object/map when provided\."):
        ConfigLoader(cfg)


def test_table_entry_must_be_object_map(minimal_config):
    """Rejects a tables list containing a non-dict entry."""
    cfg = minimal_config.copy()
    cfg["tables"] = ["not-a-dict"]

    with pytest.raises(ValueError, match=r"Table entry at index 0 must be an object/map\."):
        ConfigLoader(cfg)


def test_table_missing_name_raises(minimal_config):
    """Rejects a table entry that is missing the required 'name' field."""
    cfg = minimal_config.copy()
    cfg["tables"] = [
        {
            # missing name
            "schema_sql": "id STRING",
            "bronze_path": "s3a://bucket/bronze/x.csv",
        }
    ]

    with pytest.raises(ValueError, match=r"Table entry at index 0 missing required key: name"):
        ConfigLoader(cfg)


@pytest.mark.parametrize("bad_name", ["", "   ", None, 123])
def test_table_invalid_name_raises(minimal_config, bad_name):
    """Rejects table entries with invalid/blank/non-string names."""
    cfg = minimal_config.copy()
    cfg["tables"] = [
        {"name": bad_name, "schema_sql": "id STRING", "bronze_path": "s3a://bucket/bronze/x.csv"}
    ]

    with pytest.raises(
        ValueError,
        match=r"Table entry at index 0 has invalid 'name' \(must be non-empty string\)\.",
    ):
        ConfigLoader(cfg)


def test_table_schema_must_be_list_when_provided(minimal_config):
    """Rejects tables where 'schema' is provided but is not a list."""
    cfg = minimal_config.copy()
    cfg["tables"] = [
        {
            "name": "browser_cazy_family",
            "schema": "not-a-list",
            "bronze_path": "s3a://bucket/bronze/browser_cazy_family.csv",
        }
    ]

    with pytest.raises(
        ValueError,
        match=r"Table 'browser_cazy_family' schema must be a list when provided\.",
    ):
        ConfigLoader(cfg)


def test_table_schema_sql_must_be_string_or_null(minimal_config):
    """Rejects tables where 'schema_sql' is provided but is not a string (or null)."""
    cfg = minimal_config.copy()
    cfg["tables"] = [
        {
            "name": "browser_cazy_family",
            "schema_sql": 123,  # invalid type
            "bronze_path": "s3a://bucket/bronze/browser_cazy_family.csv",
        }
    ]

    with pytest.raises(
        ValueError,
        match=r"Table 'browser_cazy_family' schema_sql must be a string or null\.",
    ):
        ConfigLoader(cfg)


def test_structured_schema_entry_must_be_object_map(minimal_config):
    """Rejects structured schemas that contain a non-dict schema entry."""
    cfg = minimal_config.copy()
    cfg["tables"] = [
        {
            "name": "browser_cazy_family",
            "schema": ["not-a-dict"],
            "bronze_path": "s3a://bucket/bronze/browser_cazy_family.csv",
        }
    ]

    with pytest.raises(
        ValueError,
        match=r"Table 'browser_cazy_family' schema entry at index 0 must be an object/map\.",
    ):
        ConfigLoader(cfg)


def test_structured_schema_entry_missing_column_or_name_raises(minimal_config):
    """Rejects structured schema entries missing both 'column' and 'name'."""
    cfg = minimal_config.copy()
    cfg["tables"] = [
        {
            "name": "browser_cazy_family",
            "schema": [{"type": "STRING"}],  # missing column/name
            "bronze_path": "s3a://bucket/bronze/browser_cazy_family.csv",
        }
    ]

    with pytest.raises(
        ValueError,
        match=r"Table 'browser_cazy_family' schema entry at index 0 missing 'column' \(or 'name'\)\.",
    ):
        ConfigLoader(cfg)


def test_structured_schema_nullable_and_comment_type_validation(minimal_config):
    """Rejects structured schemas where 'nullable' is non-boolean and/or 'comment' is neither a string nor a dict."""
    cfg = minimal_config.copy()
    cfg["tables"] = [
        {
            "name": "browser_cazy_family",
            "schema": [
                {"column": "id", "type": "STRING", "nullable": "yes", "comment": 123},
            ],
            "bronze_path": "s3a://bucket/bronze/browser_cazy_family.csv",
        }
    ]

    with pytest.raises(ValueError) as excinfo:
        ConfigLoader(cfg)

    msg = str(excinfo.value)
    assert "has non-boolean 'nullable'." in msg
    assert "has non-string 'comment'." in msg


def test_structured_schema_accepts_dict_comment(minimal_config):
    """Accepts structured schema comments when provided as dictionaries."""
    cfg = minimal_config.copy()
    cfg["tables"] = [
        {
            "name": "browser_cazy_family",
            "schema": [
                {
                    "column": "id",
                    "type": "STRING",
                    "nullable": True,
                    "comment": {"description": "primary id"},
                }
            ],
            "bronze_path": "s3a://bucket/bronze/browser_cazy_family.csv",
        }
    ]

    loader = ConfigLoader(cfg)
    schema = loader.get_table_schema("browser_cazy_family")

    assert schema is not None
    assert schema[0]["comment"] == {"description": "primary id"}


def test_paths_present_missing_bronze_base_raises(minimal_config):
    """Rejects configs that include 'paths' but omit required paths.bronze_base."""
    cfg = minimal_config.copy()
    cfg["paths"] = {"silver_base": "s3a://bucket/silver"}  # omit bronze_base
    cfg["tables"] = [
        {"name": "browser_cazy_family", "schema_sql": "id STRING", "bronze_path": "file.csv"}
    ]

    with pytest.raises(
        ValueError,
        match=r"Missing required path keys: \['bronze_base'\]",
    ):
        ConfigLoader(cfg)


# ---------------------------------------------------------------------
# Accessor tests
# ---------------------------------------------------------------------
def test_accessors(minimal_config):
    """Verifies basic accessors and derived values from a valid configuration."""
    loader = ConfigLoader(minimal_config)
    assert loader.get_dataset() == "arkinlab"
    assert "bronze" in loader.get_paths()["bronze_base"]
    assert isinstance(loader.get_tables(), list)
    assert loader.get_table("browser_cazy_family")["name"] == "browser_cazy_family"
    assert loader.get_table("missing_table") is None
    assert loader.get_bronze_path("browser_cazy_family").endswith(".csv")
    assert loader.get_silver_path("abc") == "s3a://bucket/silver/abc"
    assert "delimiter" in loader.get_defaults_for("csv")
    assert loader.summarize()["num_tables"] == 1


def test_get_defaults_for_tsv_inherits_from_csv(minimal_config):
    """Inherits TSV defaults from CSV defaults with a tab delimiter."""
    loader = ConfigLoader(minimal_config)
    defaults = loader.get_defaults_for("tsv")
    assert defaults["delimiter"] == "\t"


def test_get_defaults_for_unknown_format_warns(minimal_config, caplog):
    """Logs a warning and returns fallback defaults for unknown formats."""
    with caplog.at_level(logging.WARNING):
        loader = ConfigLoader(minimal_config)
        df = loader.get_defaults_for("parquet")

    assert df["delimiter"] == ","
    assert "No defaults found for format 'parquet', using safe fallback." in caplog.text


def test_paths_section_can_be_omitted(minimal_config, caplog):
    """Allows configs without a paths section and skips base path validation."""
    cfg = minimal_config.copy()
    cfg.pop("paths", None)

    with caplog.at_level(logging.INFO):
        loader = ConfigLoader(cfg)

    assert loader.get_dataset() == "arkinlab"
    assert loader.get_paths() == {}
    assert "No 'paths' section found in config — skipping base path validation." in caplog.text


def test_get_silver_path_requires_silver_base(minimal_config):
    """Raises an error when resolving silver paths without paths.silver_base."""
    cfg = minimal_config.copy()
    cfg["paths"] = {"bronze_base": "s3a://bucket/bronze"}  # omit silver_base

    loader = ConfigLoader(cfg)
    with pytest.raises(ValueError, match=r"silver_base"):
        loader.get_silver_path("any_table")


def test_get_bronze_path_substitutes_bronze_base(minimal_config):
    """Substitutes ${bronze_base} when resolving a table's bronze path."""
    cfg = minimal_config.copy()
    cfg["tables"] = [
        {
            "name": "browser_cazy_family",
            "schema_sql": "id STRING",
            "bronze_path": "${bronze_base}/browser_cazy_family.csv",
        }
    ]

    loader = ConfigLoader(cfg)
    assert loader.get_bronze_path("browser_cazy_family") == (
        "s3a://bucket/bronze/browser_cazy_family.csv"
    )


def test_get_bronze_path_joins_relative_filename_with_bronze_base(minimal_config):
    """Joins relative bronze paths with paths.bronze_base."""
    cfg = minimal_config.copy()
    cfg["tables"] = [
        {
            "name": "browser_cazy_family",
            "schema_sql": "id STRING",
            "bronze_path": "browser_cazy_family.csv",
        }
    ]

    loader = ConfigLoader(cfg)
    assert loader.get_bronze_path("browser_cazy_family") == (
        "s3a://bucket/bronze/browser_cazy_family.csv"
    )


def test_structured_schema_is_accepted(minimal_config):
    """Accepts a valid structured schema and returns it via the schema accessor."""
    cfg = minimal_config.copy()
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

    loader = ConfigLoader(cfg)
    schema = loader.get_table_schema("browser_cazy_family")
    assert isinstance(schema, list)
    assert schema[0]["column"] == "id"


def test_structured_schema_missing_type_raises(minimal_config):
    """Raises a validation error when a structured schema column is missing a type."""
    cfg = minimal_config.copy()
    cfg["tables"] = [
        {
            "name": "browser_cazy_family",
            "schema": [{"column": "id"}],  # missing type
            "bronze_path": "s3a://bucket/bronze/browser_cazy_family.csv",
        }
    ]

    with pytest.raises(ValueError, match=r"missing 'type'"):
        ConfigLoader(cfg)


def test_get_table_schema_returns_none_when_table_missing(minimal_config, caplog):
    """Returns None for get_table_schema() when the requested table is missing."""
    loader = ConfigLoader(minimal_config)

    with caplog.at_level(logging.WARNING):
        schema = loader.get_table_schema("does_not_exist")

    assert schema is None
    assert "Requested table 'does_not_exist' not found in configuration." in caplog.text


def test_get_bronze_path_raises_when_table_not_found(minimal_config):
    """Raises a clear error when get_bronze_path() is called for a missing table."""
    loader = ConfigLoader(minimal_config)

    with pytest.raises(
        ValueError,
        match=r"Cannot resolve bronze path — table 'missing' not found in configuration\.",
    ):
        loader.get_bronze_path("missing")


@pytest.mark.parametrize("bad_bronze_path", [None, 123, {}, []])
def test_get_bronze_path_requires_string_bronze_path(minimal_config, bad_bronze_path):
    """Requires 'bronze_path' to be a non-empty string when resolving bronze paths."""
    cfg = minimal_config.copy()
    cfg["tables"] = [
        {"name": "browser_cazy_family", "schema_sql": "id STRING", "bronze_path": bad_bronze_path}
    ]
    loader = ConfigLoader(cfg)

    with pytest.raises(
        ValueError,
        match=r"'bronze_path' must be defined as a string for table 'browser_cazy_family'\.",
    ):
        loader.get_bronze_path("browser_cazy_family")
