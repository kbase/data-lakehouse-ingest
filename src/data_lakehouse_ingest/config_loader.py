"""
Purpose:
    Provides a configuration loader for the Data Lakehouse Ingest framework.
    Supports reading configuration from:
      - Local JSON file (with path traversal protection)
      - Inline JSON string
      - MinIO object storage (s3a://bucket/key)

    Performs validation of minimal required fields, applies defensive checks
    (e.g., path traversal prevention), and exposes convenient accessors for
    tenant, dataset, paths, and per-table definitions.

    Also supports structured metadata such as table-level and column-level comments
    for enhanced data context and discoverability.
"""

from __future__ import annotations
from typing import Any
import json
import logging
from minio.error import S3Error
from pathlib import Path
from minio import Minio

# Directory under the user's home where configuration files are safely stored.
# Prevents path traversal outside this sandbox.
SAFE_CONFIG_DIR = Path.home().joinpath(".data_lakehouse", "configs").resolve()
SAFE_CONFIG_DIR.mkdir(parents=True, exist_ok=True)


class ConfigLoader:
    """
    ConfigLoader is responsible for safely loading and validating ingestion
    configuration files used by the Data Lakehouse Ingest framework.

    It can load configurations from:
      - Local JSON files (with path traversal protection)
      - Inline JSON strings
      - MinIO object storage (via s3a:// paths)

    Notes:
        - Local file loading is restricted to a safe configuration directory.
        - Validation supports both SQL-style and structured schema definitions.
        - Table-level and column-level comments may be provided as plain strings
          or JSON-style dictionaries.

    Attributes:
        config (dict[str, Any]): The parsed configuration dictionary.
        logger (logging.Logger): The logger instance for structured logging.
        minio_client (Minio): Optional MinIO client for accessing remote configs.
    """

    def __init__(
        self,
        cfg: str | dict[str, Any],
        logger: logging.Logger | None = None,
        minio_client: Minio | None = None,
    ):
        """
        Initialize ConfigLoader.

        Args:
            cfg (str | dict[str, Any]): Path, inline JSON, or dict representing the configuration.
            logger (logging.Logger | None): Optional logger instance.
            minio_client (Minio | None): MinIO client (required for s3a:// paths).

        Raises:
            ValueError: If MinIO client is missing for s3a:// path or config is invalid.
        """
        self.logger = logger or logging.getLogger(__name__)
        self.minio_client = minio_client

        if isinstance(cfg, str) and cfg.startswith("s3a://") and self.minio_client is None:
            self.logger.error("MinIO client must be provided for s3a:// paths.")
            raise ValueError(
                "MinIO client must be provided when loading configuration from an s3a:// path."
            )

        try:
            self.config: dict[str, Any] = self._load_config(cfg)
            self.logger.info("Config loaded successfully")
            self._validate_minimal_fields()
        except Exception as e:
            self.logger.error(f"ConfigLoader initialization failed: {e}", exc_info=True)
            raise

    # ----------------------------------------------------------------------
    # Internal helpers
    # ----------------------------------------------------------------------
    def _load_config(self, cfg: str | dict[str, Any]) -> dict[str, Any]:
        """
        Load configuration from dict, local file, MinIO path, or inline JSON string.

        Args:
            cfg (str | dict[str, Any]): Configuration source.

        Returns:
            dict[str, Any]: Parsed configuration dictionary.

        Raises:
            ValueError: If config source is invalid or unsafe.
        """
        if isinstance(cfg, dict):
            self.logger.info("Using inline dict configuration")
            return cfg

        # --- MinIO path (s3a://bucket/key) ---
        if isinstance(cfg, str) and cfg.startswith("s3a://"):
            path = cfg.replace("s3a://", "")
            parts = path.split("/", 1)
            if len(parts) != 2:
                raise ValueError(f"Invalid s3a:// path format: {cfg}. Expected s3a://bucket/key")
            bucket, key = parts
            self.logger.info(f"Fetching config from MinIO: bucket={bucket}, key={key}")
            try:
                # Automatically handles closing the connection, even on exceptions
                with self.minio_client.get_object(bucket, key) as response:
                    raw_bytes = response.read()
                    data = json.loads(raw_bytes.decode("utf-8"))
                    return data
            except S3Error as e:
                self.logger.error(f"Failed to read {cfg} from MinIO: {e}", exc_info=True)
                raise RuntimeError(f"Failed to read {cfg} from MinIO: {e}")
            except Exception as e:
                self.logger.error(
                    f"Unexpected error while reading {cfg} from MinIO: {e}", exc_info=True
                )
                raise

        # --- Inline JSON string ---
        if isinstance(cfg, str) and cfg.strip().startswith(("{", "[")):
            self.logger.info("Parsing inline JSON configuration string")
            try:
                return json.loads(cfg)
            except Exception as e:
                self.logger.error(f"Invalid inline JSON configuration: {e}", exc_info=True)
                raise ValueError(f"Invalid configuration source: {e}")

        # --- Local file ---
        requested_path = Path(cfg).resolve()
        if not str(requested_path).startswith(str(SAFE_CONFIG_DIR)):
            msg = (
                f"Unsafe or out-of-sandbox config path detected.\n"
                f"Provided: {requested_path}\n"
                f"Allowed base directory: {SAFE_CONFIG_DIR}\n"
                f"Please move your config file inside the safe directory, for example:\n"
                f"    {SAFE_CONFIG_DIR.joinpath(Path(cfg).name)}"
            )
            self.logger.error(msg)
            raise ValueError(msg)

        self.logger.info(f"Loading configuration from local file: {requested_path}")

        # Safe to open now
        try:
            with open(requested_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error(f"Config file not found: {cfg}", exc_info=True)
            raise
        except PermissionError:
            self.logger.error(f"Permission denied while reading config: {cfg}", exc_info=True)
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in config file {cfg}: {e}", exc_info=True)
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error reading {cfg}: {e}", exc_info=True)
            raise

    # ----------------------------------------------------------------------
    # Validation and accessors
    # ----------------------------------------------------------------------
    def _validate_minimal_fields(self) -> None:
        """
        Validate minimal required configuration structure.

        Ensures:
          - Required top-level keys exist (dataset, tables)
          - 'paths' is optional; if present, it must include 'bronze_base'
          - Each table defines a valid non-empty 'name'
          - Table schema may be provided via:
              - 'schema_sql' (string), or
              - 'schema' (list of structured column definitions)
          - If neither schema form is provided, schema inference is allowed
          - Table-level 'comment', if provided, must be either a string or a dict
          - For structured schema entries:
              - each entry must be an object/map
              - 'column' (or 'name') and 'type' are required
              - 'nullable', if provided, must be boolean
              - 'comment', if provided, must be either a string or a dict

        Raises:
            ValueError: If required keys are missing or invalid.
        """
        validation_errors: list[str] = []

        # ---- Top-level required keys ----
        required_top = ["dataset", "tables"]
        missing_top = [k for k in required_top if k not in self.config]
        if missing_top:
            validation_errors.append(f"Missing required top-level keys: {missing_top}")

        # ---- paths (optional) ----
        paths = self.config.get("paths")
        if paths is not None:
            if not isinstance(paths, dict):
                validation_errors.append("'paths' must be an object/map when provided.")
            else:
                required_paths = ["bronze_base"]
                missing_paths = [k for k in required_paths if k not in paths]
                if missing_paths:
                    validation_errors.append(f"Missing required path keys: {missing_paths}")
        else:
            self.logger.info("No 'paths' section found in config — skipping base path validation.")

        # ---- tables ----
        tables = self.config.get("tables", [])
        if not isinstance(tables, list) or not tables:
            validation_errors.append("Config must contain a non-empty 'tables' list")
            tables = []  # allow continued validation safely

        for idx, t in enumerate(tables):
            if not isinstance(t, dict):
                validation_errors.append(f"Table entry at index {idx} must be an object/map.")
                continue

            if "name" not in t:
                validation_errors.append(f"Table entry at index {idx} missing required key: name")
                continue

            if not isinstance(t["name"], str) or not t["name"].strip():
                validation_errors.append(
                    f"Table entry at index {idx} has invalid 'name' (must be non-empty string)."
                )
                continue

            table_name = t["name"]

            # Validate optional table-level comment (must be str or dict if provided)
            table_comment = t.get("comment")
            if "comment" in t and not isinstance(table_comment, (str, dict)):
                validation_errors.append(
                    f"Table '{table_name}' has invalid 'comment' (must be a string or dict)."
                )

            schema_sql = t.get("schema_sql")
            schema_list = t.get("schema")

            # ---- schema validation ----
            if schema_list is not None and not isinstance(schema_list, list):
                validation_errors.append(
                    f"Table '{table_name}' schema must be a list when provided."
                )
                schema_list = None

            if schema_sql is not None and not isinstance(schema_sql, str):
                validation_errors.append(
                    f"Table '{table_name}' schema_sql must be a string or null."
                )
                schema_sql = None

            # ---- schema presence checks (types already validated) ----
            has_schema_list = bool(schema_list)
            has_schema_sql = bool(schema_sql and schema_sql.strip())

            # If neither is provided, nothing else to validate here
            if not has_schema_sql and not has_schema_list:
                self.logger.info(
                    f"Table '{table_name}' has no explicit schema ('schema_sql'/'schema'); "
                    f"schema will be inferred by the loader."
                )
                continue

            # ---- in-depth validation for structured schema ----
            if has_schema_list:
                for i, coldef in enumerate(schema_list):
                    if not isinstance(coldef, dict):
                        validation_errors.append(
                            f"Table '{table_name}' schema entry at index {i} must be an object/map."
                        )
                        continue

                    col_name = coldef.get("column") or coldef.get("name")
                    if not col_name:
                        validation_errors.append(
                            f"Table '{table_name}' schema entry at index {i} missing 'column' (or 'name')."
                        )
                    if not coldef.get("type"):
                        validation_errors.append(
                            f"Table '{table_name}' schema entry for column "
                            f"'{col_name or f'<unknown@{i}>'}' missing 'type'."
                        )
                    if "nullable" in coldef and not isinstance(coldef["nullable"], bool):
                        validation_errors.append(
                            f"Table '{table_name}' schema entry for column "
                            f"'{col_name}' has non-boolean 'nullable'."
                        )
                    if "comment" in coldef and not isinstance(coldef["comment"], (str, dict)):
                        validation_errors.append(
                            f"Table '{table_name}' schema entry for column "
                            f"'{col_name}' has invalid 'comment' (must be a string or dict)."
                        )

        # ---- Optional warnings ----
        if "defaults" not in self.config:
            self.logger.warning("No 'defaults' section found in config — using built-in defaults.")

        # ---- Single raise at the end ----
        if validation_errors:
            msg = ":\n- ".join(
                ["Config validation failed with the following error(s):", *validation_errors]
            )
            self.logger.error(msg)
            raise ValueError(msg)

        self.logger.info("Minimal config validation passed")

    # ----------------------------------------------------------------------
    # Accessors
    # ----------------------------------------------------------------------
    def get_tenant(self) -> str:
        return self.config.get("tenant", "")

    def get_dataset(self) -> str:
        return self.config.get("dataset", "")

    def is_tenant(self) -> bool:
        return bool(self.config.get("is_tenant", False))

    def get_paths(self) -> dict[str, str]:
        return self.config.get("paths", {})

    def get_csv_defaults(self) -> dict[str, Any]:
        return self.config.get("defaults", {}).get(
            "csv", {"header": False, "delimiter": ",", "inferSchema": False}
        )

    def get_tables(self) -> list[dict[str, Any]]:
        tables = self.config.get("tables", [])
        enabled_tables = [t for t in tables if t.get("enabled", True)]
        disabled_count = len(tables) - len(enabled_tables)
        if disabled_count > 0:
            self.logger.info(f"{disabled_count} table(s) are disabled and will be skipped.")
        return enabled_tables

    def get_table(self, name: str) -> dict[str, Any] | None:
        for t in self.config.get("tables", []):
            if t["name"] == name:
                return t
        self.logger.warning(f"Requested table '{name}' not found in configuration.")
        return None

    def get_table_schema(self, table_name: str) -> list[dict[str, Any]] | None:
        t = self.get_table(table_name)
        if not t:
            return None
        schema = t.get("schema")
        return schema if isinstance(schema, list) else None

    def is_table_enabled(self, name: str) -> bool:
        """
        Check if a table is marked as enabled in the config.
        Defaults to True if flag is missing.
        """
        table = self.get_table(name)
        if table is None:
            return False
        return bool(table.get("enabled", True))

    def get_table_comment(self, table_name: str) -> str | dict[str, Any] | None:
        """
        Retrieve the table-level comment for a given table.

        The comment may be:
        - a plain string, or
        - a structured JSON-style dictionary (for richer metadata)

        Args:
            table_name (str): Name of the table.

        Returns:
            str | dict[str, Any] | None:
                The table comment if defined, otherwise None.

        Notes:
            - If the table is not found, None is returned.
            - Only string and dict types are considered valid comment formats.
        """
        t = self.get_table(table_name)
        if not t:
            return None
        comment = t.get("comment")
        return comment if isinstance(comment, (str, dict)) or comment is None else None

    def get_bronze_path(self, table_name: str) -> str:
        """
        Resolve the Bronze path for a given table.

        Supported forms for 'bronze_path':
        - Absolute URI (e.g., s3a://...) → used as-is
        - '${bronze_base}/file.ext' → substituted using config.paths.bronze_base
        - 'file.ext' or 'subdir/file.ext' → joined with config.paths.bronze_base

        Raises:
            ValueError: If the table is not found, 'bronze_path' is missing,
                        or 'paths.bronze_base' is required but not set.
        """
        t = self.get_table(table_name)
        if not t:
            raise ValueError(
                f"Cannot resolve bronze path — table '{table_name}' not found in configuration."
            )

        bronze_path = t.get("bronze_path")
        if not bronze_path or not isinstance(bronze_path, str):
            raise ValueError(f"'bronze_path' must be defined as a string for table '{table_name}'.")

        bronze_path = bronze_path.strip()

        # Absolute path → return as-is
        if "://" in bronze_path:
            return bronze_path

        paths = self.config.get("paths")
        bronze_base = paths.get("bronze_base") if isinstance(paths, dict) else None
        if not bronze_base:
            raise ValueError(
                f"Cannot resolve bronze_path for table '{table_name}' because "
                "config.paths.bronze_base is not set."
            )

        # Variable substitution
        if bronze_path.startswith("${bronze_base}"):
            suffix = bronze_path[len("${bronze_base}") :].lstrip("/")
            return f"{bronze_base.rstrip('/')}/{suffix}"

        # Relative path / filename
        return f"{bronze_base.rstrip('/')}/{bronze_path.lstrip('/')}"

    def get_silver_path(self, table_name: str) -> str:
        paths = self.config.get("paths") or {}
        base = paths.get("silver_base")
        if not base:
            raise ValueError(
                "Cannot resolve silver path because config.paths.silver_base is not set."
            )
        return f"{base.rstrip('/')}/{table_name}"

    def get_defaults_for(self, fmt: str) -> dict[str, Any]:
        defaults = self.config.get("defaults", {})
        if fmt in defaults:
            return defaults[fmt]
        if fmt == "tsv" and "csv" in defaults:
            csv_def = defaults["csv"].copy()
            csv_def["delimiter"] = "\t"
            return csv_def
        self.logger.warning(f"No defaults found for format '{fmt}', using safe fallback.")
        return {"header": False, "delimiter": "\t" if fmt == "tsv" else ",", "inferSchema": False}

    def summarize(self) -> dict[str, Any]:
        summary = {
            "dataset": self.get_dataset(),
            "num_tables": len(self.get_tables()),
        }

        paths = self.config.get("paths")
        if isinstance(paths, dict):
            if "bronze_base" in paths:
                summary["bronze_base"] = paths["bronze_base"]

        # Add tenant only if present
        tenant = self.get_tenant()
        if tenant:
            summary["tenant"] = tenant

        return summary

    def get_full_config(self) -> dict[str, Any]:
        return self.config
