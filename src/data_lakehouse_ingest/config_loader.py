"""
Purpose:
    Provides a configuration loader for the Data Lakehouse Ingest framework.
    Supports reading configuration from:
      - Local JSON file
      - Inline JSON string
      - MinIO object storage (s3a://bucket/key)

    Performs validation of minimal required fields, applies defensive checks
    (e.g., path traversal prevention), and exposes convenient accessors for
    tenant, dataset, paths, and per-table definitions.
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
          - Required top-level keys exist (tenant, dataset, paths, tables)
          - Required path keys exist (bronze_base, silver_base)
          - Each table defines 'name' and 'schema_sql'

        Raises:
            ValueError: If required keys are missing or invalid.
        """
        required_top = ["dataset", "tables"]
        missing_top = [k for k in required_top if k not in self.config]
        if missing_top:
            self.logger.error(f"Missing required top-level keys: {missing_top}")
            raise ValueError(f"Missing required top-level keys: {missing_top}")

        required_paths = ["bronze_base", "silver_base"]
        missing_paths = [k for k in required_paths if k not in self.config["paths"]]
        if missing_paths:
            self.logger.error(f"Missing required path keys: {missing_paths}")
            raise ValueError(f"Missing required path keys: {missing_paths}")

        tables = self.config.get("tables", [])
        if not isinstance(tables, list) or not tables:
            self.logger.error("Config must contain a non-empty 'tables' list")
            raise ValueError("Config must contain a non-empty 'tables' list")

        for t in tables:
            for key in ["name", "schema_sql"]:
                if key not in t:
                    self.logger.error(f"Table entry missing required key: {key}")
                    raise ValueError(f"Table entry missing required key: {key}")

        # Optional but useful warnings
        if "defaults" not in self.config:
            self.logger.warning("No 'defaults' section found in config — using built-in defaults.")

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

    def is_table_enabled(self, name: str) -> bool:
        """
        Check if a table is marked as enabled in the config.
        Defaults to True if flag is missing.
        """
        table = self.get_table(name)
        if table is None:
            return False
        return bool(table.get("enabled", True))

    def get_bronze_path(self, table_name: str) -> str | None:
        """
        Resolve the Bronze path for a given table.

        Rules:
        1. If the table defines 'bronze_path', return it.
        2. Otherwise, raise an explicit error — we do not guess or synthesize paths.
        """
        t = self.get_table(table_name)
        if not t:
            msg = f"Cannot resolve bronze path — table '{table_name}' not found in configuration."
            self.logger.error(msg)
            raise ValueError(msg)

        # Must be explicitly defined
        bronze_path = t.get("bronze_path")
        if bronze_path:
            return bronze_path

        msg = (
            f"'bronze_path' not defined for table '{table_name}'. "
            f"Each table must specify an explicit Bronze path in configuration."
        )
        self.logger.error(msg)
        raise ValueError(msg)

    def get_silver_path(self, table_name: str) -> str:
        base = self.config["paths"]["silver_base"].rstrip("/")
        return f"{base}/{table_name}"

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
            "bronze_base": self.config["paths"]["bronze_base"],
            "silver_base": self.config["paths"]["silver_base"],
        }

        # Add tenant only if present
        tenant = self.get_tenant()
        if tenant:
            summary["tenant"] = tenant

        return summary

    def get_full_config(self) -> dict[str, Any]:
        return self.config
