from __future__ import annotations
from typing import Optional, Dict, Any, List, Union
import json
import logging
import os
from minio.error import S3Error


class ConfigLoader:
    """
    ConfigLoader for minimal data-lakehouse ingestion configuration.

    Supports loading from:
      - Local file path
      - Inline JSON string
      - MinIO (s3a://bucket/key)
    """

    def __init__(
        self,
        cfg: Union[str, Dict[str, Any]],
        logger: Optional[logging.Logger] = None,
        minio_client: Optional[Any] = None,
    ):
        self.logger = logger or logging.getLogger(__name__)
        self.minio_client = minio_client

        if isinstance(cfg, str) and cfg.startswith("s3a://") and self.minio_client is None:
            self.logger.error("❌ MinIO client must be provided for s3a:// paths.")
            raise ValueError("MinIO client must be provided when loading configuration from an s3a:// path.")

        try:
            self.config: Dict[str, Any] = self._load_config(cfg)
            self.logger.info("✅ Config loaded successfully")
            self._validate_minimal_fields()
        except Exception as e:
            self.logger.error(f"❌ ConfigLoader initialization failed: {e}", exc_info=True)
            raise

    # ----------------------------------------------------------------------
    # Internal helpers
    # ----------------------------------------------------------------------
    def _load_config(self, cfg: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Load config from dict, local file, MinIO path, or JSON string."""
        if isinstance(cfg, dict):
            self.logger.info("📄 Using inline dict configuration")
            return cfg

        # --- Local file ---
        if os.path.exists(cfg):
            self.logger.info(f"📂 Loading configuration from local file: {cfg}")
            try:
                with open(cfg, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                self.logger.error(f"❌ Failed to read local config file {cfg}: {e}", exc_info=True)
                raise

        # --- MinIO path (s3a://bucket/key) ---
        if cfg.startswith("s3a://"):
            path = cfg.replace("s3a://", "")
            bucket, key = path.split("/", 1)
            self.logger.info(f"📦 Fetching config from MinIO: bucket={bucket}, key={key}")
            try:
                response = self.minio_client.get_object(bucket, key)
                data = json.loads(response.read())
                response.close()
                response.release_conn()
                return data
            except S3Error as e:
                self.logger.error(f"❌ Failed to read {cfg} from MinIO: {e}", exc_info=True)
                raise RuntimeError(f"Failed to read {cfg} from MinIO: {e}")
            except Exception as e:
                self.logger.error(f"❌ Unexpected error while reading {cfg} from MinIO: {e}", exc_info=True)
                raise

        # --- Inline JSON string ---
        self.logger.info("🧾 Parsing inline JSON configuration string")
        try:
            return json.loads(cfg)
        except Exception as e:
            self.logger.error(f"❌ Invalid inline JSON configuration: {e}", exc_info=True)
            raise ValueError(f"Invalid configuration source: {e}")

    # ----------------------------------------------------------------------
    # Validation and accessors
    # ----------------------------------------------------------------------
    def _validate_minimal_fields(self) -> None:
        """Ensure minimal required fields exist."""
        required_top = ["tenant", "dataset", "paths", "tables"]
        missing_top = [k for k in required_top if k not in self.config]
        if missing_top:
            self.logger.error(f"❌ Missing required top-level keys: {missing_top}")
            raise ValueError(f"Missing required top-level keys: {missing_top}")

        required_paths = ["bronze_base", "silver_base"]
        missing_paths = [k for k in required_paths if k not in self.config["paths"]]
        if missing_paths:
            self.logger.error(f"❌ Missing required path keys: {missing_paths}")
            raise ValueError(f"Missing required path keys: {missing_paths}")

        tables = self.config.get("tables", [])
        if not isinstance(tables, list) or not tables:
            self.logger.error("❌ Config must contain a non-empty 'tables' list")
            raise ValueError("Config must contain a non-empty 'tables' list")

        for t in tables:
            for key in ["name", "schema_sql"]:
                if key not in t:
                    self.logger.error(f"❌ Table entry missing required key: {key}")
                    raise ValueError(f"Table entry missing required key: {key}")

        # Optional but useful warnings
        if "defaults" not in self.config:
            self.logger.warning("⚠️ No 'defaults' section found in config — using built-in defaults.")

        self.logger.info("✅ Minimal config validation passed")

    # ----------------------------------------------------------------------
    # Accessors
    # ----------------------------------------------------------------------
    def get_tenant(self) -> str:
        tenant = self.config["tenant"]
        dataset = self.config.get("dataset", "")
        return tenant.replace("${dataset}", dataset)

    def get_dataset(self) -> str:
        return self.config.get("dataset", "")

    def get_paths(self) -> Dict[str, str]:
        return self.config.get("paths", {})

    def get_csv_defaults(self) -> Dict[str, Any]:
        return self.config.get("defaults", {}).get(
            "csv", {"header": False, "delimiter": ",", "inferSchema": False}
        )

    def get_tables(self) -> List[Dict[str, Any]]:
        return self.config.get("tables", [])

    def get_table(self, name: str) -> Optional[Dict[str, Any]]:
        for t in self.config.get("tables", []):
            if t["name"] == name:
                return t
        self.logger.warning(f"⚠️ Requested table '{name}' not found in configuration.")
        return None

    def get_bronze_path(self, table_name: str) -> Optional[str]:
        t = self.get_table(table_name)
        if not t:
            self.logger.warning(f"⚠️ Cannot resolve bronze path — table '{table_name}' not found.")
            return None
        if "bronze_path" in t and t["bronze_path"]:
            return t["bronze_path"]
        base = self.config["paths"]["bronze_base"].rstrip("/")
        return f"{base}/{t['name']}.csv"

    def get_silver_path(self, table_name: str) -> str:
        base = self.config["paths"]["silver_base"].rstrip("/")
        return f"{base}/{table_name}"

    def get_defaults_for(self, fmt: str) -> Dict[str, Any]:
        defaults = self.config.get("defaults", {})
        if fmt in defaults:
            return defaults[fmt]
        if fmt == "tsv" and "csv" in defaults:
            csv_def = defaults["csv"].copy()
            csv_def["delimiter"] = "\t"
            return csv_def
        self.logger.warning(f"⚠️ No defaults found for format '{fmt}', using safe fallback.")
        return {"header": False, "delimiter": "\t" if fmt == "tsv" else ",", "inferSchema": False}

    def summarize(self) -> Dict[str, Any]:
        return {
            "tenant": self.get_tenant(),
            "dataset": self.get_dataset(),
            "num_tables": len(self.get_tables()),
            "bronze_base": self.config["paths"]["bronze_base"],
            "silver_base": self.config["paths"]["silver_base"],
        }

    def get_full_config(self) -> Dict[str, Any]:
        return self.config
