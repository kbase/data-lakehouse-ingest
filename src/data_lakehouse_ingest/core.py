import json
from typing import Union

def ingest_from_config(config: Union[str, dict], spark=None) -> bool:
    """
    Minimal ingestion stub. Returns True for both dict and JSON path configs.

    Args:
        config (Union[str, dict]): Either a dict config or a JSON file path.
        spark (optional): SparkSession (not used in v0.0.1).

    Returns:
        bool: Always True
    """
    if isinstance(config, str):
        # Treat as path to JSON file
        with open(config, "r") as f:
            _ = json.load(f)
    elif isinstance(config, dict):
        _ = config
    else:
        raise TypeError("config must be a dict or a JSON file path string")
    return True
