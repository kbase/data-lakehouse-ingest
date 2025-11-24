"""
Provides structured logging with contextual metadata for Data Lakehouse Ingest pipelines.
Supports console and file output with JSON-formatted log entries.
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from .utils.json_encoder import PipelineJSONEncoder


_logger_instance = None

class PipelineContextFilter(logging.Filter):
    """
    A custom logging filter that injects pipeline context (pipeline name, schema, table)
    into each log record. This enables structured logging across all components.
    """

    def __init__(self, pipeline_name: str, schema: str):
        super().__init__()
        self.pipeline_name = pipeline_name
        self.schema = schema
        self.target_table = "pipeline_stage"  # default

    def set_table(self, table: str):
        self.target_table = table

    def filter(self, record):
        record.pipeline_name = self.pipeline_name
        record.schema = self.schema
        record.target_table = self.target_table
        return True

def setup_logger(
    log_dir: str | Path = Path("local_logs"),
    logger_name: str = "pipeline_logger",
    pipeline_name: str = "unknown_pipeline",
    schema: str = "unknown_schema",
    log_level: str | None = None,
) -> logging.Logger:
    """
    Set up and return a structured logger with both file and console handlers.

    Args:
        log_dir (str): Directory to write log files.
        logger_name (str): Name of the logger instance.
        pipeline_name (str): Logical name of the pipeline (used in context).
        target_table (str): Target table name (used in context).
        schema (str): Schema name (used in context).
        log_level (str | None): Optional log level ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL").
            If not provided, the environment variable `PIPELINE_LOG_LEVEL` is used.
            Defaults to "DEBUG" if neither is specified.

    Returns:
        logging.Logger: A configured logger instance with structured output.

    Notes:
        - If the logger is already initialized, the same instance is returned.
        - Log output includes pipeline, schema, table, module, and log level.
        - The log level can be dynamically adjusted per environment.
    """
    global _logger_instance
    if _logger_instance:
        return _logger_instance

    # Ensure log directory exists
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().isoformat(timespec="seconds").replace(":", "-")
    log_file = os.path.join(log_dir, f"pipeline_run_{timestamp}.log")

    # Always get the same logger by name
    logger = logging.getLogger(logger_name)

    # Determine log level (argument > env var > default)
    effective_log_level = (
        log_level or os.getenv("PIPELINE_LOG_LEVEL", "DEBUG")
    ).upper()
    logger.setLevel(getattr(logging, effective_log_level, logging.DEBUG))

    # Clean up any existing handlers on this logger
    if logger.hasHandlers():
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

    # JSON-style structured formatter
    formatter = logging.Formatter(
        '{"time": "%(asctime)s", "pipeline": "%(pipeline_name)s", '
        '"schema": "%(schema)s", "table": "%(target_table)s", '
        '"level": "%(levelname)s", "module": "%(module)s", "msg": "%(message)s"}'
    )

    # File handler
    fh = logging.FileHandler(log_file, mode="w")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Add pipeline context filter
    context_filter = PipelineContextFilter(pipeline_name, schema)
    logger.addFilter(context_filter)
    logger.context_filter = context_filter

    # Store log file path for later inspection
    logger.log_file_path = log_file
    _logger_instance = logger
    return logger

def safe_log_json(logger: logging.Logger, data: object) -> None:
    """
    Safely log dictionaries or objects that may contain non-serializable elements.

    Uses the custom PipelineJSONEncoder to ensure that complex types such as
    datetime, Decimal, Path, and UUID are serialized consistently (e.g., ISO 8601
    for timestamps) and that logging never fails due to a TypeError.

    Args:
        logger (logging.Logger): The logger instance used to output the message.
        data (object): The data structure (usually a dict or report) to serialize
                       and log in JSON format.
    """
    try:
        # Serialize using the custom encoder (handles datetime, Decimal, Path, UUID, etc.)
        logger.info(json.dumps(data, indent=2, cls=PipelineJSONEncoder))
    except Exception:
        # Fallback to a plain string representation if serialization fails entirely
        logger.info(str(data))
