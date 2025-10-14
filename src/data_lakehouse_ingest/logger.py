# src/logger.py
import logging
import os
import sys
from datetime import datetime

_logger_instance = None

class PipelineContextFilter(logging.Filter):
    """
    A custom logging filter that injects pipeline context (pipeline name, schema, table)
    into each log record. This enables structured logging across all components.
    """

    def __init__(self, pipeline_name: str, target_table: str, schema: str):
        super().__init__()
        self.pipeline_name = pipeline_name
        self.target_table = target_table
        self.schema = schema

    def filter(self, record):
        record.pipeline_name = self.pipeline_name
        record.target_table = self.target_table
        record.schema = self.schema
        return True

def setup_logger(
    log_dir: str = "local_logs",
    logger_name: str = "pipeline_logger",
    pipeline_name: str = "unknown_pipeline",
    target_table: str = "unknown_table",
    schema: str = "unknown_schema"
) -> logging.Logger:
    """
    Set up and return a structured logger with both file and console handlers.

    Args:
        log_dir (str): Directory to write log files.
        logger_name (str): Name of the logger instance.
        pipeline_name (str): Logical name of the pipeline (used in context).
        target_table (str): Target table name (used in context).
        schema (str): Schema name (used in context).

    Returns:
        logging.Logger: A configured logger instance with structured output.

    Notes:
        - If the logger is already initialized, the same instance is returned.
        - Log output includes pipeline, schema, table, module, and log level.
    """
    global _logger_instance
    if _logger_instance:
        return _logger_instance

    # Ensure log directory exists
    try:
        os.makedirs(log_dir, exist_ok=True)
    except Exception as e:
        raise OSError(f"Failed to create log directory '{log_dir}': {e}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"pipeline_run_{timestamp}.log")

    # Always get the same logger by name
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    # 🔥 Clean up any existing handlers on this logger
    if logger.hasHandlers():
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

    try:
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
        context_filter = PipelineContextFilter(pipeline_name, target_table, schema)
        logger.addFilter(context_filter)

        # Store log file path for later inspection
        logger.log_file_path = log_file
        _logger_instance = logger
        return logger

    except Exception as e:
        raise RuntimeError(f"Failed to set up logger '{logger_name}': {e}")

