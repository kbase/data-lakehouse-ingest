# src/data_lakehouse_ingest/core.py

import json
import logging
from typing import Union, Dict, Any
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

from .config_loader import ConfigLoader
from .logger import setup_logger
from .utils.linkml_parser import load_linkml_schema

from .loaders.json_loader import load_json_data
from .loaders.xml_loader import load_xml_data
from .loaders.dsv_loader import load_dsv_data, load_csv_data, load_tsv_data



# ----------------------------------------------------------------------
# Main function
# ----------------------------------------------------------------------
def data_lakehouse_ingest_config(
    config: Union[str, Dict[str, Any]],
    spark: SparkSession = None,
    logger: logging.Logger = None,
    minio_client: Any = None
) -> Dict[str, Any]:
    """
    Data Lakehouse Ingest MVP (CSV/TSV/JSON/XML)
    --------------------------------------------
    Reads configuration (inline dict, local JSON, or s3a:// path),
    loads Bronze data, and writes Delta tables into corresponding Silver paths.
    """
    started_at = datetime.utcnow().isoformat() + "Z"

    # --- Spark Session ---
    if spark is None:
        error_msg = (
            "❌ SparkSession must be provided by the caller. "
            "Please start a Spark session and pass it as `spark=` argument."
        )
        logger_error = logger or logging.getLogger("data_lakehouse_ingest")
        logger_error.error(error_msg)

        ended_at = datetime.utcnow().isoformat() + "Z"
        duration_sec = (
            datetime.fromisoformat(ended_at.replace("Z", "")) -
            datetime.fromisoformat(started_at.replace("Z", ""))
        ).total_seconds()

        report = {
            "success": False,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_sec": duration_sec,
            "tables": [],
            "errors": [
                {
                    "phase": "spark_initialization",
                    "error": error_msg
                }
            ]
        }
        logger_error.info("🏁 Ingestion terminated during Spark session check")
        logger_error.info(json.dumps(report, indent=2))
        return report


    # --- Logger ---
    if logger is None:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
        logger = logging.getLogger("data_lakehouse_ingest")
        logger.info("No external logger provided; using internal basic logger.")

    # --- Config Loader ---
    #loader = ConfigLoader(config, logger=logger, minio_client=minio_client)

    try:
        loader = ConfigLoader(config, logger=logger, minio_client=minio_client)
    except Exception as e:
        logger.error(f"❌ Failed to load or validate configuration: {e}", exc_info=True)
        ended_at = datetime.utcnow().isoformat() + "Z"
        duration_sec = (
            datetime.fromisoformat(ended_at.replace("Z", "")) -
            datetime.fromisoformat(started_at.replace("Z", ""))
        ).total_seconds()
        report = {
            "success": False,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_sec": duration_sec,
            "tables": [],
            "errors": [
                {
                    "phase": "config_validation",
                    "error": str(e)
                }
            ]
        }
        logger.info("🏁 Ingestion terminated during config validation")
        logger.info(json.dumps(report, indent=2))
        return report

    tenant = loader.get_tenant()
    format_defaults = loader.get_all_defaults() if hasattr(loader, "get_all_defaults") else {}
    tables = loader.get_tables()

    logger.info(f"🔧 Loaded configuration for tenant: {tenant}")
    logger.info(f"📋 Found {len(tables)} table(s) to process")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{tenant}`")
    spark.catalog.setCurrentDatabase(tenant)

    table_reports = []
    error_list = []

    # ----------------------------------------------------------------------
    # Table-level processing
    # ----------------------------------------------------------------------
    for table in tables:
        name = table["name"]
        bronze_path = loader.get_bronze_path(name)
        silver_path = loader.get_silver_path(name)

        # Special handling: delegate to parser if process_with is defined
        if table.get("process_with") == "uniprot":
            from .parsers.uniprot_ingest import process_uniprot_to_delta
            logger.info(f"🚀 Delegating to UniProt ingestion pipeline for table: {name}")

            start_table_time = datetime.utcnow()
            process_uniprot_to_delta(
                xml_path=bronze_path,
                namespace=tenant,
                s3_silver_base=silver_path,
                batch_size=table.get("batch_size", 5000)
            )
            elapsed_sec = (datetime.utcnow() - start_table_time).total_seconds()

            # Optional: you can retrieve counts from Delta after ingestion
            try:
                rows_written = spark.read.format("delta").load(silver_path).count()
            except Exception:
                rows_written = None

            table_reports.append({
                "name": name,
                "tenant": tenant,
                "target_table": f"{tenant}.{name}",
                "mode": table.get("mode", "overwrite"),
                "format": "xml",                      # UniProt always XML
                "schema_source": "custom_parser",     # mark as special schema
                "bronze_path": bronze_path,
                "silver_path": silver_path,
                "rows_in": None,                      # parser handles batching
                "rows_written": rows_written,
                "rows_rejected": 0,                   # can update later
                "partitions_written": None,
                "quarantine_path": f"{silver_path}/quarantine/{start_table_time.isoformat().replace(':','-')}/",
                "elapsed_sec": elapsed_sec,
                "process_with": "uniprot",
                "status": "success"
            })

            continue  # skip default CSV/TSV/JSON ingestion path
            
        start_table_time = datetime.utcnow()

        try:
            # --- Determine format ---
            fmt = table.get("format")
            if fmt is None:
                ext = bronze_path.split(".")[-1].lower()
                fmt = "xml" if ext == "xml" else ("json" if ext == "json" else ("tsv" if ext == "tsv" else "csv"))

            # --- Schema handling ---
            schema_sql = table.get("schema_sql")
            linkml_schema = table.get("linkml_schema")

            # Determine initial schema source
            if linkml_schema:
                schema_source = "schema_linkml_path"
            elif schema_sql:
                schema_source = "schema_sql"
            else:
                schema_source = "inferred"

            if linkml_schema:
                #schema_source = "schema_linkml_path"
                logger.info(f"🧬 Using LinkML schema for table {name} (takes precedence over schema_sql)")
                try:
                    schema_cols = load_linkml_schema(spark, linkml_schema, logger, minio_client=minio_client)
                    schema_sql = ", ".join([f"{c} {t}" for c, t in schema_cols.items()])
                    logger.info(f"✅ Derived schema_sql from LinkML for {name}: {schema_sql}")
                except Exception as e:
                    logger.error(f"❌ Failed to parse LinkML schema for {name}: {e}", exc_info=True)
                    if schema_sql:
                        logger.warning(f"⚠️ Falling back to inline schema_sql for {name}.")
                    else:
                        logger.warning(f"⚠️ No schema_sql fallback available for {name}. Using inferred schema.")
                        schema_source = "inferred"

            # --- Load format defaults ---
            if hasattr(loader, "get_defaults_for"):
                opts = loader.get_defaults_for(fmt)
            else:
                opts = format_defaults.get(fmt, {"header": True, "delimiter": "\t" if fmt == "tsv" else ",", "inferSchema": False})
            
            opts = {k: (str(v).lower() if isinstance(v, bool) else v) for k, v in opts.items()}
            opts["recursiveFileLookup"] = "true"

            # --- Load data ---
            logger.info(f"📦 Processing table: {name}")
            logger.info(f"   Bronze: {bronze_path}")
            logger.info(f"   Silver: {silver_path}")

            try:
                # Map format names to their corresponding loader functions
                fmt_to_loader = {
                    "json": load_json_data,
                    "xml": load_xml_data,
                    "csv": load_csv_data,
                    "tsv": load_tsv_data,
                }

                # Check if the format is supported
                if fmt not in fmt_to_loader:
                    raise ValueError(f"❌ Unsupported file format '{fmt}' for table '{name}'")

                # Retrieve and call the correct loader function dynamically
                loader_fn = fmt_to_loader[fmt]
                df = loader_fn(spark, bronze_path, opts, logger)

                # Log and count successfully loaded records
                rows_in = df.count()
                logger.info(f"✅ Loaded {rows_in} records for table '{name}'")

            except Exception as e:
                # Log errors with detailed context
                logger.error(f"❌ Failed to load data for table '{name}': {e}", exc_info=True)
                error_entry = {
                    "name": name,
                    "error": str(e),
                    "phase": "data_loading",
                    "bronze_path": bronze_path,
                    "format": fmt,
                    "status": "failed"
                }
                table_reports.append(error_entry)
                error_list.append(error_entry)
                continue  # skip rest of the loop for this table



            rows_in = df.count()

            # --- Apply schema ---
            if schema_sql:
                target_cols = [x.strip().split(" ")[0] for x in schema_sql.split(",")]
                if len(target_cols) == len(df.columns):
                    df = df.toDF(*target_cols)
                    logger.info(f"   Applied inline schema: {schema_sql}")
                else:
                    logger.warning(f"⚠️ Schema column mismatch for {name}: {len(df.columns)} in data vs {len(target_cols)} in schema. Skipping rename.")

            # --- Write to Delta ---
            partition_by = table.get("partition_by")
            mode = table.get("mode", "overwrite")
            writer = df.write.format("delta").mode(mode)
            if partition_by:
                writer = writer.partitionBy(partition_by)

            writer.save(silver_path)
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS `{tenant}`.`{name}`
                USING DELTA
                LOCATION '{silver_path}'
            """)

            rows_written = spark.read.format("delta").load(silver_path).count()

            # --- Mocked placeholders for extended metrics ---
            rows_rejected = 0  # To be filled by DQ checks later
            partitions_written = None  # Could be obtained via Delta metadata
            quarantine_path = f"{silver_path}/quarantine/{started_at.replace(':', '-')}/"
            elapsed_sec = (datetime.utcnow() - start_table_time).total_seconds()

            logger.info(f"✅ Table {tenant}.{name}: {rows_in} → {rows_written} rows in {elapsed_sec:.2f}s")

            table_reports.append({
                "name": name,
                "tenant": tenant,
                "target_table": f"{tenant}.{name}",
                "mode": mode,
                "format": fmt,
                "schema_source": schema_source,
                "bronze_path": bronze_path,
                "silver_path": silver_path,
                "rows_in": rows_in,
                "rows_written": rows_written,
                "rows_rejected": rows_rejected,
                "partitions_written": partitions_written,
                "quarantine_path": quarantine_path,
                "elapsed_sec": elapsed_sec,
                "status": "success"
            })

        except AnalysisException as e:
            logger.error(f"❌ Spark AnalysisException for table {name}: {e}", exc_info=True)
            error_entry = {"name": name, "error": str(e), "bronze_path": bronze_path, "status": "failed"}
            table_reports.append(error_entry)
            error_list.append(error_entry)

        except Exception as e:
            logger.error(f"❌ Unexpected error for table {name}: {e}", exc_info=True)
            error_entry = {"name": name, "error": str(e), "bronze_path": bronze_path, "status": "failed"}
            table_reports.append(error_entry)
            error_list.append(error_entry)

    # ----------------------------------------------------------------------
    # Final report
    # ----------------------------------------------------------------------
    ended_at = datetime.utcnow().isoformat() + "Z"
    duration_sec = (
        datetime.fromisoformat(ended_at.replace("Z", "")) -
        datetime.fromisoformat(started_at.replace("Z", ""))
    ).total_seconds()

    report = {
        "success": all(t["status"] == "success" for t in table_reports),
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_sec": duration_sec,
        "tables": table_reports,
        "errors": error_list,
    }

    logger.info("🏁 Ingestion complete")
    logger.info(json.dumps(report, indent=2))
    return report
