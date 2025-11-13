# src/data_lakehouse_ingest/parsers/uniprot_ingest.py
import os
import datetime
import logging
import tempfile
from pyspark.sql import SparkSession
from ..utils.uniprot_writer import write_uniprot_to_delta

from .uniprot import (
    prepare_local_xml,
    save_datasource_record,
    get_spark_session,
    parse_entries,
    schema_entities,
    schema_identifiers,
    schema_names,
    schema_proteins,
    schema_associations,
    schema_publications,
)

def process_uniprot_to_delta(
    xml_path: str,
    spark: SparkSession,
    namespace: str,
    s3_silver_base: str,
    batch_size: int = 5000,
    minio_client=None,
):
    """
    Parse a UniProt XML file (possibly gzipped) and write Delta tables directly to MinIO (s3a://).
    """
    logger = logging.getLogger("uniprot_ingest")
    logger.setLevel(logging.INFO)

    logger.info(f"🧬 Starting UniProt XML ingestion for {xml_path}")

    current_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # Copy XML from MinIO (s3a://) to local temp file if needed
    if xml_path.startswith("s3a://"):
        local_path = copy_s3a_to_local(minio_client, xml_path)
        logger.info(f"📥 Copied XML from {xml_path} → {local_path}")
        xml_path = local_path

    entities, identifiers, names, proteins, associations, publications = [], [], [], [], [], []
    tables = {
        "entities": (entities, schema_entities),
        "identifiers": (identifiers, schema_identifiers),
        "names": (names, schema_names),
        "proteins": (proteins, schema_proteins),
        "associations": (associations, schema_associations),
        "publications": (publications, schema_publications),
    }

    entry_count, skipped = parse_entries(
        local_xml_path=xml_path,
        target_date=None,
        batch_size=batch_size,
        spark=spark,
        tables=tables,
        output_dir="/tmp",
        namespace=namespace,
        current_timestamp=current_timestamp
    )

    # Write parsed records to Delta on MinIO
    write_uniprot_to_delta(spark, tables, namespace, s3_silver_base, logger)

    # Clean up old local tables
    for table in tables.keys():
        spark.sql(f"DROP TABLE IF EXISTS `{namespace}`.`{table}`")
        spark.sql(f"""
            CREATE TABLE `{namespace}`.`{table}`
            USING DELTA
            LOCATION '{s3_silver_base.rstrip('/')}/{table}'
        """)
        logger.info(f"🔁 Re-registered table {namespace}.{table} to {s3_silver_base.rstrip('/')}/{table}")

    logger.info(f"✅ UniProt ingestion complete: {entry_count} entries processed, {skipped} skipped.")


def copy_s3a_to_local(minio_client, s3_path: str) -> str:
    """
    Copy an s3a:// file to a local temp file using MinIO client
    (Spark Connect cannot use Hadoop FS APIs).
    """
    if not s3_path.startswith("s3a://"):
        return s3_path

    bucket, *key = s3_path.replace("s3a://", "").split("/", 1)
    key = key[0] if key else ""

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(s3_path)[-1])
    tmp_path = tmp.name
    tmp.close()

    # download file
    minio_client.fget_object(bucket, key, tmp_path)

    return tmp_path

