# src/data_lakehouse_ingest/parsers/uniprot_ingest.py
import os
import datetime
import logging
import tempfile
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

def process_uniprot_to_delta(xml_path: str, namespace: str, s3_silver_base: str, batch_size: int = 5000):
    """
    Parse a UniProt XML file (possibly gzipped) and write Delta tables directly to MinIO (s3a://).
    """
    logger = logging.getLogger("uniprot_ingest")
    logger.setLevel(logging.INFO)

    logger.info(f"🧬 Starting UniProt XML ingestion for {xml_path}")

    current_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    spark = get_spark_session(namespace)

    # Copy XML from MinIO (s3a://) to local temp file if needed
    if xml_path.startswith("s3a://"):
        local_path = copy_s3a_to_local(spark, xml_path)
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
    spark.stop()



def copy_s3a_to_local(spark, s3_path: str) -> str:
    """
    Copy an S3A file (e.g., s3a://test-bucket/path/to/file.xml) to a temporary local file
    using Hadoop FS API, and return the local path.
    """
    if not s3_path.startswith("s3a://"):
        return s3_path  # already local

    local_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(s3_path)[-1])
    hadoop_conf = spark._jsc.hadoopConfiguration()
    
    uri = spark._jvm.java.net.URI(s3_path)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    
    src_path = spark._jvm.org.apache.hadoop.fs.Path(s3_path)
    dst_path = spark._jvm.org.apache.hadoop.fs.Path(local_tmp.name)
    
    fs.copyToLocalFile(False, src_path, dst_path)
    return local_tmp.name
