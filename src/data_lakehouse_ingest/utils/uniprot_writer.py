# src/data_lakehouse_ingest/utils/uniprot_writer.py
import os

def write_uniprot_to_delta(spark, tables: dict, namespace: str, base_path: str, logger):
    """
    Write UniProt tables to Delta on MinIO (s3a://) using same logic as config-based ingestion.
    Each key in `tables` is a table name; value is a tuple (records, schema).
    """
    for table, (records, schema) in tables.items():
        if not records:
            logger.info(f"Skipping empty UniProt table: {table}")
            continue

        # ✅ Fix: Construct S3 paths safely without os.path.join
        silver_path = (
            f"{base_path.rstrip('/')}/{table}"
            if base_path.startswith("s3a://")
            else os.path.join(base_path, table)
        )

        logger.info(f"💾 Writing UniProt table {namespace}.{table} to {silver_path}")

        df = spark.createDataFrame(records, schema)
        writer = (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
        )
        writer.save(silver_path)

        # ✅ Explicitly register in Spark with correct external location
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS `{namespace}`.`{table}`
            USING DELTA
            LOCATION '{silver_path}'
        """)

        count = spark.read.format("delta").load(silver_path).count()
        logger.info(f"✅ Table {namespace}.{table} written successfully with {count} rows.")
