import os
from pyspark.sql import SparkSession

def start_spark_session(app_name="Delta + Hive + MinIO") -> SparkSession:
    jar_dir = "/home/jovyan/jars"
    all_jars = [os.path.join(jar_dir, f) for f in os.listdir(jar_dir) if f.endswith(".jar")]
    jars_csv = ",".join(all_jars)

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", jars_csv) \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .enableHiveSupport() \
        .getOrCreate()

    return spark
