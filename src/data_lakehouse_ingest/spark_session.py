import os
import logging
from pyspark.sql import SparkSession


def start_spark_session(app_name: str = "Delta + Hive + MinIO", logger: logging.Logger = None) -> SparkSession:
    """
    Initializes and returns a SparkSession configured for Delta Lake, Hive Metastore, and MinIO (S3-compatible).

    Args:
        app_name (str): Name of the Spark application.
        logger (logging.Logger): Logger instance for capturing logs.

    Returns:
        SparkSession: Configured Spark session with Hive and Delta support.

    Raises:
        ValueError: If no logger is provided.
        EnvironmentError: If required environment variables are missing.
        Exception: If session creation fails or required jars are missing.
    """
    if logger is None:
        raise ValueError("Logger instance must be provided to start_spark_session")

    try:
        jar_dir = "/home/jovyan/jars"

        # --- Step 1: Load environment variables ---
        s3_endpoint = os.getenv("MINIO_ENDPOINT")
        s3_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        hive_uri = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")

        # --- Step 2: Validate environment variables ---
        if not all([s3_endpoint, s3_access_key, s3_secret_key]):
            missing = []
            if not s3_endpoint:
                missing.append("MINIO_ENDPOINT")
            if not s3_access_key:
                missing.append("AWS_ACCESS_KEY_ID")
            if not s3_secret_key:
                missing.append("AWS_SECRET_ACCESS_KEY")

            missing_str = ", ".join(missing)
            logger.error(f"❌ Missing required environment variable(s): {missing_str}")
            raise EnvironmentError(f"Missing required environment variable(s): {missing_str}")

        # --- Step 3: Verify JARs ---
        try:
            all_jars = [
                os.path.join(jar_dir, f)
                for f in os.listdir(jar_dir)
                if f.endswith(".jar")
            ]
            if not all_jars:
                raise FileNotFoundError(f"No JARs found in {jar_dir}")

            # Define the required JARs for Spark + Delta + MinIO
            required_jars = {
                "hadoop-aws-3.3.4.jar",
                "aws-java-sdk-bundle-1.12.367.jar",
                "delta-core_2.12-2.4.0.jar",
                "delta-storage-2.4.0.jar",
            }

            available = {os.path.basename(f) for f in all_jars}
            missing_required = required_jars - available
            extra_jars = available - required_jars

            if missing_required:
                logger.error(f"❌ Missing required JAR(s): {', '.join(sorted(missing_required))}")
                logger.error(f"🧩 Found but incomplete JAR set: {', '.join(sorted(available))}")
                raise FileNotFoundError(
                    f"Missing required JAR(s): {', '.join(sorted(missing_required))}"
                )

            # Log the full picture
            logger.info(f"✅ All required JARs found: {', '.join(sorted(required_jars))}")

            if extra_jars:
                logger.warning(f"⚠️ Extra JAR(s) detected (not required for Delta + MinIO): {', '.join(sorted(extra_jars))}")

            # Create CSV string for Spark
            jars_csv = ",".join(all_jars)
            
        except Exception as e:
            logger.error(f"Error verifying JAR directory '{jar_dir}': {e}", exc_info=True)
            raise


        # --- Step 4: Initialize SparkSession ---
        try:
            logger.info("Starting Spark session...")
            spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.jars", jars_csv) \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .config("spark.executor.cores", "4") \
                .config("spark.memory.fraction", "0.6") \
                .config("spark.memory.storageFraction", "0.5") \
                .config("spark.sql.shuffle.partitions", "8") \
                .config("spark.default.parallelism", "8") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.sql.catalogImplementation", "hive") \
                .config("hive.metastore.uris", hive_uri) \
                .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
                .config("spark.hadoop.fs.s3a.access.key", s3_access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .enableHiveSupport() \
                .getOrCreate()

            logger.info("✅ Spark session started successfully")
            return spark

        except Exception as e:
            logger.error("❌ Failed to initialize Spark session", exc_info=True)
            raise

    except Exception as final_error:
        logger.critical("🚨 Unable to start Spark session. See error logs for details.")
        raise final_error
