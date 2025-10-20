import os
import logging
from minio import Minio
from minio.error import S3Error


def get_minio_client(logger: logging.Logger | None = None) -> Minio:
    """
    Returns a MinIO client using environment variables.

    Environment Variables:
        MINIO_ENDPOINT         - MinIO server endpoint (e.g., http://minio:9000)
        AWS_ACCESS_KEY_ID      - Access key for MinIO
        AWS_SECRET_ACCESS_KEY  - Secret key for MinIO

    Args:
        logger (logging.Logger, optional): Logger instance for logging messages.

    Returns:
        Minio: Configured MinIO client instance.

    Raises:
        EnvironmentError: If required environment variables are missing.
        ConnectionError: If the MinIO client cannot connect to the endpoint.
    """
    # Use a basic logger if none provided
    if logger is None:
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    try:
        # --- Load environment variables ---
        endpoint = os.getenv("MINIO_ENDPOINT")
        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        # --- Validate environment variables ---
        missing = [var for var, val in {
            "MINIO_ENDPOINT": endpoint,
            "AWS_ACCESS_KEY_ID": access_key,
            "AWS_SECRET_ACCESS_KEY": secret_key
        }.items() if not val]

        if missing:
            logger.error(f"❌ Missing required environment variable(s): {', '.join(missing)}")
            raise EnvironmentError(f"Missing required environment variable(s): {', '.join(missing)}")

        # --- Create MinIO client ---
        endpoint_clean = endpoint.replace("http://", "").replace("https://", "")
        secure = endpoint.startswith("https://")

        try:
            client = Minio(endpoint_clean, access_key=access_key, secret_key=secret_key, secure=secure)
            # Optional: test connection (e.g., list_buckets)
            client.list_buckets()
            logger.info(f"✅ Connected successfully to MinIO at {endpoint}")
            return client

        except S3Error as e:
            logger.error(f"❌ Failed to connect to MinIO ({endpoint}): {e}")
            raise ConnectionError(f"Failed to connect to MinIO: {e}") from e

        except Exception as e:
            logger.error(f"❌ Unexpected error while creating MinIO client: {e}", exc_info=True)
            raise

    except Exception as e:
        logger.critical("🚨 Unable to initialize MinIO client. See logs for details.")
        raise
