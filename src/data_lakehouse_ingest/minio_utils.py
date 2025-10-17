import os
from minio import Minio

def get_minio_client():
    """
    Returns a MinIO client using environment variables.
    """
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    return Minio(
        endpoint.replace("http://", "").replace("https://", ""),
        access_key=access_key,
        secret_key=secret_key,
        secure=endpoint.startswith("https://")
    )
