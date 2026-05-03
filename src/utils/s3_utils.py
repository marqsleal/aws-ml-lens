import logging
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError

from config import settings

logger = logging.getLogger(__name__)


def get_s3_client():
    endpoint_url = f"http://localhost:{settings.MINIO_API_PORT}"
    logger.info("Creating S3 client with endpoint: %s", endpoint_url)
    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=settings.MINIO_ROOT_USER,
        aws_secret_access_key=settings.MINIO_ROOT_PASSWORD,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    logger.info("S3 client created successfully")
    return client


def ensure_bucket(s3_client, bucket: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket)
        logger.info("Bucket exists: %s", bucket)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in {"404", "NoSuchBucket"}:
            logger.info("Bucket does not exist, creating: %s", bucket)
            s3_client.create_bucket(Bucket=bucket)
            return
        raise


def get_df_from_s3(s3_client, bucket: str, key: str):
    logger.info("Downloading file from S3: s3://%s/%s", bucket, key)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    payload = BytesIO(obj["Body"].read())
    if key.endswith(".parquet"):
        df = pd.read_parquet(payload)
    elif key.endswith(".csv"):
        df = pd.read_csv(payload)
    else:
        raise ValueError(f"Unsupported file extension for DataFrame loading: {key}")
    logger.info("File downloaded and loaded into DataFrame successfully")
    return df


def upload_file_to_s3(local_path: Path, bucket: str, bucket_key: str) -> None:
    logger.info("Uploading file to S3: %s", local_path)
    if not local_path.exists():
        raise FileNotFoundError(f"File not found: {local_path}")

    s3 = get_s3_client()
    ensure_bucket(s3, bucket)
    s3.upload_file(str(local_path), bucket, bucket_key)
    logger.info("File uploaded to S3 %s: %s", bucket, bucket_key)
