from config import settings
from logger import get_logger
from path import SOR_DIR
from src.utils.s3_client import ensure_bucket, get_s3_client

logger = get_logger(__name__)

DATASET_NAME = "creditcard.parquet"
DATASET_PATH = SOR_DIR / DATASET_NAME
BUCKET_KEY = f"sor/transactions/{DATASET_NAME}"


def upload_to_s3(df_path=DATASET_PATH, bucket=settings.MINIO_BUCKET, bucket_key=BUCKET_KEY):
    logger.info("Uploading dataset to S3: %s", df_path)
    if not df_path.exists():
        raise FileNotFoundError(f"Dataset file not found: {df_path}")

    s3 = get_s3_client()
    ensure_bucket(s3, bucket)

    s3.upload_file(str(df_path), bucket, bucket_key)
    logger.info("Dataset uploaded to S3 %s: %s", bucket, bucket_key)


if __name__ == "__main__":
    upload_to_s3()
    logger.info("Done: %s", DATASET_PATH)
