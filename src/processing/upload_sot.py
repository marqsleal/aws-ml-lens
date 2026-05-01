from pathlib import Path

from config import settings
from logger import get_logger
from path import SOT_DIR
from src.utils.s3_client import ensure_bucket, get_s3_client

logger = get_logger(__name__)

SOT_NAME = "creditcard_sot.parquet"
SOT_PATH = SOT_DIR / SOT_NAME
SOT_BUCKET_KEY = f"sot/transactions/{SOT_NAME}"


def upload_to_s3(
    sot_path: Path = SOT_PATH,
    bucket: str = settings.MINIO_BUCKET,
    bucket_key: str = SOT_BUCKET_KEY,
) -> None:
    logger.info("Uploading SOT to S3: %s", sot_path)
    if not sot_path.exists():
        raise FileNotFoundError(f"SOT file not found: {sot_path}")

    s3 = get_s3_client()
    ensure_bucket(s3, bucket)
    s3.upload_file(str(sot_path), bucket, bucket_key)
    logger.info("SOT uploaded to S3 %s: %s", bucket, bucket_key)


if __name__ == "__main__":
    upload_to_s3()
    logger.info("Done: %s", SOT_PATH)
