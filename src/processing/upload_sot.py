from config import settings
from logger import get_logger
from path import SOT_DIR
from src.utils.s3_utils import upload_file_to_s3

logger = get_logger(__name__)

SOT_NAME = "creditcard_sot.parquet"
SOT_PATH = SOT_DIR / SOT_NAME
SOT_BUCKET_KEY = f"sot/transactions/{SOT_NAME}"


if __name__ == "__main__":
    upload_file_to_s3(SOT_PATH, settings.MINIO_BUCKET, SOT_BUCKET_KEY)
    logger.info("Done: %s", SOT_PATH)
