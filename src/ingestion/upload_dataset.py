from config import settings
from logger import get_logger
from path import SOR_DIR
from src.utils.s3_utils import upload_file_to_s3

logger = get_logger(__name__)

DATASET_NAME = "creditcard.parquet"
SOR_PATH = SOR_DIR / DATASET_NAME
SOR_BUCKET_KEY = f"sor/transactions/{DATASET_NAME}"


if __name__ == "__main__":
    upload_file_to_s3(SOR_PATH, settings.MINIO_BUCKET, SOR_BUCKET_KEY)
    logger.info("Done: %s", SOR_PATH)
