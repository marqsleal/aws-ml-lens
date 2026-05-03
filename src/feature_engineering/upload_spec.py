from config import settings
from logger import get_logger
from path import SPEC_DIR
from src.utils.s3_utils import upload_file_to_s3

logger = get_logger(__name__)

SPEC_NAME = "creditcard_spec.parquet"
SPEC_PATH = SPEC_DIR / SPEC_NAME
SPEC_BUCKET_KEY = f"SPEC/transactions/{SPEC_NAME}"


if __name__ == "__main__":
    upload_file_to_s3(SPEC_PATH, settings.MINIO_BUCKET, SPEC_BUCKET_KEY)
    logger.info("Done: %s", SPEC_PATH)
