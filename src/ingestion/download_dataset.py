import os
import tempfile
from pathlib import Path

import pandas as pd

from config import settings
from logger import get_logger
from path import SOR_DIR

logger = get_logger(__name__)
DATASET_CSV_NAME = "creditcard.csv"
DATASET_PARQUET_NAME = "creditcard.parquet"


def download_dataset(
    dataset: str = settings.KAGGLE_DATASET,
    destination: Path = SOR_DIR,
):
    logger.info("Starting dataset download from Kaggle: %s", dataset)
    os.environ["KAGGLE_USERNAME"] = settings.KAGGLE_USERNAME
    os.environ["KAGGLE_KEY"] = settings.KAGGLE_KEY
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()
    with tempfile.TemporaryDirectory(prefix="kaggle_download_") as temp_dir:
        logger.info("Downloading dataset to temporary directory: %s", temp_dir)
        api.dataset_download_files(
            dataset=dataset,
            path=temp_dir,
            unzip=True,
        )
        csv_path = Path(temp_dir) / DATASET_CSV_NAME
        if not csv_path.exists():
            raise FileNotFoundError(f"Expected CSV file not found after download: {csv_path}")

        parquet_path = destination / DATASET_PARQUET_NAME
        logger.info("Converting dataset from CSV to Parquet: %s", parquet_path)
        pd.read_csv(csv_path).to_parquet(parquet_path, index=False)

    parquet_path = destination / DATASET_PARQUET_NAME
    logger.info("Temporary download directory cleaned up")
    logger.info("Dataset downloaded and persisted as Parquet: %s", parquet_path)


if __name__ == "__main__":
    download_dataset()
    logger.info("Done: %s", SOR_DIR)
