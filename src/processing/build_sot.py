import uuid
from pathlib import Path

import numpy as np
import pandas as pd

from config import settings
from logger import get_logger
from src.ingestion.upload_dataset import BUCKET_KEY as SOR_BUCKET_KEY
from src.processing.upload_sot import SOT_PATH
from src.utils.s3_client import get_df_from_s3, get_s3_client

logger = get_logger(__name__)

EXPECTED_COLUMNS = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount", "Class"]
ALLOWED_CLASSES = {0, 1}
TRANSACTION_ID_NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")


def validate_input(df: pd.DataFrame) -> None:
    logger.info("Validating SOR input data")

    missing_cols = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    extra_cols = [c for c in df.columns if c not in EXPECTED_COLUMNS]

    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")
    if extra_cols:
        raise ValueError(f"Unexpected columns: {extra_cols}")
    logger.info("Input columns validated")

    if not pd.api.types.is_numeric_dtype(df["Time"]):
        raise ValueError("Time must be numeric")
    if not pd.api.types.is_numeric_dtype(df["Amount"]):
        raise ValueError("Amount must be numeric")
    if not pd.api.types.is_numeric_dtype(df["Class"]):
        raise ValueError("Class must be numeric")
    logger.info("Input column dtypes validated")

    if not (df["Amount"] >= 0).all():
        raise ValueError("Amount has negative values")
    if not set(df["Class"].dropna().unique()).issubset(ALLOWED_CLASSES):
        raise ValueError("Class must be in {0,1}")
    logger.info("Input value constraints validated")


def build_sot(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building SOT DataFrame")
    sot_df: pd.DataFrame = df.copy()

    sot_df["amount_log"] = np.log1p(sot_df["Amount"]).astype("float64")
    logger.info("Column created: amount_log")

    sot_df["Time"] = sot_df["Time"].astype("int64")
    logger.info("Column casted: Time -> int64")
    sot_df["Class"] = sot_df["Class"].astype("int8")
    logger.info("Column casted: Class -> int8")
    sot_df["Amount"] = sot_df["Amount"].astype("float64")
    logger.info("Column casted: Amount -> float64")
    sot_df["amount_log"] = sot_df["amount_log"].astype("float64")
    logger.info("Column casted: amount_log -> float64")

    sot_df = sot_df.sort_values("Time").reset_index(drop=True)
    logger.info("DataFrame sorted by Time")
    sot_df["transaction_id"] = [
        str(uuid.uuid5(TRANSACTION_ID_NAMESPACE, f"tx-{index}")) for index in sot_df.index
    ]
    logger.info("Column created: transaction_id")

    return sot_df


def validate_sot(df_raw: pd.DataFrame, df_sot: pd.DataFrame) -> None:
    logger.info("Validating SOT output")
    if len(df_raw) != len(df_sot):
        raise ValueError("SOT row count does not match input row count")
    if not df_sot["transaction_id"].is_unique:
        raise ValueError("transaction_id is not unique")
    if not df_sot["Time"].is_monotonic_increasing:
        raise ValueError("Time is not sorted in ascending order")
    if not np.isfinite(df_sot["amount_log"]).all():
        raise ValueError("amount_log has invalid values")
    logger.info("SOT output validated")


def save_sot(df: pd.DataFrame, path: Path) -> None:
    logger.info("Saving SOT to local path: %s", path)
    df.to_parquet(path, index=False)
    logger.info("SOT saved locally")


def run_sot_pipeline() -> pd.DataFrame:
    logger.info("Starting SOT pipeline")
    s3_client = get_s3_client()
    sor_df = get_df_from_s3(s3_client, settings.MINIO_BUCKET, SOR_BUCKET_KEY)
    validate_input(sor_df)

    sot_df = build_sot(sor_df)
    validate_sot(sor_df, sot_df)

    save_sot(sot_df, SOT_PATH)
    logger.info("SOT pipeline finished")
    return sot_df


if __name__ == "__main__":
    run_sot_pipeline()
