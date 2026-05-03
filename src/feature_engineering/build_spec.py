from pathlib import Path

import numpy as np
import pandas as pd

from config import settings
from logger import get_logger
from src.feature_engineering.upload_spec import SPEC_PATH
from src.processing.upload_sot import SOT_BUCKET_KEY
from src.utils.s3_utils import get_df_from_s3, get_s3_client

HOUR_PER_DAY: int = 24
SECONDS_PER_HOUR: int = 3600
Z_SCORE_GUARDRAIL = 1e-9
LOW_QUANTILE = 0.10
HIGH_QUANTILE = 0.90
BASELINE_IMPORTANCE_VALUE_FEATURES: list[str] = ["V14", "V10", "V12", "V4"]
DIV_AMOUNT_GUARDRAIL = 1e-6
V_COLS: list[str] = [f"V{i}" for i in range(1, 29)]
BASE_COLUMNS: list[str] = ["Time", *V_COLS, "Amount", "Class", "transaction_id"]
BASE_SPEC_FEATURE_COLUMNS: list[str] = [
    "t_sec",
    "t_hour",
    "t_day",
    "hour_of_day",
    "hour_sin",
    "hour_cos",
    "t_dt",
    "tx_count_last_1h",
    "amount_log",
    "amount_zscore",
    "amount_is_low",
    "amount_is_high",
    "avg_amount_last_1h",
    "v_l2_norm",
    "v_l1_norm",
    "v_max_abs",
    "v_extreme_count",
    "v_mean",
    "v_std",
    "v_skew_proxy",
]
IMPORTANCE_SPEC_FEATURE_COLUMNS: list[str] = [
    feature
    for c in BASELINE_IMPORTANCE_VALUE_FEATURES
    for feature in (f"{c}_x_amount_log", f"{c}_div_amount")
]
SPEC_FEATURE_COLUMNS: list[str] = BASE_SPEC_FEATURE_COLUMNS + IMPORTANCE_SPEC_FEATURE_COLUMNS

logger = get_logger(__name__)


def build_temporal_featutures(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    logger.info("Building temporal features")

    df["t_sec"] = df["Time"]
    logger.info("Column copied: Time -> t_sec")

    df["t_hour"] = df["t_sec"] / SECONDS_PER_HOUR
    logger.info("Column created: t_hour = t_sec / 3600")

    df["t_day"] = df["t_hour"] / HOUR_PER_DAY
    logger.info("Column created: t_day = t_hour / 24")

    df["hour_of_day"] = ((df["t_sec"] // SECONDS_PER_HOUR) % HOUR_PER_DAY).astype(int)
    logger.info("Column created: hour_of_day = (t_sec // 3600) % 24")

    df["hour_sin"] = np.sin(2 * np.pi * df["hour_of_day"] / HOUR_PER_DAY)
    logger.info("Column created: hour_sin = sin(2 * pi * hour_of_day / 24)")

    df["hour_cos"] = np.cos(2 * np.pi * df["hour_of_day"] / HOUR_PER_DAY)
    logger.info("Column created: hour_cos = cos(2 * pi * hour_of_day / 24)")

    df["t_dt"] = pd.to_datetime(df["t_sec"], unit="s")
    logger.info("Column created: t_dt = pd.to_datetime(t_sec, unit='s')")

    df["tx_count_last_1h"] = (
        df.set_index("t_dt")["transaction_id"].rolling("1h").count().shift(1).values
    )
    df["tx_count_last_1h"] = df["tx_count_last_1h"].fillna(0.0)
    logger.info("Column created: tx_count_last_1h = rolling count of transactions in last 1 hour")

    logger.info("Temporal features built")

    return df


def build_amount_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    logger.info("Building amount features")

    df["amount_log"] = np.log1p(df["Amount"])
    logger.info("Column created: amount_log = log1p(Amount)")

    df["amount_zscore"] = (df["Amount"] - df["Amount"].mean()) / (
        df["Amount"].std() + Z_SCORE_GUARDRAIL
    )
    logger.info("Column created: amount_zscore = (Amount - mean) / std")

    df["amount_is_low"] = (df["Amount"] < df["Amount"].quantile(LOW_QUANTILE)).astype(int)
    logger.info("Column created: amount_is_low = Amount < %s quantile", LOW_QUANTILE)

    df["amount_is_high"] = (df["Amount"] > df["Amount"].quantile(HIGH_QUANTILE)).astype(int)
    logger.info("Column created: amount_is_high = Amount > %s quantile", HIGH_QUANTILE)

    df["avg_amount_last_1h"] = (
        df.set_index("t_dt")["amount_log"].rolling("1h").mean().shift(1).values
    )
    df["avg_amount_last_1h"] = df["avg_amount_last_1h"].fillna(0.0)
    logger.info("Column created: avg_amount_last_1h = rolling mean of amount_log in last 1 hour")

    logger.info("Amount features built")

    return df


def build_mag_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    logger.info("Building mag features")

    df["v_l2_norm"] = np.sqrt(np.sum(df[V_COLS] ** 2, axis=1))
    logger.info("Column created: v_l2_norm = sqrt(sum of squares of V1-V28)")

    df["v_l1_norm"] = df[V_COLS].abs().sum(axis=1)
    logger.info("Column created: v_l1_norm = sum of absolute values of V1-V28")

    df["v_max_abs"] = df[V_COLS].abs().max(axis=1)
    logger.info("Column created: v_max_abs = max of absolute values of V1-V28")

    logger.info("Mag features built")

    return df


def build_ext_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    logger.info("Building extremes features")

    absV = df[V_COLS].abs()
    thr = absV.stack().quantile(HIGH_QUANTILE)

    df["v_extreme_count"] = (absV > thr).sum(axis=1)
    logger.info(
        "Column created: v_extreme_count = count of V1-V28 with abs value > %s quantile",
        HIGH_QUANTILE,
    )

    logger.info("Extreme features built")

    return df


def build_statistical_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    logger.info("Building statistical features")

    df["v_mean"] = df[V_COLS].mean(axis=1)
    logger.info("Column created: v_mean = mean of V1-V28")

    df["v_std"] = df[V_COLS].std(axis=1)
    logger.info("Column created: v_std = std of V1-V28")

    df["v_skew_proxy"] = ((df[V_COLS] - df["v_mean"].values.reshape(-1, 1)) ** 3).mean(axis=1)
    logger.info("Column created: v_skew_proxy = proxy for skewness of V1-V28")

    logger.info("Statistical features built")

    return df


def build_importance_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    logger.info("Building importance features")

    for c in BASELINE_IMPORTANCE_VALUE_FEATURES:
        df[f"{c}_x_amount_log"] = df[c] * df["amount_log"]
        logger.info("Column created: %s_x_amount_log = %s * amount_log", c, c)

        df[f"{c}_div_amount"] = df[c] / (df["amount_log"] + DIV_AMOUNT_GUARDRAIL)
        logger.info(
            "Column created: %s_div_amount = %s / (amount_log + %s)",
            c,
            c,
            DIV_AMOUNT_GUARDRAIL,
        )

    logger.info("Importance features built")
    return df


def build_spec(df) -> pd.DataFrame:

    df = build_temporal_featutures(df)
    df = build_amount_features(df)
    df = build_mag_features(df)
    df = build_ext_features(df)
    df = build_statistical_features(df)
    df = build_importance_features(df)

    return df


def validate_spec(df: pd.DataFrame) -> None:
    logger.info("Validating SPEC output")

    expected_columns = BASE_COLUMNS + SPEC_FEATURE_COLUMNS
    missing_cols = [c for c in expected_columns if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing SPEC columns: {missing_cols}")

    if len(df) == 0:
        raise ValueError("SPEC is empty")

    if not df["transaction_id"].is_unique:
        raise ValueError("transaction_id is not unique in SPEC")
    if not df["Time"].is_monotonic_increasing:
        raise ValueError("Time is not sorted in ascending order in SPEC")

    if not pd.api.types.is_datetime64_any_dtype(df["t_dt"]):
        raise ValueError("t_dt must be datetime dtype")
    if not pd.api.types.is_integer_dtype(df["hour_of_day"]):
        raise ValueError("hour_of_day must be integer dtype")

    if not df["hour_of_day"].between(0, HOUR_PER_DAY - 1).all():
        raise ValueError("hour_of_day must be within [0, 23]")

    if not df["hour_sin"].between(-1.000001, 1.000001).all():
        raise ValueError("hour_sin must be within [-1, 1]")
    if not df["hour_cos"].between(-1.000001, 1.000001).all():
        raise ValueError("hour_cos must be within [-1, 1]")

    for col in ["amount_is_low", "amount_is_high"]:
        unique_values = set(df[col].dropna().unique())
        if not unique_values.issubset({0, 1}):
            raise ValueError(f"{col} must contain only 0/1")

    if (df["v_extreme_count"] < 0).any() or (df["v_extreme_count"] > len(V_COLS)).any():
        raise ValueError("v_extreme_count must be within [0, 28]")

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    for col in numeric_cols:
        if np.isinf(df[col]).any():
            raise ValueError(f"Column has infinite values: {col}")
        if df[col].isna().any():
            raise ValueError(f"Column has NaN values: {col}")

    logger.info("SPEC output validated")


def save_spec(df: pd.DataFrame, path: Path) -> None:
    logger.info("Saving SPEC to local path: %s", path)
    df.to_parquet(path, index=False)
    logger.info("SPEC saved locally")


def run_spec_pipeline() -> pd.DataFrame:
    logger.info("Starting SPEC pipeline")

    client = get_s3_client()
    df = get_df_from_s3(client, bucket=settings.MINIO_BUCKET, key=SOT_BUCKET_KEY)

    spec_df = build_spec(df)
    validate_spec(spec_df)
    save_spec(spec_df, SPEC_PATH)

    logger.info("SPEC pipeline finished")
    return spec_df


if __name__ == "__main__":
    run_spec_pipeline()
