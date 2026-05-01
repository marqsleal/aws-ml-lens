import os
from dataclasses import dataclass
from pathlib import Path


def load_dotenv(dotenv_path: Path | str = ".env") -> None:
    path = Path(dotenv_path)
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


load_dotenv()


@dataclass(frozen=True)
class Settings:
    # Kaggle
    KAGGLE_USERNAME: str = os.getenv("KAGGLE_USERNAME", "")
    KAGGLE_KEY: str = os.getenv("KAGGLE_KEY", "")
    KAGGLE_DATASET: str = os.getenv("KAGGLE_DATASET", "")

    # MinIO
    MINIO_API_PORT: int = int(os.getenv("MINIO_API_PORT", "9000"))
    MINIO_CONSOLE_PORT: int = int(os.getenv("MINIO_CONSOLE_PORT", "9001"))
    MINIO_ROOT_USER: str = os.getenv("MINIO_ROOT_USER", "minio_root_user")
    MINIO_ROOT_PASSWORD: str = os.getenv("MINIO_ROOT_PASSWORD", "minio_root_password")
    MINIO_BUCKET: str = os.getenv("MINIO_BUCKET", "aws-ml-lens")


settings = Settings()
