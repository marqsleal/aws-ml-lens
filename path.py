from pathlib import Path

# Root
PROJECT_ROOT = Path(__file__).resolve().parent

# Main folders
SRC_DIR = PROJECT_ROOT / "src"
DATA_DIR = PROJECT_ROOT / "data"
ARTEFACTS_DIR = PROJECT_ROOT / "artefacts"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
SERVING_DIR = PROJECT_ROOT / "serving"

# Data layers
SOR_DIR = DATA_DIR / "SOR"
SOT_DIR = DATA_DIR / "SOT"
SPEC_DIR = DATA_DIR / "SPEC"
MODELS_DIR = DATA_DIR / "models"
MINIO_DATA_DIR = DATA_DIR / "minio"

# Environment files
ENV_FILE = PROJECT_ROOT / ".env"


for _dir in (
    SRC_DIR,
    DATA_DIR,
    ARTEFACTS_DIR,
    NOTEBOOKS_DIR,
    SERVING_DIR,
    SOR_DIR,
    SOT_DIR,
    SPEC_DIR,
    MODELS_DIR,
    MINIO_DATA_DIR,
):
    _dir.mkdir(parents=True, exist_ok=True)
