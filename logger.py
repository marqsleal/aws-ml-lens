import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from path import ARTEFACTS_DIR


def setup_logging(
    level: int = logging.INFO,
    log_dir: Path | None = None,
    log_filename: str = "app.log",
) -> None:
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return

    target_dir = log_dir or (ARTEFACTS_DIR / "logs")
    target_dir.mkdir(parents=True, exist_ok=True)
    log_file = target_dir / log_filename

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    root_logger.setLevel(level)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    setup_logging()
    return logging.getLogger(name)
