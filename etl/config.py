"""
etl/config.py

Centralized configuration, paths, logging, and environment settings
for the GlobalShop Enterprise Big Data & Analytics Pipeline.
"""
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")
DEFAULT_INPUT_DIR = PROJECT_ROOT / "data"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "etl" / "output"
DEFAULT_SQL_DIR = PROJECT_ROOT / "sql"
DEFAULT_SQL_QUERIES_DIR = DEFAULT_SQL_DIR / "queries"
DEFAULT_SQL_OUTPUTS_DIR = DEFAULT_SQL_QUERIES_DIR / "outputs"
DEFAULT_ANALYSIS_OUTPUT_DIR = PROJECT_ROOT / "analysis" / "output"


@dataclass
class ETLConfig:
    """Runtime configuration settings for ETL pipeline execution."""
    input_dir: Path = DEFAULT_INPUT_DIR
    output_dir: Path = DEFAULT_OUTPUT_DIR
    spark_master: str = os.getenv("SPARK_MASTER", "local[*]")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    engine: str = os.getenv("ETL_ENGINE", "auto")  # 'auto', 'spark', 'pandas'


def setup_logging(output_dir: Path, log_level: str = "INFO") -> logging.Logger:
    """
    Configure structured logging to stdout and file handler (etl.log).
    Ensures safe handler registration without clobbering external loggers.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    log_file = output_dir / "etl.log"

    logger = logging.getLogger("GlobalShop_ETL")
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)

    # Avoid adding duplicate handlers if already initialized
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
