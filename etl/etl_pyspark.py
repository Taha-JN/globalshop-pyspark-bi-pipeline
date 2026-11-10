"""
etl_pyspark.py

Backward-compatible facade for the GlobalShop Star Schema Lakehouse ETL pipeline.
Delegates to modular etl package components (etl.main, etl.engines).
"""
import sys
from pathlib import Path

# Add project root to sys.path if invoked directly
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.config import DEFAULT_INPUT_DIR, DEFAULT_OUTPUT_DIR, setup_logging
from etl.engines.pandas_engine import (
    PandasETLEngine,
    build_fact_funnel_pandas,
    build_star_schema_pandas,
    clean_online_retail_pandas,
)
from etl.engines.spark_engine import SparkETLEngine, is_spark_runnable
from etl.main import main


def spark_pipeline(input_dir: Path, output_dir: Path, spark_master: str = "local[*]"):
    """Legacy wrapper executing SparkETLEngine."""
    engine = SparkETLEngine(input_dir, output_dir, spark_master=spark_master)
    return engine.run()


def pandas_pipeline(input_dir: Path, output_dir: Path):
    """Legacy wrapper executing PandasETLEngine."""
    engine = PandasETLEngine(input_dir, output_dir)
    return engine.run()


if __name__ == "__main__":
    main()
