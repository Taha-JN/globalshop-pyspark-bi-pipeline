"""
etl/main.py

Primary CLI entrypoint for the GlobalShop Star Schema Lakehouse ETL pipeline.
Provides engine selection (Apache Spark vs. Vectorized Pandas), structured logging,
and validation.
"""
import argparse
import sys
from pathlib import Path

# Add project root to sys.path so 'etl' package is importable when run directly
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.config import DEFAULT_INPUT_DIR, DEFAULT_OUTPUT_DIR, setup_logging
from etl.engines.pandas_engine import PandasETLEngine
from etl.engines.spark_engine import SparkETLEngine, is_spark_runnable


def main():
    parser = argparse.ArgumentParser(
        description="GlobalShop Kimball Star Schema Lakehouse ETL Pipeline"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=str(DEFAULT_INPUT_DIR),
        help="Input raw data directory containing CSV files",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(DEFAULT_OUTPUT_DIR),
        help="Target Lakehouse output directory for Parquet tables",
    )
    parser.add_argument(
        "--engine",
        type=str,
        choices=["auto", "spark", "pandas"],
        default="auto",
        help="Execution engine ('spark', 'pandas', or 'auto' for dynamic JVM detection)",
    )
    parser.add_argument(
        "--spark-master",
        type=str,
        default="local[*]",
        help="Apache Spark cluster master URL (default: local[*])",
    )
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger = setup_logging(output_dir)
    logger.info(
        "Starting GlobalShop Lakehouse ETL Pipeline. Input=%s, Output=%s, RequestedEngine=%s",
        input_dir,
        output_dir,
        args.engine,
    )

    use_spark = False
    if args.engine == "spark":
        use_spark = True
    elif args.engine == "auto":
        use_spark = is_spark_runnable()

    if use_spark:
        try:
            logger.info("Initializing Distributed Apache Spark Engine...")
            spark_engine = SparkETLEngine(
                input_dir=input_dir,
                output_dir=output_dir,
                spark_master=args.spark_master,
            )
            spark_engine.run()
            logger.info("Spark ETL Pipeline execution finished successfully.")
            return
        except Exception as e:
            if args.engine == "spark":
                logger.error("Spark execution failed in explicit spark mode: %s", e, exc_info=True)
                sys.exit(1)
            logger.warning(
                "Spark execution encountered an issue: %s. Defaulting to Vectorized Pandas Engine.",
                e,
            )

    logger.info("Initializing Vectorized Pandas & PyArrow Engine...")
    pandas_engine = PandasETLEngine(input_dir=input_dir, output_dir=output_dir)
    pandas_engine.run()
    logger.info("Pandas ETL Pipeline execution finished successfully.")


if __name__ == "__main__":
    main()
