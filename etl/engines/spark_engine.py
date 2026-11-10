"""
etl/engines/spark_engine.py

Distributed Apache Spark implementation of the GlobalShop Star Schema Lakehouse ETL pipeline.
Resolves cluster bottlenecks with distributed-safe surrogate keys, full DimRegion referential
integrity, and FactFunnel behavioral clickstream integration.
"""
import json
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from etl.engines.base import BaseETLEngine
from etl.schemas import (
    DIM_CUSTOMER_COLUMNS,
    DIM_DATE_COLUMNS,
    DIM_PRODUCT_COLUMNS,
    DIM_REGION_COLUMNS,
    FACT_FUNNEL_COLUMNS,
    FACT_SALES_COLUMNS,
    StarSchemaContainer,
    UNKNOWN_CUSTOMER_ID,
)

logger = logging.getLogger("GlobalShop_ETL")

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )
    from pyspark.sql.window import Window
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


def is_spark_runnable() -> bool:
    """Pre-flight check: Verify PySpark is installed and local JVM is compatible (Java 17+)."""
    if not SPARK_AVAILABLE:
        return False
    try:
        res = subprocess.run(
            ["java", "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5,
        )
        out = res.stderr or res.stdout
        # PySpark 4+ requires Java 17+; Java 8 (1.8.0) is incompatible
        if "1.8.0" in out or 'version "1.' in out:
            logger.info("Local JVM is Java 8; PySpark 4.x requires Java 17+. Defaulting to Pandas/PyArrow engine.")
            return False
        return True
    except Exception:
        return False


class SparkETLEngine(BaseETLEngine):
    """Distributed Apache Spark ETL engine for high-throughput batch lakehouse ingestion."""

    def __init__(self, input_dir: Path, output_dir: Path, spark_master: str = "local[*]"):
        super().__init__(input_dir, output_dir)
        self.spark_master = spark_master
        self.spark: SparkSession = None

    def _get_spark_session(self) -> SparkSession:
        if self.spark is None:
            self.spark = (
                SparkSession.builder.master(self.spark_master)
                .appName("GlobalShop_StarSchema_Lakehouse_ETL")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .getOrCreate()
            )
        return self.spark

    def extract(self) -> Dict[str, Any]:
        spark = self._get_spark_session()
        logger.info("Extracting CSV datasets with SparkSession from: %s", self.input_dir)

        retail_path = str(self.input_dir / "online_retail_ii.csv")
        sample_path = str(self.input_dir / "sample_1k_online_retail.csv")
        actual_path = retail_path if Path(retail_path).exists() else sample_path

        raw_retail = spark.read.option("header", True).csv(actual_path)

        orders_path = self.input_dir / "olist_orders.csv"
        raw_orders = (
            spark.read.option("header", True).csv(str(orders_path))
            if orders_path.exists()
            else None
        )

        events_path = self.input_dir / "retailrocket_events.csv"
        raw_events = (
            spark.read.option("header", True).csv(str(events_path))
            if events_path.exists()
            else None
        )

        return {
            "raw_retail": raw_retail,
            "raw_orders": raw_orders,
            "raw_events": raw_events,
        }

    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        spark = self._get_spark_session()
        raw_online = raw_data["raw_retail"]
        raw_count = raw_online.count()

        # 1. Clean and validate retail transactions
        online = (
            raw_online.withColumn("Quantity", F.col("Quantity").cast(IntegerType()))
            .withColumn("UnitPrice", F.col("UnitPrice").cast(DoubleType()))
            .filter((F.col("Quantity") > 0) & (F.col("UnitPrice") > 0))
            .withColumn("InvoiceTimestamp", F.to_timestamp(F.col("InvoiceDate")))
            .filter(F.col("InvoiceTimestamp").isNotNull())
            .withColumn("TotalPrice", F.round(F.col("Quantity") * F.col("UnitPrice"), 4))
            .withColumn("Year", F.year(F.col("InvoiceTimestamp")))
            .withColumn("Month", F.month(F.col("InvoiceTimestamp")))
            .withColumn("Date", F.to_date(F.col("InvoiceTimestamp")))
            .withColumn(
                "CustomerID",
                F.when(
                    F.col("CustomerID").isNull()
                    | F.trim(F.col("CustomerID")).isin("", "nan", "None", "null"),
                    F.lit(UNKNOWN_CUSTOMER_ID),
                ).otherwise(F.trim(F.col("CustomerID"))),
            )
            .withColumn(
                "Channel",
                F.when(F.col("Country") == "United Kingdom", F.lit("web")).otherwise(F.lit("marketplace")),
            )
            .dropDuplicates(["InvoiceNo", "StockCode", "InvoiceTimestamp", "CustomerID"])
        ).persist()

        clean_count = online.count()

        # 2. DimRegion (Deduplicated country mappings - Distributed 64-bit SHA-256 Surrogate Key)
        dim_region = (
            online.select("Country")
            .distinct()
            .withColumn(
                "RegionKey",
                F.conv(F.substring(F.sha2(F.col("Country"), 256), 1, 15), 16, 10).cast(LongType()),
            )
            .select(DIM_REGION_COLUMNS)
        ).persist()

        # 3. DimProduct (Preserve UnitPrice schema parity - Distributed 64-bit SHA-256 Surrogate Key)
        dim_product = (
            online.groupBy("StockCode", "Description")
            .agg(F.round(F.mean("UnitPrice"), 4).alias("UnitPrice"))
            .dropDuplicates(["StockCode"])
            .withColumn(
                "ProductKey",
                F.conv(F.substring(F.sha2(F.col("StockCode"), 256), 1, 15), 16, 10).cast(LongType()),
            )
            .select(DIM_PRODUCT_COLUMNS)
        ).persist()

        # 4. DimCustomer (Enforce referential integrity to DimRegion - Distributed 64-bit SHA-256 Surrogate Key)
        dim_customer = (
            online.select("CustomerID", "Country")
            .dropDuplicates(["CustomerID"])
            .join(dim_region.select("Country", "RegionKey"), on="Country", how="left")
            .withColumn(
                "CustomerKey",
                F.conv(F.substring(F.sha2(F.col("CustomerID"), 256), 1, 15), 16, 10).cast(LongType()),
            )
            .select(DIM_CUSTOMER_COLUMNS)
        ).persist()

        # 5. DimDate
        dim_date = (
            online.select("Date")
            .distinct()
            .withColumn("DateKey", F.date_format(F.col("Date"), "yyyyMMdd").cast(IntegerType()))
            .withColumn("FullDate", F.col("Date"))
            .withColumn("Year", F.year(F.col("Date")))
            .withColumn("Month", F.month(F.col("Date")))
            .withColumn("Day", F.dayofmonth(F.col("Date")))
            .withColumn("DayOfWeek", F.dayofweek(F.col("Date")))
            .withColumn("Quarter", F.quarter(F.col("Date")))
            .select(DIM_DATE_COLUMNS)
        )

        # 6. FactSales (Distributed-Safe Surrogate Keys - Decoupled RegionKey from DimCustomer)
        # Replaces single-partition Window with distributed cryptographic SHA256 key
        # Directly joins DimRegion via transaction Country to prevent guest misattribution
        fact_sales = (
            online.join(dim_product.select("StockCode", "ProductKey"), on="StockCode", how="left")
            .join(dim_customer.select("CustomerID", "CustomerKey"), on="CustomerID", how="left")
            .join(dim_region.select("Country", "RegionKey"), on="Country", how="left")
            .withColumn("DateKey", F.date_format(F.col("Date"), "yyyyMMdd").cast(IntegerType()))
            .withColumn("RegionKey", F.coalesce(F.col("RegionKey"), F.lit(-1)).cast(LongType()))
            .withColumn(
                "SalesKey",
                F.substring(
                    F.sha2(
                        F.concat_ws(
                            "_",
                            F.col("InvoiceNo"),
                            F.col("StockCode"),
                            F.col("InvoiceTimestamp").cast(StringType()),
                        ),
                        256,
                    ),
                    1,
                    16,
                ),
            )
            .select(FACT_SALES_COLUMNS)
        )

        # 7. FactFunnel (Behavioral Clickstream Data Integration)
        raw_events = raw_data.get("raw_events")
        if raw_events is not None:
            # Aggregate and pivot clickstream events by visitor and item
            funnel_agg = (
                raw_events.groupBy("visitorid", "itemid")
                .pivot("event_type", ["view", "addtocart", "transaction"])
                .count()
                .na.fill(0)
                .withColumnRenamed("visitorid", "VisitorID")
                .withColumnRenamed("itemid", "StockCode")
                .withColumnRenamed("view", "Views")
                .withColumnRenamed("addtocart", "AddToCart")
                .withColumnRenamed("transaction", "Transactions")
            )

            fact_funnel = (
                funnel_agg.join(
                    dim_product.select("StockCode", "ProductKey"), on="StockCode", how="left"
                )
                .withColumn("ProductKey", F.coalesce(F.col("ProductKey"), F.lit(-1)))
                .withColumn("Converted", F.col("Transactions") > 0)
                .withColumn(
                    "FunnelKey",
                    F.substring(
                        F.sha2(F.concat_ws("_", F.col("VisitorID"), F.col("StockCode")), 256), 1, 16
                    ),
                )
                .select(FACT_FUNNEL_COLUMNS)
            )
        else:
            fact_funnel = None

        return StarSchemaContainer(
            fact_sales=fact_sales,
            fact_funnel=fact_funnel,
            dim_product=dim_product,
            dim_customer=dim_customer,
            dim_date=dim_date,
            dim_region=dim_region,
            clean_metrics={
                "raw_records": raw_count,
                "clean_records": clean_count,
            },
            cleaned_sales=online,
            orders_clean=raw_data.get("raw_orders"),
            events_clean=raw_data.get("raw_events"),
        )

    def load(self, star_schema: StarSchemaContainer) -> Dict[str, Any]:
        output_dir = self.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        fact_sales = star_schema.fact_sales
        fact_funnel = star_schema.fact_funnel
        dim_product = star_schema.dim_product
        dim_customer = star_schema.dim_customer
        dim_date = star_schema.dim_date
        dim_region = star_schema.dim_region
        online = star_schema.cleaned_sales

        try:
            # Persist FactSales (Partitioned by Year, Month only - eliminating redundant monolithic export)
            fact_sales.write.partitionBy("Year", "Month").mode("overwrite").parquet(
                str(output_dir / "fact_sales")
            )

            # Persist Dimensions
            dim_product.write.mode("overwrite").parquet(str(output_dir / "dim_product.parquet"))
            dim_customer.write.mode("overwrite").parquet(str(output_dir / "dim_customer.parquet"))
            dim_date.write.mode("overwrite").parquet(str(output_dir / "dim_date.parquet"))
            dim_region.write.mode("overwrite").parquet(str(output_dir / "dim_region.parquet"))
            if online is not None:
                online.write.mode("overwrite").parquet(str(output_dir / "online_retail_clean.parquet"))

            # Persist FactFunnel
            funnel_count = 0
            if fact_funnel is not None:
                fact_funnel.write.mode("overwrite").parquet(str(output_dir / "fact_funnel.parquet"))
                funnel_count = fact_funnel.count()

            # Clean auxiliary datasets
            if star_schema.orders_clean is not None:
                star_schema.orders_clean.write.mode("overwrite").parquet(
                    str(output_dir / "orders_clean.parquet")
                )
            if star_schema.events_clean is not None:
                star_schema.events_clean.write.mode("overwrite").parquet(
                    str(output_dir / "events_clean.parquet")
                )

            raw_count = star_schema.clean_metrics.get("raw_records", 0)
            clean_count = star_schema.clean_metrics.get("clean_records", 0)
            summary = {
                "pipeline_completed_at": datetime.now(timezone.utc).isoformat(),
                "engine": "pyspark",
                "raw_records": raw_count,
                "fact_sales_records": clean_count,
                "fact_funnel_records": funnel_count,
                "retention_rate_pct": round(
                    (clean_count / max(1, raw_count)) * 100, 2
                ),
            }

            with open(output_dir / "summary.json", "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2)

            logger.info("Distributed Spark pipeline successfully executed. Retention: %.2f%%", summary["retention_rate_pct"])
            return summary
        finally:
            logger.info("Releasing cached Spark DataFrames to prevent executor memory leaks...")
            cached_dfs = [
                star_schema.cleaned_sales,
                star_schema.dim_region,
                star_schema.dim_product,
                star_schema.dim_customer,
            ]
            for df in cached_dfs:
                if df is not None:
                    try:
                        df.unpersist()
                    except Exception as ex:
                        logger.warning("Error unpersisting Spark DataFrame: %s", ex)

    def run(self) -> Dict[str, Any]:
        try:
            return super().run()
        finally:
            if self.spark is not None:
                self.spark.stop()
                self.spark = None
