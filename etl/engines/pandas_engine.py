"""
etl/engines/pandas_engine.py

High-performance, vectorized Pandas & PyArrow implementation of the GlobalShop
Star Schema Lakehouse ETL pipeline.
"""
import hashlib
import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd

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


def clean_online_retail_pandas(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Clean raw online retail transactional data:
    - Cast types
    - Remove cancellations/returns (negative/zero Quantity)
    - Remove non-positive UnitPrice
    - Standardize timestamps
    - Impute missing CustomerIDs as 'GUEST'
    - Deduplicate line items
    """
    initial_count = len(df)
    metrics = {"raw_records": initial_count}

    df = df.copy()
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype(int)
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce").fillna(0.0).astype(float)

    # Filter out returns, damaged items, promotional zero-prices
    valid_mask = (df["Quantity"] > 0) & (df["UnitPrice"] > 0)
    metrics["negative_or_zero_qty_dropped"] = int((df["Quantity"] <= 0).sum())
    metrics["zero_unit_price_dropped"] = int((df["UnitPrice"] <= 0).sum())

    cleaned = df[valid_mask].copy()

    # Parse timestamps
    cleaned["InvoiceTimestamp"] = pd.to_datetime(cleaned["InvoiceDate"], errors="coerce")
    cleaned = cleaned.dropna(subset=["InvoiceTimestamp"])

    # Derived attributes
    cleaned["TotalPrice"] = np.round(cleaned["Quantity"] * cleaned["UnitPrice"], 4)
    cleaned["Year"] = cleaned["InvoiceTimestamp"].dt.year
    cleaned["Month"] = cleaned["InvoiceTimestamp"].dt.month
    cleaned["Date"] = cleaned["InvoiceTimestamp"].dt.date

    # Missing customer imputation
    cleaned["CustomerID"] = cleaned["CustomerID"].astype(str).str.strip()
    missing_cust = cleaned["CustomerID"].isna() | cleaned["CustomerID"].isin(["", "nan", "None", "null"])
    metrics["missing_customers_imputed"] = int(missing_cust.sum())
    cleaned.loc[missing_cust, "CustomerID"] = UNKNOWN_CUSTOMER_ID

    # Multi-channel allocation
    cleaned["Channel"] = np.where(cleaned["Country"] == "United Kingdom", "web", "marketplace")

    # Deduplicate line items
    before_dedup = len(cleaned)
    cleaned = cleaned.drop_duplicates(subset=["InvoiceNo", "StockCode", "InvoiceTimestamp", "CustomerID"])
    metrics["duplicates_dropped"] = before_dedup - len(cleaned)
    metrics["clean_records"] = len(cleaned)

    return cleaned, metrics


def build_fact_funnel_pandas(
    events_df: pd.DataFrame, dim_product: pd.DataFrame
) -> pd.DataFrame:
    """
    Construct FactFunnel from behavioral clickstream events (RetailRocket):
    Pivots event actions (view, addtocart, transaction) per visitor & item,
    and links to DimProduct.
    """
    if events_df is None or events_df.empty:
        return pd.DataFrame(columns=FACT_FUNNEL_COLUMNS)

    df = events_df.copy()
    df["visitorid"] = df["visitorid"].astype(str).str.strip()
    df["itemid"] = df["itemid"].astype(str).str.strip()

    # Pivot event counts per visitor and item
    pivot_df = (
        df.groupby(["visitorid", "itemid", "event_type"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    for col in ["view", "addtocart", "transaction"]:
        if col not in pivot_df.columns:
            pivot_df[col] = 0

    pivot_df = pivot_df.rename(
        columns={
            "visitorid": "VisitorID",
            "itemid": "StockCode",
            "view": "Views",
            "addtocart": "AddToCart",
            "transaction": "Transactions",
        }
    )

    # Link with DimProduct surrogate key
    merged = pivot_df.merge(
        dim_product[["StockCode", "ProductKey"]], on="StockCode", how="left"
    )
    merged["ProductKey"] = merged["ProductKey"].fillna(-1).astype(int)

    # Conversion flag
    merged["Converted"] = merged["Transactions"] > 0

    # Deterministic distributed-safe surrogate key
    merged["FunnelKey"] = [
        hashlib.sha256(f"{v}_{s}".encode("utf-8")).hexdigest()[:16]
        for v, s in zip(merged["VisitorID"], merged["StockCode"])
    ]

    return merged[FACT_FUNNEL_COLUMNS]


def build_star_schema_pandas(
    cleaned_sales: pd.DataFrame,
    events_df: pd.DataFrame = None,
    orders_df: pd.DataFrame = None,
) -> StarSchemaContainer[pd.DataFrame]:
    """
    Construct Kimball Star Schema:
    - DimRegion (RegionKey PK, Country)
    - DimProduct (ProductKey PK, StockCode, Description, UnitPrice)
    - DimCustomer (CustomerKey PK, CustomerID, Country, RegionKey FK)
    - DimDate (DateKey PK, FullDate, Year, Month, Day, DayOfWeek, Quarter)
    - FactSales (SalesKey PK, ProductKey, CustomerKey, DateKey, RegionKey, measures)
    - FactFunnel (FunnelKey PK, VisitorID, StockCode, ProductKey, funnel metrics)
    """
    # 1. DimRegion
    unique_countries = sorted(cleaned_sales["Country"].dropna().unique().tolist())
    if orders_df is not None and "state" in orders_df.columns:
        if "Brazil" not in unique_countries:
            unique_countries.append("Brazil")
        unique_countries = sorted(unique_countries)

    dim_region = pd.DataFrame({"Country": unique_countries})
    dim_region["RegionKey"] = dim_region.index + 1
    dim_region = dim_region[DIM_REGION_COLUMNS]

    # 2. DimProduct
    dim_product = cleaned_sales[["StockCode", "Description"]].drop_duplicates(subset=["StockCode"]).copy()
    dim_product = dim_product.sort_values("StockCode").reset_index(drop=True)
    dim_product["ProductKey"] = dim_product.index + 1
    avg_price = cleaned_sales.groupby("StockCode")["UnitPrice"].mean().reset_index()
    dim_product = dim_product.merge(avg_price, on="StockCode", how="left")
    dim_product["UnitPrice"] = np.round(dim_product["UnitPrice"], 4)
    dim_product = dim_product[DIM_PRODUCT_COLUMNS]

    # 3. DimCustomer (Enforce referential integrity to DimRegion)
    dim_customer = cleaned_sales[["CustomerID", "Country"]].drop_duplicates(subset=["CustomerID"]).copy()
    dim_customer = dim_customer.sort_values("CustomerID").reset_index(drop=True)
    dim_customer["CustomerKey"] = dim_customer.index + 1
    dim_customer = dim_customer.merge(dim_region, on="Country", how="left")
    dim_customer["RegionKey"] = dim_customer["RegionKey"].fillna(-1).astype(int)
    dim_customer = dim_customer[DIM_CUSTOMER_COLUMNS]

    # 4. DimDate
    unique_dates = pd.to_datetime(cleaned_sales["Date"].unique())
    dim_date = pd.DataFrame({"Date": unique_dates}).sort_values("Date").reset_index(drop=True)
    dim_date["DateKey"] = dim_date["Date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["FullDate"] = dim_date["Date"].dt.date
    dim_date["Year"] = dim_date["Date"].dt.year
    dim_date["Month"] = dim_date["Date"].dt.month
    dim_date["Day"] = dim_date["Date"].dt.day
    dim_date["DayOfWeek"] = (
        dim_date["Date"].dt.day_of_week + 1
        if hasattr(dim_date["Date"].dt, "day_of_week")
        else dim_date["Date"].dt.dayofweek + 1
    )
    dim_date["Quarter"] = dim_date["Date"].dt.quarter
    dim_date = dim_date[DIM_DATE_COLUMNS]

    # 5. FactSales (Decouple RegionKey from DimCustomer: join DimRegion via Country)
    fact_sales = (
        cleaned_sales.merge(
            dim_product[["StockCode", "ProductKey"]], on="StockCode", how="left"
        )
        .merge(
            dim_customer[["CustomerID", "CustomerKey"]], on="CustomerID", how="left"
        )
        .merge(
            dim_region[["Country", "RegionKey"]], on="Country", how="left"
        )
    )
    fact_sales["RegionKey"] = fact_sales["RegionKey"].fillna(-1).astype(int)
    fact_sales["DateKey"] = pd.to_datetime(fact_sales["Date"]).dt.strftime("%Y%m%d").astype(int)

    # Distributed-safe deterministic surrogate SalesKey (SHA256)
    timestamps = fact_sales["InvoiceTimestamp"].astype(str)
    inv_nos = fact_sales["InvoiceNo"].astype(str)
    stocks = fact_sales["StockCode"].astype(str)
    fact_sales["SalesKey"] = [
        hashlib.sha256(f"{inv}_{stk}_{ts}".encode("utf-8")).hexdigest()[:16]
        for inv, stk, ts in zip(inv_nos, stocks, timestamps)
    ]

    fact_sales = fact_sales.sort_values("InvoiceTimestamp").reset_index(drop=True)
    fact_sales = fact_sales[FACT_SALES_COLUMNS]

    # 6. FactFunnel (Behavioral funnel modeling)
    fact_funnel = build_fact_funnel_pandas(events_df, dim_product)

    return StarSchemaContainer(
        fact_sales=fact_sales,
        fact_funnel=fact_funnel,
        dim_product=dim_product,
        dim_customer=dim_customer,
        dim_date=dim_date,
        dim_region=dim_region,
    )


class PandasETLEngine(BaseETLEngine):
    """End-to-end Pandas/PyArrow implementation of the GlobalShop pipeline."""

    def extract(self) -> Dict[str, Any]:
        logger.info("Extracting raw CSV datasets from: %s", self.input_dir)
        retail_file = self.input_dir / "online_retail_ii.csv"
        orders_file = self.input_dir / "olist_orders.csv"
        events_file = self.input_dir / "retailrocket_events.csv"

        if not retail_file.exists():
            sample_file = self.input_dir / "sample_1k_online_retail.csv"
            if sample_file.exists():
                logger.info("Main dataset not found, using sample file: %s", sample_file)
                retail_file = sample_file
            else:
                raise FileNotFoundError(
                    f"Input retail file not found at {retail_file}. Run data/generate_synthetic_data.py first."
                )

        raw_retail = pd.read_csv(retail_file)
        raw_orders = pd.read_csv(orders_file) if orders_file.exists() else None
        raw_events = pd.read_csv(events_file) if events_file.exists() else None

        return {
            "raw_retail": raw_retail,
            "raw_orders": raw_orders,
            "raw_events": raw_events,
        }

    def transform(self, raw_data: Dict[str, Any]) -> StarSchemaContainer[pd.DataFrame]:
        logger.info("Cleaning raw retail records and validating data quality...")
        cleaned_retail, clean_metrics = clean_online_retail_pandas(raw_data["raw_retail"])

        logger.info("Constructing Kimball Star Schema and FactFunnel...")
        star_schema = build_star_schema_pandas(
            cleaned_sales=cleaned_retail,
            events_df=raw_data["raw_events"],
            orders_df=raw_data["raw_orders"],
        )

        star_schema.clean_metrics = clean_metrics
        star_schema.cleaned_sales = cleaned_retail

        # Clean auxiliary datasets for parity
        if raw_data["raw_orders"] is not None:
            orders = raw_data["raw_orders"].copy()
            orders["order_purchase_timestamp"] = pd.to_datetime(
                orders["order_purchase_timestamp"], errors="coerce"
            )
            star_schema.orders_clean = orders

        if raw_data["raw_events"] is not None:
            events = raw_data["raw_events"].copy()
            events["timestamp"] = pd.to_datetime(events["timestamp"], errors="coerce")
            star_schema.events_clean = events

        return star_schema

    def load(self, star_schema: StarSchemaContainer[pd.DataFrame]) -> Dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        fact_sales = star_schema.fact_sales
        fact_funnel = star_schema.fact_funnel

        # Persist FactSales (Partitioned Lakehouse & Flat) using Atomic Directory Swap
        fact_out = self.output_dir / "fact_sales"
        staging_out = self.output_dir / ".staging_fact_sales"
        backup_out = self.output_dir / ".backup_fact_sales"

        if staging_out.exists():
            shutil.rmtree(staging_out)
        staging_out.mkdir(parents=True, exist_ok=True)

        logger.info("Persisting FactSales partitioned by (Year, Month) via atomic staging to %s", fact_out)
        fact_sales.to_parquet(self.output_dir / "fact_sales.parquet", index=False)
        fact_sales.to_parquet(staging_out, partition_cols=["Year", "Month"], index=False)

        # Atomic directory swap: guarantees zero data loss if process crashes mid-write
        if fact_out.exists():
            if backup_out.exists():
                shutil.rmtree(backup_out)
            fact_out.rename(backup_out)
            staging_out.rename(fact_out)
            shutil.rmtree(backup_out)
        else:
            staging_out.rename(fact_out)

        # Persist FactFunnel
        funnel_file = self.output_dir / "fact_funnel.parquet"
        if fact_funnel is not None:
            fact_funnel.to_parquet(funnel_file, index=False)
            logger.info("Persisted FactFunnel (%d rows) to %s", len(fact_funnel), funnel_file)

        # Persist Dimension Tables
        dim_mappings = {
            "DimProduct": (star_schema.dim_product, "dim_product.parquet"),
            "DimCustomer": (star_schema.dim_customer, "dim_customer.parquet"),
            "DimDate": (star_schema.dim_date, "dim_date.parquet"),
            "DimRegion": (star_schema.dim_region, "dim_region.parquet"),
        }
        for dim_name, (dim_df, filename) in dim_mappings.items():
            dim_file = self.output_dir / filename
            dim_df.to_parquet(dim_file, index=False)
            logger.info("Persisted %s (%d rows) to %s", dim_name, len(dim_df), dim_file)

        # Cleaned flat datasets
        if star_schema.cleaned_sales is not None:
            star_schema.cleaned_sales.to_parquet(
                self.output_dir / "online_retail_clean.parquet", index=False
            )
        if star_schema.orders_clean is not None:
            star_schema.orders_clean.to_parquet(
                self.output_dir / "orders_clean.parquet", index=False
            )
        if star_schema.events_clean is not None:
            star_schema.events_clean.to_parquet(
                self.output_dir / "events_clean.parquet", index=False
            )

        # Audit Summary
        clean_metrics = star_schema.clean_metrics
        funnel_rows = len(fact_funnel) if fact_funnel is not None else 0
        summary = {
            "pipeline_completed_at": datetime.now(timezone.utc).isoformat(),
            "engine": "pandas",
            "clean_metrics": clean_metrics,
            "star_schema_counts": {
                "fact_sales_rows": len(fact_sales),
                "fact_funnel_rows": funnel_rows,
                "dim_product_rows": len(star_schema.dim_product),
                "dim_customer_rows": len(star_schema.dim_customer),
                "dim_date_rows": len(star_schema.dim_date),
                "dim_region_rows": len(star_schema.dim_region),
            },
            "retention_rate_pct": round(
                (len(fact_sales) / max(1, clean_metrics["raw_records"])) * 100, 2
            ),
        }

        summary_file = self.output_dir / "summary.json"
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        logger.info(
            "ETL Pipeline successfully finished. Retention Rate: %.2f%%. Summary in %s",
            summary["retention_rate_pct"],
            summary_file,
        )
        return summary
