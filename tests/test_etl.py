"""
test_etl.py

Comprehensive test suite for GlobalShop Big Data & Analytics Pipeline.
Validates data cleaning, Kimball Star Schema integrity, DimRegion foreign key linking,
FactFunnel clickstream integration, distributed surrogate keys, and DuckDB OLAP queries.
"""
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import pytest

from etl.engines.pandas_engine import (
    build_fact_funnel_pandas,
    build_star_schema_pandas,
    clean_online_retail_pandas,
)
from etl.schemas import (
    DIM_CUSTOMER_COLUMNS,
    DIM_DATE_COLUMNS,
    DIM_PRODUCT_COLUMNS,
    DIM_REGION_COLUMNS,
    FACT_FUNNEL_COLUMNS,
    FACT_SALES_COLUMNS,
)


@pytest.fixture
def sample_raw_retail_df():
    """Provides a synthetic test dataframe with intentional anomalies."""
    return pd.DataFrame({
        "InvoiceNo": ["INV1001", "INV1002", "INV1003", "INV1004", "INV1005", "INV1006"],
        "StockCode": ["P001", "P002", "P001", "P003", "P004", "P005"],
        "Description": ["Mug", "Plate", "Mug", "Bowl", "Fork", "Knife"],
        "Quantity": [5, -2, 0, 10, 3, 2],
        "InvoiceDate": [
            "2023-05-15 14:30:00",
            "2023-05-15 15:00:00",
            "2023-05-16 10:15:00",
            "2023-06-20 18:45:00",
            "2023-07-01 09:00:00",
            "2023-08-10 12:00:00",
        ],
        "UnitPrice": [12.50, 20.00, 15.00, 5.00, 0.0, 8.50],
        "CustomerID": ["1001", "1002", "1001", "", None, "1005"],
        "Country": ["United Kingdom", "Germany", "United Kingdom", "France", "United Kingdom", "United States"],
    })


@pytest.fixture
def sample_events_df():
    """Provides synthetic clickstream event logs."""
    return pd.DataFrame({
        "timestamp": [
            "2023-05-15 14:00:00",
            "2023-05-15 14:10:00",
            "2023-05-15 14:20:00",
            "2023-06-20 18:00:00",
            "2023-08-10 11:30:00",
        ],
        "visitorid": ["V100", "V100", "V100", "V200", "V300"],
        "itemid": ["P001", "P001", "P001", "P003", "P005"],
        "event_type": ["view", "addtocart", "transaction", "view", "view"],
        "transactionid": ["INV1001", "", "INV1001", "", ""],
    })


def test_negative_and_zero_quantity_removal(sample_raw_retail_df):
    """Ensure negative returns and zero quantities are filtered out."""
    cleaned, metrics = clean_online_retail_pandas(sample_raw_retail_df)
    assert (cleaned["Quantity"] > 0).all(), "All cleaned records must have positive Quantity"
    assert metrics["negative_or_zero_qty_dropped"] == 2
    assert "INV1002" not in cleaned["InvoiceNo"].values
    assert "INV1003" not in cleaned["InvoiceNo"].values


def test_zero_unit_price_removal(sample_raw_retail_df):
    """Ensure non-positive prices (promotions/errors) are dropped."""
    cleaned, metrics = clean_online_retail_pandas(sample_raw_retail_df)
    assert (cleaned["UnitPrice"] > 0).all(), "All cleaned records must have positive UnitPrice"
    assert "INV1005" not in cleaned["InvoiceNo"].values


def test_missing_customer_imputation(sample_raw_retail_df):
    """Ensure empty or null CustomerIDs are gracefully imputed to 'GUEST'."""
    cleaned, metrics = clean_online_retail_pandas(sample_raw_retail_df)
    assert not cleaned["CustomerID"].isin(["", "nan", "None"]).any()
    inv_1004 = cleaned[cleaned["InvoiceNo"] == "INV1004"]
    assert inv_1004["CustomerID"].values[0] == "GUEST"


def test_dim_date_generation(sample_raw_retail_df):
    """Validate that DimDate derives proper integer DateKey and dimensional attributes."""
    cleaned, _ = clean_online_retail_pandas(sample_raw_retail_df)
    star_schema = build_star_schema_pandas(cleaned)
    dim_date = star_schema["DimDate"]

    expected_cols = set(DIM_DATE_COLUMNS)
    assert expected_cols.issubset(set(dim_date.columns))
    assert dim_date["DateKey"].dtype in ["int32", "int64"]
    assert (dim_date["DateKey"] > 20200000).all()


def test_star_schema_referential_integrity(sample_raw_retail_df, sample_events_df):
    """Validate FactSales foreign keys and referential integrity to all dimensions."""
    cleaned, _ = clean_online_retail_pandas(sample_raw_retail_df)
    star_schema = build_star_schema_pandas(cleaned, events_df=sample_events_df)

    fact_sales = star_schema["FactSales"]
    dim_product = star_schema["DimProduct"]
    dim_customer = star_schema["DimCustomer"]
    dim_date = star_schema["DimDate"]
    dim_region = star_schema["DimRegion"]

    # Validate Schema Columns
    assert list(fact_sales.columns) == FACT_SALES_COLUMNS
    assert list(dim_product.columns) == DIM_PRODUCT_COLUMNS
    assert list(dim_customer.columns) == DIM_CUSTOMER_COLUMNS
    assert list(dim_region.columns) == DIM_REGION_COLUMNS

    # Validate Foreign Keys
    assert fact_sales["ProductKey"].isin(dim_product["ProductKey"]).all()
    assert fact_sales["CustomerKey"].isin(dim_customer["CustomerKey"]).all()
    assert fact_sales["DateKey"].isin(dim_date["DateKey"]).all()
    assert fact_sales["RegionKey"].isin(dim_region["RegionKey"]).all()
    assert dim_customer["RegionKey"].isin(dim_region["RegionKey"]).all()

    # Validate Distributed-Safe Hash Surrogate Key
    assert fact_sales["SalesKey"].str.len().eq(16).all()
    assert fact_sales["SalesKey"].is_unique

    # Validate Metric Calculations
    expected_total = fact_sales["Quantity"] * fact_sales["UnitPrice"]
    assert np.allclose(fact_sales["TotalPrice"], expected_total, atol=1e-4)


def test_fact_funnel_generation(sample_raw_retail_df, sample_events_df):
    """Validate FactFunnel behavioral clickstream pivoting and product linking."""
    cleaned, _ = clean_online_retail_pandas(sample_raw_retail_df)
    star_schema = build_star_schema_pandas(cleaned, events_df=sample_events_df)
    fact_funnel = star_schema["FactFunnel"]

    assert list(fact_funnel.columns) == FACT_FUNNEL_COLUMNS
    assert len(fact_funnel) == 3  # V100-P001, V200-P003, V300-P005

    # Check V100 conversion
    v100_p001 = fact_funnel[(fact_funnel["VisitorID"] == "V100") & (fact_funnel["StockCode"] == "P001")]
    assert len(v100_p001) == 1
    row = v100_p001.iloc[0]
    assert row["Views"] == 1
    assert row["AddToCart"] == 1
    assert row["Transactions"] == 1
    assert row["Converted"] is True or row["Converted"] == 1

    # Check V200 non-conversion
    v200 = fact_funnel[fact_funnel["VisitorID"] == "V200"].iloc[0]
    assert v200["Views"] == 1
    assert v200["AddToCart"] == 0
    assert v200["Transactions"] == 0
    assert v200["Converted"] is False or v200["Converted"] == 0


def test_duckdb_olap_query_execution(tmp_path, sample_raw_retail_df, sample_events_df):
    """Validate that DuckDB executes relational queries on generated Parquet tables."""
    cleaned, _ = clean_online_retail_pandas(sample_raw_retail_df)
    star_schema = build_star_schema_pandas(cleaned, events_df=sample_events_df)

    # Persist mock Parquet tables
    fact_p = tmp_path / "fact_sales.parquet"
    prod_p = tmp_path / "dim_product.parquet"
    cust_p = tmp_path / "dim_customer.parquet"
    reg_p = tmp_path / "dim_region.parquet"

    star_schema["FactSales"].to_parquet(fact_p, index=False)
    star_schema["DimProduct"].to_parquet(prod_p, index=False)
    star_schema["DimCustomer"].to_parquet(cust_p, index=False)
    star_schema["DimRegion"].to_parquet(reg_p, index=False)

    fact_str = fact_p.as_posix()
    prod_str = prod_p.as_posix()
    reg_str = reg_p.as_posix()

    con = duckdb.connect()
    con.execute(f"CREATE VIEW FactSales AS SELECT * FROM read_parquet('{fact_str}')")
    con.execute(f"CREATE VIEW DimProduct AS SELECT * FROM read_parquet('{prod_str}')")
    con.execute(f"CREATE VIEW DimRegion AS SELECT * FROM read_parquet('{reg_str}')")

    res = con.execute("""
        SELECT r.Country, SUM(f.TotalPrice) AS rev
        FROM FactSales f
        JOIN DimRegion r ON f.RegionKey = r.RegionKey
        GROUP BY r.Country
        ORDER BY rev DESC
    """).df()

    assert not res.empty
    assert "rev" in res.columns
    assert "Country" in res.columns
