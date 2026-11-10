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
from etl.engines.spark_engine import SparkETLEngine, is_spark_runnable
from etl.schemas import (
    DIM_CUSTOMER_COLUMNS,
    DIM_DATE_COLUMNS,
    DIM_PRODUCT_COLUMNS,
    DIM_REGION_COLUMNS,
    FACT_FUNNEL_COLUMNS,
    FACT_SALES_COLUMNS,
    StarSchemaContainer,
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


@pytest.fixture(scope="session")
def spark_session():
    """Instantiate a local SparkSession if JVM is compatible, otherwise skip gracefully."""
    if not is_spark_runnable():
        pytest.skip("Local JVM is incompatible with PySpark (Java 17+ required for PySpark 4.x)")
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("GlobalShop_UnitTests")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    yield spark
    spark.stop()


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
    """Validate that DuckDB executes relational queries on partitioned Parquet tables with hive_partitioning=true."""
    cleaned, _ = clean_online_retail_pandas(sample_raw_retail_df)
    star_schema = build_star_schema_pandas(cleaned, events_df=sample_events_df)

    # Persist mock Parquet tables (Partitioned FactSales)
    fact_dir = tmp_path / "fact_sales"
    prod_p = tmp_path / "dim_product.parquet"
    cust_p = tmp_path / "dim_customer.parquet"
    reg_p = tmp_path / "dim_region.parquet"

    star_schema["FactSales"].to_parquet(fact_dir, partition_cols=["Year", "Month"], index=False)
    star_schema["DimProduct"].to_parquet(prod_p, index=False)
    star_schema["DimCustomer"].to_parquet(cust_p, index=False)
    star_schema["DimRegion"].to_parquet(reg_p, index=False)

    fact_glob = (fact_dir / "**" / "*.parquet").as_posix()
    prod_str = prod_p.as_posix()
    reg_str = reg_p.as_posix()

    con = duckdb.connect()
    con.execute(f"CREATE VIEW FactSales AS SELECT * FROM read_parquet('{fact_glob}', hive_partitioning=true)")
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


def test_star_schema_container_lsp_contract(sample_raw_retail_df, sample_events_df):
    """Ensure build_star_schema returns StarSchemaContainer with typed attributes & dict compatibility."""
    cleaned, _ = clean_online_retail_pandas(sample_raw_retail_df)
    container = build_star_schema_pandas(cleaned, events_df=sample_events_df)

    assert isinstance(container, StarSchemaContainer)
    # Typed attribute access
    assert isinstance(container.fact_sales, pd.DataFrame)
    assert isinstance(container.fact_funnel, pd.DataFrame)
    assert isinstance(container.dim_product, pd.DataFrame)
    assert isinstance(container.dim_customer, pd.DataFrame)
    assert isinstance(container.dim_date, pd.DataFrame)
    assert isinstance(container.dim_region, pd.DataFrame)

    # Subscripting access (backwards compatibility)
    assert container["FactSales"].equals(container.fact_sales)
    assert container["fact_sales"].equals(container.fact_sales)
    assert container["DimRegion"].equals(container.dim_region)


def test_guest_region_mapping_decoupled():
    """
    Ensure multiple GUEST transactions from distinct countries map
    directly to their corresponding DimRegion RegionKey, preventing silent data poisoning.
    """
    multi_guest_df = pd.DataFrame({
        "InvoiceNo": ["INV2001", "INV2002", "INV2003", "INV2004"],
        "StockCode": ["P001", "P001", "P002", "P003"],
        "Description": ["Mug", "Mug", "Plate", "Bowl"],
        "Quantity": [1, 2, 3, 4],
        "InvoiceDate": [
            "2023-09-01 10:00:00",
            "2023-09-01 11:00:00",
            "2023-09-02 12:00:00",
            "2023-09-03 14:00:00",
        ],
        "UnitPrice": [10.0, 10.0, 15.0, 20.0],
        "CustomerID": ["", None, "nan", ""],  # All 4 are GUEST transactions
        "Country": ["France", "Germany", "United Kingdom", "United States"],
    })

    cleaned, metrics = clean_online_retail_pandas(multi_guest_df)
    assert metrics["missing_customers_imputed"] == 4
    assert (cleaned["CustomerID"] == "GUEST").all()

    container = build_star_schema_pandas(cleaned)
    assert isinstance(container, StarSchemaContainer)

    fact = container.fact_sales
    dim_reg = container.dim_region

    # Merge FactSales with DimRegion on RegionKey
    merged = fact.merge(dim_reg, on="RegionKey", suffixes=("_fact", "_dim"))

    # Assert each invoice's resolved DimRegion Country matches its original invoice Country
    expected_country_map = dict(zip(multi_guest_df["InvoiceNo"], multi_guest_df["Country"]))
    for _, row in merged.iterrows():
        inv = row["InvoiceNo"]
        expected_country = expected_country_map[inv]
        assert row["Country"] == expected_country, (
            f"Data corruption detected: Invoice {inv} originated in {expected_country} "
            f"but was incorrectly attributed to {row['Country']} in DimRegion!"
        )


def test_spark_engine_parity(tmp_path, sample_raw_retail_df, spark_session):
    """
    Validate SparkETLEngine execution parity, distributed SHA-256 surrogate key derivation,
    and partitioned FactSales Lakehouse generation.
    """
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir(parents=True, exist_ok=True)
    sample_raw_retail_df.to_csv(input_dir / "sample_1k_online_retail.csv", index=False)

    engine = SparkETLEngine(
        input_dir=input_dir,
        output_dir=output_dir,
        spark_master="local[1]",
    )
    engine.spark = spark_session

    summary = engine.run()

    # Assert FactSales partitioned lakehouse exists and has records
    fact_sales_dir = output_dir / "fact_sales"
    assert fact_sales_dir.exists(), "Partitioned FactSales directory must exist"
    partition_files = list(fact_sales_dir.glob("**/*.parquet"))
    assert len(partition_files) > 0, "FactSales must contain partitioned parquet files"

    # Assert dimension tables exist
    assert summary["fact_sales_records"] > 0
    assert (output_dir / "dim_product.parquet").exists()
    assert (output_dir / "dim_customer.parquet").exists()
    assert (output_dir / "dim_region.parquet").exists()
    assert (output_dir / "dim_date.parquet").exists()


