"""
analysis.py

Authentic OLAP Query Execution Engine & Executive BI Asset Generator.
Uses DuckDB to execute analytical SQL queries in `sql/queries/` directly
against the Kimball Star Schema Parquet Lakehouse (`FactSales`, `DimProduct`,
`DimCustomer`, `DimDate`, `DimRegion`, `FactFunnel`).

Outputs authentic query CSV exports and high-resolution BI visual assets:
- monthly_sales.png (continuous time-series with zero-fill)
- top_products.png
- sales_by_region.png
- customer_segments.png (vectorized RFM segmentation)
- kpi_card.png (executive summary KPIs including funnel conversion rate)

Usage:
    python analysis/analysis.py --input etl/output --output analysis/output
"""
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_DIR = PROJECT_ROOT / "etl" / "output"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "analysis" / "output"
SQL_QUERIES_DIR = PROJECT_ROOT / "sql" / "queries"
SQL_OUTPUTS_DIR = SQL_QUERIES_DIR / "outputs"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("GlobalShop_Analysis")
sns.set_theme(style="whitegrid")


def initialize_duckdb_lakehouse(input_dir: Path) -> duckdb.DuckDBPyConnection:
    """
    Initialize an in-memory DuckDB OLAP engine and register virtual views
    pointing directly to Kimball Star Schema Parquet tables.
    """
    con = duckdb.connect(database=":memory:")

    fact_sales_path = input_dir / "fact_sales.parquet"
    partitioned_dir = input_dir / "fact_sales"

    # Prioritize partitioned lakehouse directory if present
    if partitioned_dir.exists() and any(partitioned_dir.glob("**/*.parquet")):
        # Read partitioned lakehouse with glob pattern
        glob_path = str(partitioned_dir / "**" / "*.parquet").replace("\\", "/")
        con.execute(f"CREATE OR REPLACE VIEW FactSales AS SELECT * FROM read_parquet('{glob_path}')")
        logger.info("Registered FactSales view from partitioned lakehouse: %s", glob_path)
    elif fact_sales_path.exists():
        p = str(fact_sales_path).replace("\\", "/")
        con.execute(f"CREATE OR REPLACE VIEW FactSales AS SELECT * FROM read_parquet('{p}')")
        logger.info("Registered FactSales view from monolithic parquet: %s", p)
    else:
        raise FileNotFoundError(
            f"FactSales parquet files not found in {input_dir}. Please run etl/main.py first."
        )

    # Register Dimension views
    dims = {
        "DimProduct": "dim_product.parquet",
        "DimCustomer": "dim_customer.parquet",
        "DimDate": "dim_date.parquet",
        "DimRegion": "dim_region.parquet",
    }
    for view_name, file_name in dims.items():
        dim_file = input_dir / file_name
        if dim_file.exists():
            p = str(dim_file).replace("\\", "/")
            con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{p}')")
            logger.info("Registered %s view from %s", view_name, p)

    # Register FactFunnel view
    funnel_file = input_dir / "fact_funnel.parquet"
    if funnel_file.exists():
        p = str(funnel_file).replace("\\", "/")
        con.execute(f"CREATE OR REPLACE VIEW FactFunnel AS SELECT * FROM read_parquet('{p}')")
        logger.info("Registered FactFunnel view from %s", p)

    return con


def execute_sql_query(con: duckdb.DuckDBPyConnection, sql_file: Path) -> pd.DataFrame:
    """Read authentic SQL file and execute via DuckDB OLAP engine."""
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL query file not found: {sql_file}")
    query = sql_file.read_text(encoding="utf-8")
    logger.info("Executing DuckDB OLAP query: %s", sql_file.name)
    df = con.execute(query).df()
    return df


def export_dataframe(df: pd.DataFrame, filename: str, target_dirs: List[Path]):
    """Export query results cleanly to target directories."""
    for out in target_dirs:
        out.mkdir(parents=True, exist_ok=True)
        csv_path = out / filename
        df.to_csv(csv_path, index=False)
        logger.info("Exported authentic query CSV: %s (%d rows)", csv_path, len(df))


def process_q1_top_products(con: duckdb.DuckDBPyConnection, out_dirs: List[Path]):
    """Execute Q1: Top 10 products by revenue."""
    sql_file = SQL_QUERIES_DIR / "q1_top_products_by_revenue.sql"
    df = execute_sql_query(con, sql_file)
    export_dataframe(df, "q1_top_products_by_revenue.csv", out_dirs)

    # High-resolution chart
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(
        data=df,
        x="revenue",
        y="Description",
        hue="Description",
        palette="viridis",
        legend=False,
        ax=ax,
    )
    ax.set_title("Top 10 Products by Total Revenue (DuckDB OLAP)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Revenue ($)", fontsize=12)
    ax.set_ylabel("Product Description", fontsize=12)
    plt.tight_layout()
    chart_path = out_dirs[0] / "top_products.png"
    fig.savefig(chart_path, dpi=300)
    plt.close(fig)
    logger.info("Saved visual asset: %s", chart_path)


def process_q2_monthly_trend(con: duckdb.DuckDBPyConnection, out_dirs: List[Path]):
    """
    Execute Q2: Monthly sales trend with YoY calculations.
    Vectorized reindexing guarantees continuous calendar months with zero gaps.
    """
    sql_file = SQL_QUERIES_DIR / "q2_monthly_sales_trend.sql"
    df = execute_sql_query(con, sql_file)
    df["month"] = pd.to_datetime(df["month"])

    # Vectorized continuous calendar reindexing to prevent shift(12) index-skipping bugs
    if not df.empty:
        all_months = pd.date_range(
            start=df["month"].min(), end=df["month"].max(), freq="MS"
        )
        reindexed = (
            df.set_index("month")
            .reindex(all_months)
            .rename_axis("month")
            .reset_index()
        )
        reindexed["monthly_revenue"] = reindexed["monthly_revenue"].fillna(0.0)
        reindexed["prior_year_revenue"] = reindexed["monthly_revenue"].shift(12)
        reindexed["yoy_growth_pct"] = np.round(
            (reindexed["monthly_revenue"] - reindexed["prior_year_revenue"])
            / reindexed["prior_year_revenue"].replace(0, np.nan)
            * 100,
            2,
        )
        export_df = reindexed
    else:
        export_df = df

    export_dataframe(export_df, "q2_monthly_sales_trend.csv", out_dirs)

    # Time-series line chart
    fig, ax = plt.subplots(figsize=(11, 5))
    sns.lineplot(
        data=export_df,
        x="month",
        y="monthly_revenue",
        marker="o",
        color="#1f77b4",
        linewidth=2.5,
        ax=ax,
    )
    ax.set_title("Monthly Sales Revenue Trend (DuckDB OLAP)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Month", fontsize=12)
    ax.set_ylabel("Revenue ($)", fontsize=12)
    plt.tight_layout()
    chart_path = out_dirs[0] / "monthly_sales.png"
    fig.savefig(chart_path, dpi=300)
    plt.close(fig)
    logger.info("Saved visual asset: %s", chart_path)


def process_q3_customer_segmentation(con: duckdb.DuckDBPyConnection, out_dirs: List[Path]):
    """Execute Q3: RFM customer segmentation via DuckDB."""
    sql_file = SQL_QUERIES_DIR / "q3_customer_segmentation.sql"
    top_df = execute_sql_query(con, sql_file)
    export_dataframe(top_df, "q3_customer_segmentation.csv", out_dirs)

    # Full distribution query for visualization
    full_seg_query = """
    WITH customer_metrics AS (
      SELECT
        c.CustomerID,
        COUNT(DISTINCT f.InvoiceNo) AS orders_count,
        SUM(f.TotalPrice) AS total_spend
      FROM FactSales f
      JOIN DimCustomer c ON f.CustomerKey = c.CustomerKey
      WHERE c.CustomerID IS NOT NULL AND c.CustomerID != 'GUEST'
      GROUP BY c.CustomerID
    )
    SELECT
      CASE
        WHEN total_spend >= 500 AND orders_count >= 5 THEN 'High-Value VIP'
        WHEN total_spend >= 200 THEN 'Loyal Customer'
        WHEN orders_count > 1 THEN 'Returning'
        ELSE 'First-Time / Occasional'
      END AS segment,
      COUNT(*) AS count
    FROM customer_metrics
    GROUP BY segment
    ORDER BY count DESC;
    """
    seg_counts = con.execute(full_seg_query).df()

    if not seg_counts.empty:
        fig, ax = plt.subplots(figsize=(7, 7))
        colors = sns.color_palette("Set2", len(seg_counts))
        ax.pie(
            seg_counts["count"],
            labels=seg_counts["segment"],
            autopct="%1.1f%%",
            startangle=140,
            colors=colors,
        )
        ax.set_title("Customer Segmentation Distribution (DuckDB RFM)", fontsize=14, fontweight="bold")
        plt.tight_layout()
        chart_path = out_dirs[0] / "customer_segments.png"
        fig.savefig(chart_path, dpi=300)
        plt.close(fig)
        logger.info("Saved visual asset: %s", chart_path)


def process_q4_peak_hours(con: duckdb.DuckDBPyConnection, out_dirs: List[Path]):
    """Execute Q4: Peak transaction hours."""
    sql_file = SQL_QUERIES_DIR / "q4_peak_hours.sql"
    df = execute_sql_query(con, sql_file)
    export_dataframe(df, "q4_peak_hours.csv", out_dirs)


def process_q5_country_profitability(con: duckdb.DuckDBPyConnection, out_dirs: List[Path]):
    """Execute Q5: Country-level sales and profitability via DimRegion."""
    sql_file = SQL_QUERIES_DIR / "q5_country_profitability.sql"
    df = execute_sql_query(con, sql_file)
    export_dataframe(df, "q5_country_profitability.csv", out_dirs)

    # Bar chart
    fig, ax = plt.subplots(figsize=(8, 6))
    sns.barplot(
        data=df,
        x="revenue",
        y="Country",
        hue="Country",
        palette="Blues_r",
        legend=False,
        ax=ax,
    )
    ax.set_title("Total Revenue by Country (DimRegion Join)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Revenue ($)", fontsize=12)
    ax.set_ylabel("Country", fontsize=12)
    plt.tight_layout()
    chart_path = out_dirs[0] / "sales_by_region.png"
    fig.savefig(chart_path, dpi=300)
    plt.close(fig)
    logger.info("Saved visual asset: %s", chart_path)


def generate_executive_kpis(con: duckdb.DuckDBPyConnection, output_dir: Path):
    """Generate executive KPI summary visual using authentic DuckDB aggregations."""
    kpi_query = """
    SELECT
      COALESCE(SUM(TotalPrice), 0.0) AS total_revenue,
      COALESCE(COUNT(DISTINCT InvoiceNo), 0) AS total_orders,
      COALESCE(SUM(TotalPrice) / NULLIF(COUNT(DISTINCT InvoiceNo), 0), 0.0) AS avg_order_value,
      COALESCE(SUM(Quantity), 0) AS units_sold
    FROM FactSales;
    """
    kpi_df = con.execute(kpi_query).df()
    row = kpi_df.iloc[0]

    # Check for FactFunnel
    funnel_kpi = ""
    try:
        funnel_res = con.execute(
            """
            SELECT
              ROUND(COUNT(CASE WHEN Converted THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS conv_rate
            FROM FactFunnel
            """
        ).df()
        if not funnel_res.empty and pd.notna(funnel_res.iloc[0]["conv_rate"]):
            rate = funnel_res.iloc[0]["conv_rate"]
            funnel_kpi = f"\nFunnel Conv: {rate:.1f}%"
    except Exception:
        pass

    fig, ax = plt.subplots(figsize=(10, 3.2))
    ax.axis("off")

    cards = [
        f"Total Revenue\n${row['total_revenue']:,.2f}",
        f"Total Orders\n{int(row['total_orders']):,}",
        f"Avg Order Value\n${row['avg_order_value']:,.2f}",
        f"Units Sold\n{int(row['units_sold']):,}{funnel_kpi}",
    ]

    for i, text in enumerate(cards):
        x_pos = 0.04 + (i * 0.24)
        ax.text(
            x_pos,
            0.45,
            text,
            fontsize=12,
            fontweight="bold",
            verticalalignment="center",
            bbox=dict(
                boxstyle="round,pad=0.6",
                facecolor="#f0f2f6",
                edgecolor="#0969da",
                linewidth=1.2,
            ),
        )

    ax.set_title("GlobalShop Executive Performance KPIs (DuckDB OLAP)", fontsize=13, fontweight="bold", pad=15)
    plt.tight_layout()
    chart_path = output_dir / "kpi_card.png"
    fig.savefig(chart_path, dpi=300)
    plt.close(fig)
    logger.info("Saved visual asset: %s", chart_path)


def main():
    parser = argparse.ArgumentParser(
        description="GlobalShop DuckDB OLAP Analytical Engine and BI Asset Generator"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=str(DEFAULT_INPUT_DIR),
        help="ETL output directory containing Parquet tables",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(DEFAULT_OUTPUT_DIR),
        help="Target directory for generated BI charts and reports",
    )
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    SQL_OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    out_dirs = [output_dir, SQL_OUTPUTS_DIR]

    logger.info("Connecting to DuckDB OLAP engine for input: %s", input_dir)
    con = initialize_duckdb_lakehouse(input_dir)

    process_q1_top_products(con, out_dirs)
    process_q2_monthly_trend(con, out_dirs)
    process_q3_customer_segmentation(con, out_dirs)
    process_q4_peak_hours(con, out_dirs)
    process_q5_country_profitability(con, out_dirs)
    generate_executive_kpis(con, output_dir)

    logger.info("All analytical queries and visual assets successfully generated via DuckDB.")


if __name__ == "__main__":
    main()
