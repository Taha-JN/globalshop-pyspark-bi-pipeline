"""
etl/schemas.py

Schema contracts, table column definitions, and integrity specifications
for the GlobalShop Kimball Star Schema Lakehouse.
"""
from typing import Dict, List

# Star Schema Table Column Specifications
FACT_SALES_COLUMNS: List[str] = [
    "SalesKey",
    "InvoiceNo",
    "ProductKey",
    "CustomerKey",
    "DateKey",
    "RegionKey",
    "InvoiceTimestamp",
    "Year",
    "Month",
    "Quantity",
    "UnitPrice",
    "TotalPrice",
    "Channel",
]

FACT_FUNNEL_COLUMNS: List[str] = [
    "FunnelKey",
    "VisitorID",
    "StockCode",
    "ProductKey",
    "Views",
    "AddToCart",
    "Transactions",
    "Converted",
]

DIM_PRODUCT_COLUMNS: List[str] = [
    "ProductKey",
    "StockCode",
    "Description",
    "UnitPrice",
]

DIM_CUSTOMER_COLUMNS: List[str] = [
    "CustomerKey",
    "CustomerID",
    "Country",
    "RegionKey",
]

DIM_DATE_COLUMNS: List[str] = [
    "DateKey",
    "FullDate",
    "Year",
    "Month",
    "Day",
    "DayOfWeek",
    "Quarter",
]

DIM_REGION_COLUMNS: List[str] = [
    "RegionKey",
    "Country",
]

# Data Quality Thresholds & Constants
UNKNOWN_CUSTOMER_ID: str = "GUEST"
DEFAULT_GROSS_MARGIN: float = 0.40
VALID_EVENT_TYPES: List[str] = ["view", "addtocart", "transaction"]
