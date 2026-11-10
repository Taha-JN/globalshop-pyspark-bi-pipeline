"""
etl/schemas.py

Schema contracts, table column definitions, and integrity specifications
for the GlobalShop Kimball Star Schema Lakehouse.
"""
from dataclasses import dataclass, field
from typing import Any, Dict, Generic, List, Optional, TypeVar

T = TypeVar("T")


@dataclass
class StarSchemaContainer(Generic[T]):
    """
    Standardized container for Star Schema entities produced by ETL transform engines.
    Guarantees Liskov Substitution Principle (LSP) between Pandas and Spark engines.
    """
    fact_sales: T
    fact_funnel: Optional[T]
    dim_product: T
    dim_customer: T
    dim_date: T
    dim_region: T
    clean_metrics: Dict[str, Any] = field(default_factory=dict)
    cleaned_sales: Optional[T] = None
    orders_clean: Optional[T] = None
    events_clean: Optional[T] = None

    def __getitem__(self, key: str) -> Any:
        mapping = {
            "FactSales": self.fact_sales,
            "fact_sales": self.fact_sales,
            "FactFunnel": self.fact_funnel,
            "fact_funnel": self.fact_funnel,
            "DimProduct": self.dim_product,
            "dim_product": self.dim_product,
            "DimCustomer": self.dim_customer,
            "dim_customer": self.dim_customer,
            "DimDate": self.dim_date,
            "dim_date": self.dim_date,
            "DimRegion": self.dim_region,
            "dim_region": self.dim_region,
            "clean_metrics": self.clean_metrics,
            "metrics": self.clean_metrics,
            "online_retail_clean": self.cleaned_sales,
            "cleaned_sales": self.cleaned_sales,
            "orders_clean": self.orders_clean,
            "events_clean": self.events_clean,
        }
        if key in mapping:
            return mapping[key]
        raise KeyError(f"Key '{key}' not found in StarSchemaContainer")

    def __contains__(self, key: str) -> bool:
        return key in [
            "FactSales", "fact_sales",
            "FactFunnel", "fact_funnel",
            "DimProduct", "dim_product",
            "DimCustomer", "dim_customer",
            "DimDate", "dim_date",
            "DimRegion", "dim_region",
            "clean_metrics", "metrics",
            "online_retail_clean", "cleaned_sales",
            "orders_clean", "events_clean"
        ]

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default


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
