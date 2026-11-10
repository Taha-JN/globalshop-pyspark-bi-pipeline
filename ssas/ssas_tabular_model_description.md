# SSAS Tabular Model Description

## Kimball Star Schema Architecture

### Tables:
- **FactSales** (`SalesKey` PK)
  - Foreign Keys: `ProductKey`, `CustomerKey`, `DateKey`, `RegionKey`
  - Measures: `TotalPrice`, `Quantity`, `UnitPrice`
- **FactFunnel** (`FunnelKey` PK)
  - Foreign Keys: `ProductKey`
  - Attributes: `VisitorID`, `StockCode`, `Views`, `AddToCart`, `Transactions`, `Converted`
- **DimProduct** (`ProductKey` PK)
  - Attributes: `StockCode`, `Description`, `UnitPrice`
- **DimCustomer** (`CustomerKey` PK)
  - Attributes: `CustomerID`, `Country`, `RegionKey`
- **DimDate** (`DateKey` PK)
  - Attributes: `FullDate`, `Year`, `Month`, `Day`, `DayOfWeek`, `Quarter`
- **DimRegion** (`RegionKey` PK)
  - Attributes: `Country`

### Relationships:
- `FactSales[ProductKey]` (Many) -> `DimProduct[ProductKey]` (One)
- `FactSales[CustomerKey]` (Many) -> `DimCustomer[CustomerKey]` (One)
- `FactSales[DateKey]` (Many) -> `DimDate[DateKey]` (One)
- `FactSales[RegionKey]` (Many) -> `DimRegion[RegionKey]` (One)
- `DimCustomer[RegionKey]` (Many) -> `DimRegion[RegionKey]` (One)
- `FactFunnel[ProductKey]` (Many) -> `DimProduct[ProductKey]` (One)

### Production DAX Measures:

```dax
Total Sales = SUM(FactSales[TotalPrice])

AvgOrderValue = DIVIDE([Total Sales], DISTINCTCOUNT(FactSales[InvoiceNo]))

ProfitMargin = DIVIDE([Total Sales] - SUM(FactSales[UnitPrice] * FactSales[Quantity] * 0.6), [Total Sales])

FunnelConversionRate = DIVIDE(SUM(FactFunnel[Transactions]), SUM(FactFunnel[Views]))

VisitorConversionRate = DIVIDE(CALCULATE(COUNTROWS(FactFunnel), FactFunnel[Converted] = TRUE), COUNTROWS(FactFunnel))

ReturningCustomerRate = DIVIDE(
    CALCULATE(
        DISTINCTCOUNT(FactSales[CustomerKey]),
        FILTER(ALL(FactSales), FactSales[CustomerKey] IN VALUES(FactSales[CustomerKey]) AND CALCULATE(COUNTROWS(FactSales)) > 1)
    ),
    DISTINCTCOUNT(FactSales[CustomerKey])
)
```
