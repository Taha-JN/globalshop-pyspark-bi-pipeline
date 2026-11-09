# SSAS Tabular Model Description

Tables:
- FactSales (SalesKey PK) - measures: Total Sales, Orders, Avg Order Value
- DimProduct (ProductKey PK)
- DimCustomer (CustomerKey PK)
- DimDate (DateKey PK)
- DimRegion (RegionKey PK)

Relationships:
- FactSales[ProductKey] -> DimProduct[ProductKey]
- FactSales[CustomerKey] -> DimCustomer[CustomerKey]
- FactSales[DateKey] -> DimDate[DateKey]
- DimCustomer[Country] -> DimRegion[Country]

Sample DAX measures:

-- Total Sales
Total Sales = SUM(FactSales[TotalPrice])

-- Avg Order Value
AvgOrderValue = DIVIDE([Total Sales], DISTINCTCOUNT(FactSales[InvoiceNo]))

-- Profit Margin (synthetic example)
ProfitMargin = DIVIDE([Total Sales] - SUM(FactSales[UnitPrice] * FactSales[Quantity] * 0.6), [Total Sales])

-- Conversion Rate (events -> transactions)
ConversionRate = DIVIDE(CALCULATE(COUNTROWS(FactSales), FactSales[Channel] = "web"), CALCULATE(COUNTROWS('retailrocket_events'), 'retailrocket_events'[event_type] = "view"))

-- Returning Customer Rate
ReturningCustomerRate = DIVIDE(CALCULATE(DISTINCTCOUNT(FactSales[CustomerKey]), FILTER(FactSales, [orders_count] > 1)), DISTINCTCOUNT(FactSales[CustomerKey]))
