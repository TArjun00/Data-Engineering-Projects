# Project 3 — E-Commerce Data Pipeline
## Databricks Community Edition · PySpark · Delta Lake

---

## Overview

An end-to-end data pipeline that processes 300,000+ e-commerce orders across 3 source tables — Orders, Customers and Products — implementing Medallion Architecture (Bronze → Silver → Gold) on Databricks.

---

## Dataset

| Table | Rows | Description |
|-------|------|-------------|
| Orders | 300,000 | Transaction records (2021–2024) |
| Customers | 50,000 | Customer profiles |
| Products | 352 | Product catalogue |

---

## Architecture

```
CSV Files (Databricks Volumes)
        |
        v
Bronze Layer — Read 3 raw CSV files into DataFrames
        |
        v
Silver Layer — Clean each table + Join into master table (Salesdata)
        |
        v
Gold Layer — 4 business-ready aggregated Delta tables
```

---

## Bronze Layer

Read raw CSV files from Databricks Volumes:

```python
df_bronze_orders    = spark.read.option("header","true").option("inferSchema","true").csv("/Volumes/.../orders.csv")
df_bronze_customers = spark.read.option("header","true").option("inferSchema","true").csv("/Volumes/.../customers.csv")
df_bronze_products  = spark.read.option("header","true").option("inferSchema","true").csv("/Volumes/.../products.csv")
```

---

## Silver Layer — Data Cleaning

**Orders Cleaning:**
- Remove duplicates on `OrderID`
- Drop nulls in `OrderID`, `CustomerID`, `ProductID`, `NetAmount`, `OrderDate`
- Standardise `Status` to uppercase
- Filter out Cancelled orders
- Add audit timestamp

**Products Cleaning:**
- Remove duplicates on `ProductID`
- Drop nulls in `ProductID`, `ProductName`, `Category`
- Standardise `Category` and `Brand` to Title Case
- Rename `Status` to `ProductStatus` to avoid column conflict in join
- Filter out Inactive products
- Add audit timestamp

**Customers Cleaning:**
- Remove duplicates on `CustomerID`
- Drop nulls in `CustomerID`, `FirstName`, `LastName`
- Fill nulls — `City` and `CustomerSegment` with `Unknown`
- Standardise text columns to Title Case
- Convert `RegistrationDate` to DateType
- Filter inactive customers and invalid ages (18–80)
- Add audit timestamp

**Silver Master Table — Join:**
- Orders LEFT JOIN Customers on `CustomerID`
- Result LEFT JOIN Products on `ProductID`
- Saved as `basic.default.Salesdata`

---

## Gold Layer

### Gold 1 — Monthly Revenue Trend
```
Table   : basic.default.MonthlyRevenue_SalesData
Group by: Year, Month
Metrics : TotalRevenue, TotalOrders, AvgOrderValue
```

### Gold 2 — Customer Segment Analysis
```
Table   : basic.default.customersegmentation_salesdata
Group by: CustomerSegment
Metrics : TotalCustomers, TotalOrders, TotalRevenue, AvgOrderValue
Window  : dense_rank() by TotalRevenue
```

### Gold 3 — Product Performance Ranking
```
Table   : basic.default.Product_performance_Salesdata
Group by: ProductName, Category
Metrics : TotalCustomers, TotalOrders, TotalRevenue, AvgOrderValue
Window  : dense_rank() partitionBy(Category) — ranks products within each category
```

### Gold 4 — Regional Sales Analysis
```
Table     : basic.default.region_sales_salesdata
Group by  : ShippingState, Year, Month
Metrics   : TotalCustomers, TotalOrders, TotalRevenue, AvgOrderValue, RunningTotal
Window    : sum().over(Window.partitionBy(ShippingState)) — running total per state
Partition : Delta table partitioned by ShippingState
```

---

## Key Concepts Used

- Medallion Architecture (Bronze / Silver / Gold)
- Window Functions — `dense_rank()`, `partitionBy()`
- Running total — `sum().over(Window)`
- Partitioned Delta table — `partitionBy("ShippingState")`
- Left joins across 3 tables
- Null handling — `dropna()`, `fillna()`
- Text standardisation — `upper()`, `trim()`, `initcap()`

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Databricks Community Edition | PySpark notebook environment |
| PySpark | Large-scale data transformation |
| Delta Lake | Reliable table storage with ACID transactions |
| Unity Catalog | Table and volume management |

---

## Author
**Arjun Tamilselvan**
Cloud Engineer | Azure Data Engineer
