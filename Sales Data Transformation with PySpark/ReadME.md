# Project 2 — Sales Data Transformation with PySpark
## Databricks Community Edition · PySpark · Delta Lake

---

## Overview

A PySpark transformation project that processes 200,000 rows of retail sales data on Databricks, implementing Medallion Architecture (Bronze → Silver → Gold).

---

## Architecture

```
Bronze  →  Raw data (200,000 rows) from catalog table
Silver  →  Cleaned and validated (133,000 rows)
Gold    →  4 business-ready aggregated tables
```

---

## What it does

**Silver Layer — Data Cleaning:**
- Remove duplicates on SaleID
- Drop nulls in critical columns
- Filter only Completed orders
- Standardise text columns to uppercase
- Add audit timestamp

**Gold Layer — Aggregations:**

| Table | Description |
|-------|-------------|
| sales_monthly_revenue | Monthly revenue trend by Region, Year, Month |
| sales_top_10_products | Top 10 products by total revenue |
| sales_salesrep_scorecard | Sales rep performance — revenue, orders, avg discount |
| sales_customer_segment | Revenue breakdown by customer segment |

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Databricks Free Edition | PySpark notebook environment |
| PySpark | Data transformation |
| Delta Lake | Table storage format |
| Unity Catalog | Table management |

---

## Author
**Arjun Tamilselvan**
Cloud Engineer | Azure Data Engineer
