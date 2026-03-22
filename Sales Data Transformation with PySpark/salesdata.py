"""
============================================================
Sales Data Transformation with PySpark — Databricks
============================================================
Author      : Arjun Tamilselvan
Tools       : Databricks Community Edition, PySpark, Delta Lake
Dataset     : 200,000 rows of retail sales data
Architecture: Medallion Architecture (Bronze → Silver → Gold)

Tables Built:
  Silver → df_cleaned                      (133K cleaned rows)
  Gold 1 → sales_monthly_revenue           (monthly trend by region)
  Gold 2 → sales_top_10_products_byrevenue (top 10 products)
  Gold 3 → sales_salesrep_scorecard        (sales rep performance)
  Gold 4 → sales_customer_segment          (customer segment analysis)
============================================================
"""

from pyspark.sql.functions import *

# ══════════════════════════════════════════════════════════════════
# BRONZE LAYER — Read raw data from catalog table
# ══════════════════════════════════════════════════════════════════

df = spark.read.table("basic.default.sales_data_200_k")
display(df.limit(5))

# ══════════════════════════════════════════════════════════════════
# SILVER LAYER — Clean and validate raw data
# ══════════════════════════════════════════════════════════════════
# Steps:
#   1. Remove duplicates on SaleID
#   2. Drop nulls in critical columns
#   3. Filter only Completed orders
#   4. Standardise Status, ProductName, Region to uppercase
#   5. Add audit timestamp

df_cleaned = df\
    .dropDuplicates(["SaleID"])\
    .dropna(subset=["SaleID","SaleDate","ProductName","Region","NetAmount"])\
    .filter(col("Status") == "Completed")\
    .withColumn("Status",trim(upper(col("Status"))))\
    .withColumn("ProductName",trim(upper(col("ProductName"))))\
    .withColumn("Region",trim(upper(col("Region"))))\
    .withColumn("ProcessedAt", current_timestamp())

display(df.count())
df_cleaned.count()
display(df_cleaned.limit(5))

# ══════════════════════════════════════════════════════════════════
# GOLD LAYER — Business-ready aggregated tables
# ══════════════════════════════════════════════════════════════════

# ── Gold 1 — Monthly Revenue Trend ───────────────────────────────
# Monthly revenue and order count grouped by Region, Year and Month

df_monthly_revenue = df_cleaned \
    .groupBy("Region", "Year", "Month") \
    .agg(
        count("SaleID").alias("TotalOrders"),
        round(sum("NetAmount"), 2).alias("TotalRevenue")
    ) \
    .orderBy("Year", "Month", "Region")

# ── Gold 2 — Top 10 Products by Revenue ──────────────────────────
# Top 10 best performing products ranked by total revenue

df_top_10_products_byrevenue = df_cleaned \
    .groupBy("ProductName","Category")\
    .agg(round(sum("NetAmount"),2).alias("TotalRevenue"),count("SaleID").alias("TotalOrders"))\
    .orderBy(desc("TotalRevenue")).limit(10)

df_top_10_products_byrevenue.display()

# ── Gold 3 — SalesRep Scorecard ───────────────────────────────────
# Performance scorecard per sales representative
# Metrics: total revenue, total orders, average discount given

df_salesrep_scorecard = df_cleaned \
    .groupBy("SalesRep")\
    .agg(round(sum("NetAmount"),2).alias("TotalRevenue"),count("SaleID").alias("TotalOrders"),avg("Discount_Pct").alias("AvgDiscount") )\
    .orderBy(desc("TotalRevenue"))

display(df_salesrep_scorecard)

# ── Gold 4 — Customer Segment Analysis ───────────────────────────
# Revenue and order breakdown by customer segment
# Metrics: total orders, revenue, average order value, average discount

df_customer_segment = df_cleaned \
    .groupBy("CustomerSegment") \
    .agg(
        count("SaleID").alias("TotalOrders"),
        round(sum("NetAmount"), 2).alias("TotalRevenue"),
        round(avg("NetAmount"), 2).alias("AvgOrderValue"),
        round(avg("Discount_Pct"), 2).alias("AvgDiscount")
    ) \
    .orderBy("TotalRevenue", ascending=False)

display(df_customer_segment)

# ══════════════════════════════════════════════════════════════════
# WRITE — Save all tables to catalog
# ══════════════════════════════════════════════════════════════════

df_monthly_revenue.write.mode("overwrite").saveAsTable("basic.default.sales_monthly_revenue")
df_top_10_products_byrevenue.write.mode("overwrite").saveAsTable("basic.default.sales_top_10_products_byrevenue")
df_salesrep_scorecard.write.mode("overwrite").saveAsTable("basic.default.sales_salesrep_scorecard")
df_customer_segment.write.mode("overwrite").saveAsTable("basic.default.sales_customer_segment")

print("Gold 1 saved : basic.default.sales_monthly_revenue")
print("Gold 2 saved : basic.default.sales_top_10_products_byrevenue")
print("Gold 3 saved : basic.default.sales_salesrep_scorecard")
print("Gold 4 saved : basic.default.sales_customer_segment")
