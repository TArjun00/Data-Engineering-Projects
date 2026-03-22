from pyspark.sql.functions import *
from pyspark.sql.window import Window

## Reading the raw data files,writing to DFs and checking the rows count ##

df_bronze_orders = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/basic/default/test/orders.csv")

df_bronze_customers = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/basic/default/test/customers.csv")

df_bronze_products = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/basic/default/test/products.csv")

print(f"Bronze Orders    : {df_bronze_orders.count():,} rows")
print(f"Bronze Customers : {df_bronze_customers.count():,} rows")
print(f"Bronze Products  : {df_bronze_products.count():,} rows")
print("Bronze layer saved successfully!")

## cleaning the Orders bronze DF and saving into a new cleaned DF ##


Orders_cleaned = df_bronze_orders \
    .dropDuplicates(["OrderID"]) \
    .dropna(subset=["OrderID", "CustomerID", "ProductID","NetAmount","OrderDate"]) \
    .withColumn("Status", upper(col("Status"))) \
    .filter(col("Status") != "CANCELLED")\
    .withColumn("Orders_ProcessedAt", current_timestamp())

print(f"Orders_cleaned: {Orders_cleaned.count():,} rows")
print(f"Raw: {df_bronze_orders.count():,} rows")


## cleaning the Products bronze DF and saving into a new cleaned DF ##

Products_cleaned = df_bronze_products \
    .dropDuplicates(["ProductID"]) \
    .dropna(subset=["ProductID", "ProductName", "Category"]) \
    .withColumn("Category",trim(initcap(col("Category")))) \
    .withColumn("Brand",trim(initcap(col("Brand")))) \
    .withColumnRenamed("Status","ProductStatus") \
    .withColumn("ProductStatus",upper(trim(col("ProductStatus")))) \
    .filter(col("ProductStatus") != "INACTIVE") \
    .withColumn("Products_ProcessedAt", current_timestamp())

print(f"Raw products    : {df_bronze_products.count():,}")
print(f"Cleaned products: {Products_cleaned.count():,}")


## cleaning the Customers bronze DF and saving into a new cleaned DF ##

Customers_cleaned = df_bronze_customers \
    .dropDuplicates(["CustomerID"]) \
    .dropna(subset=["CustomerID", "FirstName", "LastName"]) \
    .fillna({"City": "Unknown", "CustomerSegment": "Unknown"}) \
    .withColumn("FirstName",trim(initcap(col("FirstName")))) \
    .withColumn("LastName",trim(initcap(col("LastName")))) \
    .withColumn("City",trim(initcap(col("City")))) \
    .withColumn("State",trim(initcap(col("State")))) \
    .withColumn("CustomerSegment",trim(initcap(col("CustomerSegment")))) \
    .withColumn("Gender",trim(initcap(col("Gender")))) \
    .withColumn("Email",trim(col("Email"))) \
    .withColumn("RegistrationDate",to_date(col("RegistrationDate"), "yyyy-MM-dd")) \
    .withColumn("IsActive",upper(trim(col("IsActive")))) \
    .filter(col("IsActive") != "NO") \
    .filter((col("Age") >= 18) & (col("Age") <= 80)) \
    .withColumn("Customers_ProcessedAt",current_timestamp())

print(f"Raw customers    : {df_bronze_customers.count():,}")
print(f"Cleaned customers: {Customers_cleaned.count():,}")

## creating one Master DF by joining otherDF to create a cleaned master table "Salesdata" for final business Insight processing ## 

df_orders_customers = Orders_cleaned.join(
    Customers_cleaned, ["CustomerID"], "left"
)
Salesdata = df_orders_customers.join(
    Products_cleaned, ["ProductID"], "left"
)

print(f"Silver master rows: {Salesdata.count():,}")

Salesdata.write.format("delta").mode("overwrite") \
    .saveAsTable("basic.default.Salesdata")

#### Creating a Monthly revenue table with Salesdata table ####

data = spark.read.table("basic.default.Salesdata")
display(data.limit(10))
MonthlyRevenue_SalesData = Salesdata \
    .groupBy(
        year("OrderDate").alias("Year"),
        month("OrderDate").alias("Month")
    ) \
    .agg(
        round(sum("NetAmount"), 2).alias("TotalRevenue"),
        count("OrderID").alias("TotalOrders"),
        round(avg("NetAmount"), 2).alias("AvgOrderValue")
    ) \
    .orderBy("Year", "Month")

MonthlyRevenue_SalesData.write.format("delta").mode("overwrite") \
    .saveAsTable("basic.default.MonthlyRevenue_SalesData")


#### Creating a customer segment table with Salesdata table ####

customersegmentation_salesdata = Salesdata \
    .groupBy("CustomerSegment") \
    .agg(
        countDistinct("CustomerID").alias("TotalCustomers"),
        count("OrderID").alias("TotalOrders"),
        round(sum("NetAmount"), 2).alias("TotalRevenue"),
        round(avg("NetAmount"), 2).alias("AvgOrderValue")
    ) \
    .orderBy("TotalRevenue", ascending=False)\
    .filter(col("CustomerSegment").isNotNull()) \
    .filter(col("CustomerSegment") != "Unknown") \
    .withColumn("RevenueRank", dense_rank().over(Window.orderBy(col("TotalRevenue").desc())))
    

customersegmentation_salesdata.write.format("delta").mode("overwrite") \
    .saveAsTable("basic.default.customersegmentation_salesdata")
display(customersegmentation_salesdata.limit(10))


#### creating a product performance table fromm sales data table ####

Product_performance_Saledata = Salesdata\
    .groupBy("ProductName","Category")\
    .agg(
        countDistinct("CustomerID").alias("TotalCustomers"),
        count("OrderID").alias("TotalOrders"),
        round(sum("NetAmount"), 2).alias("TotalRevenue"),
        round(avg("NetAmount"), 2).alias("AvgOrderValue")
    )\
    .filter(col("ProductName").isNotNull())\
    .filter(col("ProductName") != "Unknown")\
    .withColumn("CategoryRevenueRank", dense_rank().over(Window.partitionBy(col("Category")).orderBy(col("TotalRevenue").desc())))\
    .orderBy("TotalRevenue", ascending=False)\
    
Product_performance_Saledata.write.format("delta").mode("overwrite") \
    .saveAsTable("basic.default.Product_performance_Salesdata")

display(Product_performance_Saledata.limit(100))

#### creating a region level sales data table from Salesdata table ####


region_sales_salesdata = Salesdata\
    .filter(col("ShippingState").isNotNull())\
    .filter(col("ShippingState") != "Unknown")\
    .groupBy("ShippingState", year("OrderDate").alias("Year"),month("OrderDate").alias("Month"))\
    .agg(countDistinct("CustomerID").alias("TotalCustomers"),count("OrderID").alias("TotalOrders"),\
        round(sum("NetAmount"), 2).alias("TotalRevenue"),
        round(avg("NetAmount"), 2).alias("AvgOrderValue")
    )\
    .withColumn("revenuebyState",round(sum("TotalRevenue").over(Window.partitionBy(col("ShippingState")).orderBy(col("Year"),col("Month"))),2))\
    .orderBy(asc("Year"))

display(region_sales_salesdata.limit(100))
    
region_sales_salesdata.write.format("delta").mode("overwrite").partitionBy("ShippingState") \
    .saveAsTable("basic.default.region_sales_salesdata")
