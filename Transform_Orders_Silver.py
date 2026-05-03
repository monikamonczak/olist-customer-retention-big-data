# Databricks notebook source
from pyspark.sql.functions import col, to_timestamp, trim, lower, coalesce


df_orders = spark.read.table("olist_project.bronze.olist_orders_dataset")

silver_orders_prepared = (df_orders
    .withColumn("order_status", lower(trim(col("order_status"))))
    .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp")))
    .withColumn("order_approved_at", to_timestamp(col("order_approved_at")))
    .withColumn("order_delivered_carrier_date", to_timestamp(col("order_delivered_carrier_date")))
    .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))
    .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date")))
)

silver_orders_clean = silver_orders_prepared.filter(
    ~((col("order_status") == "delivered") & (col("order_delivered_customer_date").isNull()))
)


silver_orders_clean = (silver_orders_prepared
    .filter(~((col("order_status") == "delivered") & (col("order_delivered_customer_date").isNull())))
    .filter(col("order_purchase_timestamp") <= coalesce(col("order_delivered_customer_date"), col("order_purchase_timestamp")))
    .dropDuplicates(["order_id"]) # Każde zamówienie musi być unikalne
)

(silver_orders_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true") 
    .saveAsTable("olist_project.silver.orders"))


original_count = df_orders.count()
clean_count = silver_orders_clean.count()
print(f"Oryginalna liczba wierszy: {original_count}")
print(f"Liczba wierszy po czyszczeniu: {clean_count}")
print(f"Usunięto {original_count - clean_count} błędnych rekordów.")