# Databricks notebook source
from pyspark.sql.functions import col, when, datediff, trim


customers_final = spark.read.table("olist_project.silver.customers") \
    .select(
        trim(col("customer_id")).alias("c_id"), 
        col("customer_unique_id"), 
        col("customer_state")
    )


reviews_final = spark.read.table("olist_project.silver.order_reviews") \
    .select(
        trim(col("order_id")).alias("r_id"), 
        col("review_score")
    )

items_final = spark.read.table("olist_project.silver.order_items") \
    .select(
        trim(col("order_id")).alias("i_id"), 
        col("product_id"), 
        col("price"), 
        col("freight_value")
    )


orders = spark.read.table("olist_project.silver.orders")

fact_sales = (orders
   
    .withColumn("o_id", trim(col("order_id")))
    .withColumn("o_cust_id", trim(col("customer_id")))
    .join(items_final, col("o_id") == col("i_id"), "inner")
    .join(customers_final, col("o_cust_id") == col("c_id"), "inner")
    .join(reviews_final, col("o_id") == col("r_id"), "left")
    .select(
        col("o_id").alias("order_id"), 
        col("customer_unique_id"), 
        col("customer_state"),
        col("product_id"),
        col("price"),
        col("freight_value"),
        col("review_score"), 
        col("order_purchase_timestamp").alias("sale_date"),
        
        datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date")).alias("delay_days"),
        
        when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"), 1)
        .otherwise(0).alias("is_late")
    )
)

(fact_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_project.gold.fact_sales"))

print("Sukces! Tabela Gold Fact Sales jest kompletna i zawiera dane geograficzne.")