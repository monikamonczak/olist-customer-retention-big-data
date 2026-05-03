# Databricks notebook source
from pyspark.sql.functions import col, regexp_replace, round, trim, to_timestamp


df_items = spark.read.table("olist_project.bronze.olist_order_items_dataset")


silver_order_items = (df_items
    .withColumn("price", 
                regexp_replace(col("price").cast("string"), ",", ".").cast("double"))
    .withColumn("freight_value", 
                regexp_replace(col("freight_value").cast("string"), ",", ".").cast("double"))
    .filter(col("price") > 0)
    .dropDuplicates(["order_id", "order_item_id"]) 

    

    .withColumn("price", round(col("price"), 2))
    .withColumn("freight_value", round(col("freight_value"), 2))
    
    
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("product_id", trim(col("product_id")))
    .withColumn("seller_id", trim(col("seller_id")))
    .withColumn("shipping_limit_date", to_timestamp(col("shipping_limit_date")))
    
    
    .filter(col("price") > 0)
    .filter(col("order_id").isNotNull() & col("product_id").isNotNull())
)


(silver_order_items.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_project.silver.order_items"))

print("Tabela Order Items sfinalizowana z pełnym zabezpieczeniem separatorów.")