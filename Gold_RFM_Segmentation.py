# Databricks notebook source
from pyspark.sql.functions import col, max, count, sum, datediff, lit, when, expr
from pyspark.sql import functions as F

fact_sales = spark.read.table("olist_project.gold.fact_sales")


reference_date = fact_sales.select(max("sale_date")).collect()[0][0]

rfm_base = fact_sales.groupBy("customer_unique_id").agg(
    datediff(lit(reference_date), max("sale_date")).alias("recency"),
    count("order_id").alias("frequency"),
    sum("price").alias("monetary")
)


rfm_segmented = rfm_base.withColumn("segment", 
    when((col("frequency") >= 3) | (col("monetary") > 500), "Champions")
    .when((col("frequency") == 2), "Loyal Customers")
    .when((col("recency") > 180) & (col("frequency") >= 2), "At Risk")
    .when((col("recency") <= 90) & (col("frequency") == 1), "New Customers")
    .otherwise("Lost")
)

(rfm_segmented.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_project.gold.rfm_segments"))

print("Sukces! Tabela RFM przygotowana do analizy segmentacji.")