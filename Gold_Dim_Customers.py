# Databricks notebook source
from pyspark.sql.functions import col, max, min, count, sum, datediff, lit, when, countDistinct

fact_sales = spark.read.table("olist_project.gold.fact_sales")

dim_customers = (fact_sales
    .groupBy("customer_unique_id")
    .agg(
        min("sale_date").alias("first_purchase_date"),
        max("sale_date").alias("last_purchase_date"),
        countDistinct("order_id").alias("total_orders"), 
        sum("price").alias("total_spent")
    )
)


dim_customers_final = (dim_customers
    .withColumn("days_since_last_purchase", datediff(lit("2018-10-31"), col("last_purchase_date")))
    .withColumn("customer_segment", 
        when(col("total_orders") > 1, "Returning Customer")
        .otherwise("One-Time Buyer"))
)


(dim_customers_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_project.gold.dim_customers"))

print("Sukces! Tabela Gold zaktualizowana o datę pierwszego zakupu.")