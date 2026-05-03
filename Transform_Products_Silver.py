# Databricks notebook source
from pyspark.sql.functions import col, lower, trim, coalesce, lit, regexp_replace, round

df_prod = spark.read.table("olist_project.bronze.olist_products_dataset")


silver_products = (df_prod
    .withColumn("product_category_name", lower(trim(col("product_category_name"))))
    .withColumn("product_category_name", coalesce(col("product_category_name"), lit("unknown")))
    .withColumn("product_weight_g", regexp_replace(col("product_weight_g").cast("string"), ",", ".").cast("double"))
    .withColumn("product_length_cm", regexp_replace(col("product_length_cm").cast("string"), ",", ".").cast("double"))
    .withColumn("product_height_cm", regexp_replace(col("product_height_cm").cast("string"), ",", ".").cast("double"))
    .withColumn("product_width_cm", regexp_replace(col("product_width_cm").cast("string"), ",", ".").cast("double"))
    .filter((col("product_weight_g") >= 0) | col("product_weight_g").isNull())
)

(silver_products.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_project.silver.products"))

print("Tabela Products: Wymiary ustandaryzowane, NULLe w kategoriach obsłużone.")