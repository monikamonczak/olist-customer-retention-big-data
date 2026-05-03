# Databricks notebook source
from pyspark.sql.functions import col, lower, trim


products = spark.read.table("olist_project.silver.products")
translations = spark.read.table("olist_project.silver.category_translation")


dim_products_gold = (products
    .join(translations, "product_category_name", "left")
    .select(
        col("product_id"),
        col("product_category_name_english").alias("category_name"),
        col("product_weight_g"),
        col("product_length_cm"),
        col("product_height_cm"),
        col("product_width_cm")
    )
)

(dim_products_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_project.gold.dim_products"))

print("Sukces! Tabela Wymiaru Produktów z angielskimi nazwami gotowa.")