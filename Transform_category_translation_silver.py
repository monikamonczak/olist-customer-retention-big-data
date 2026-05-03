# Databricks notebook source
from pyspark.sql.functions import col, lower, trim

df_trans = spark.read.table("olist_project.bronze.product_category_name_translation")

silver_trans = (df_trans
    .withColumn("product_category_name", lower(trim(col("product_category_name"))))
    .withColumn("product_category_name_english", lower(trim(col("product_category_name_english"))))
    .dropDuplicates(["product_category_name"])
)


(silver_trans.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_project.silver.category_translation"))

print(f"Słownik kategorii gotowy. Mamy {silver_trans.count()} unikalnych par językowych.")