# Databricks notebook source
from pyspark.sql.functions import col, sum as _sum


df_prod = spark.read.table("olist_project.bronze.olist_products_dataset")


display(df_prod.limit(5))
df_prod.printSchema()

display(df_prod.summary())


display(df_prod.select([_sum(col(c).isNull().cast("int")).alias(c) for c in df_prod.columns]))

# COMMAND ----------

df_trans = spark.read.table("olist_project.bronze.product_category_name_translation")
display(df_trans)