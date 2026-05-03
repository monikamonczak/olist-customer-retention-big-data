# Databricks notebook source

df_items = spark.read.table("olist_project.bronze.olist_order_items_dataset")


display(df_items.limit(5))
df_items.printSchema()


display(df_items.summary())


from pyspark.sql.functions import col, sum as _sum
display(df_items.select([_sum(col(c).isNull().cast("int")).alias(c) for c in df_items.columns]))

print("Rekordy z przecinkami w cenie:")
df_items.filter(col("price").cast("string").contains(",")).show()


df_items.printSchema()