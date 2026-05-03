# Databricks notebook source
df_reviews = spark.read.table("olist_project.bronze.olist_order_reviews_dataset")

display(df_reviews.limit(5))
display(df_reviews.summary())
df_reviews.printSchema()

from pyspark.sql.functions import col, sum as _sum
display(df_reviews.select([_sum(col(c).isNull().cast("int")).alias(c) for c in df_reviews.columns]))