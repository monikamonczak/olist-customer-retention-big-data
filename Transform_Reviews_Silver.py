# Databricks notebook source
from pyspark.sql.functions import col, lower, trim, coalesce, lit, expr

df_rev = spark.read.table("olist_project.bronze.olist_order_reviews_dataset")

silver_reviews = (df_rev
    .filter(col("order_id").isNotNull())
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("review_score", expr("try_cast(review_score as int)"))
    .dropDuplicates(["order_id"]) 
)

(silver_reviews.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_project.silver.order_reviews"))