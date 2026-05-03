# Databricks notebook source

df = spark.read.table("olist_project.bronze.olist_orders_dataset")


display(df.limit(5))

df.printSchema() 


display(df.summary()) 


from pyspark.sql.functions import col, sum as _sum
display(df.select([_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]))