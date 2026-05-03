# Databricks notebook source
from pyspark.sql.functions import col, trim, lower, regexp_replace, upper


df_customers = spark.read.table("olist_project.bronze.olist_customers_dataset")

silver_customers = (df_customers
    .withColumn("customer_city", lower(trim(col("customer_city"))))
    .withColumn("customer_city", regexp_replace(col("customer_city"), " +", " "))
    .withColumn("customer_city", regexp_replace(col("customer_city"), "[ãáâà]", "a"))
    .withColumn("customer_city", regexp_replace(col("customer_city"), "[éê]", "e"))
    .withColumn("customer_city", regexp_replace(col("customer_city"), "[í]", "i"))
    .withColumn("customer_city", regexp_replace(col("customer_city"), "[õóô]", "o"))
    .withColumn("customer_city", regexp_replace(col("customer_city"), "[ú]", "u"))
    .withColumn("customer_city", regexp_replace(col("customer_city"), "[ç]", "c"))
    .withColumn("customer_state", upper(trim(col("customer_state"))))
    .dropDuplicates(["customer_id"])
)


(silver_customers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_project.silver.customers"))

print("Tabela Customers: Oczyszczona z akcentów i nadmiarowych spacji.")