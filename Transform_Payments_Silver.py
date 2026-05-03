# Databricks notebook source
from pyspark.sql.functions import col, when, lower, trim, regexp_replace, round


df_pay = spark.read.table("olist_project.bronze.olist_order_payments_dataset")


silver_payments = (df_pay
  
    .withColumn("payment_type", lower(trim(col("payment_type"))))   
    .withColumn("payment_value", 
                regexp_replace(col("payment_value").cast("string"), ",", ".").cast("double"))
    .withColumn("payment_value", round(col("payment_value"), 2))  
    .withColumn("payment_installments", 
                when(col("payment_installments") == 0, 1)
                .otherwise(col("payment_installments")))
    .filter(col("payment_type") != "not_defined")
    .filter(col("payment_value") >= 0)
    .filter(col("order_id").isNotNull())
)


(silver_payments.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true") 
    .saveAsTable("olist_project.silver.payments"))

print(f"Tabela Silver Payments gotowa. Liczba rekordów: {silver_payments.count()}")