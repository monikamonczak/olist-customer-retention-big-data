# Databricks notebook source
from pyspark.sql.functions import col, sum as _sum

df_pay = spark.read.table("olist_project.bronze.olist_order_payments_dataset")


display(df_pay.limit(5))
df_pay.printSchema()

display(df_pay.summary())

display(df_pay.select("payment_type").distinct()) 


display(df_pay.select([_sum(col(c).isNull().cast("int")).alias(c) for c in df_pay.columns]))

# COMMAND ----------

# Sprawdźmy, jakie to typy płatności mają wartość 0
display(df_pay.filter(col("payment_value") == 0))

# COMMAND ----------

display(df_pay.filter(col("payment_installments") == 0))