# Databricks notebook source

df_cust = spark.read.table("olist_project.bronze.olist_customers_dataset")


display(df_cust.limit(5))
df_cust.printSchema()
display(df_cust.summary())

from pyspark.sql.functions import col, count, when
display(df_cust.select([count(when(col(c).isNull(), c)).alias(c) for c in df_cust.columns]))

# COMMAND ----------

from pyspark.sql.functions import countDistinct

display(df_cust.select(
    countDistinct("customer_id").alias("Unikalne_Sesje"), 
    countDistinct("customer_unique_id").alias("Fizyczni_Klienci")
))

# COMMAND ----------

# Sprawdzanie miast, które mają w nazwie znaki spoza standardowego alfabetu A-Z
display(df_cust.filter(col("customer_city").rlike("[^a-zA-Z\s]")).select("customer_city").distinct())