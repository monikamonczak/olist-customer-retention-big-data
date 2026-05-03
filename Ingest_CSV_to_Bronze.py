# Databricks notebook source

path_to_raw_files = "/Volumes/olist_project/bronze/raw_files/"


target_schema = "olist_project.bronze"

# COMMAND ----------


files = [
    "olist_customers_dataset",
    "olist_geolocation_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "product_category_name_translation"
]


for file_name in files:
    print(f"Przetwarzam plik: {file_name}...")
    
 
    df = (spark.read
          .format("csv")
          .option("header", "true")       
          .option("inferSchema", "true")  
          .load(f"{path_to_raw_files}{file_name}.csv"))
    
   
    (df.write
     .format("delta")
     .mode("overwrite")
     .saveAsTable(f"{target_schema}.{file_name}"))
    
    print(f"Sukces! Tabela {file_name} została utworzona.")