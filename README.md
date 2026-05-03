# 📊 Olist Customer Retention & RFM Analysis
**Big Data Project | Databricks Lakehouse | PySpark | Power BI**

## 🎯 Project Overview
This project addresses the "Leaky Bucket" syndrome in e-commerce. Using the Olist Brazilian dataset, I built a complete data pipeline to transform raw transactional logs into actionable business insights.

## 🏗️ Architecture (Medallion)
The project follows the **Medallion Architecture** to ensure data quality and reliability:
- **Bronze:** Automated ingestion of raw CSV files into Delta Lake.
- **Silver:** Data cleaning, schema enforcement, and standardization (PySpark).
- **Gold:** Dimensional modeling (Star Schema) and RFM behavioral segmentation.

## 🛠️ Tech Stack
- **Data Engineering:** Apache Spark (PySpark), Databricks.
- **Storage:** Delta Lake (ACID transactions).
- **Analytics:** RFM Modeling, SQL.
- **Visualization:** Power BI (DirectQuery/Import).

## 📈 Business Insights (Power BI)
![Retention Dashboard 1](PBI%20page%201.png)

![Retention Dashboard 2](PBI%20page%202.png)

![Retention Dashboard 3](PBI%20page%203.png)
1. **Retention Crisis:** 96.9% of customers are one-time buyers.
2. **Operational Churn:** Delivery delays are the primary driver of customer loss (94.5% churn for late deliveries).
3. **Revenue Potential:** A 12% increase in retention could significantly boost total revenue.

## 🚀 How to Run
1. Import the notebooks from the `/src` folder into your Databricks Workspace.
2. Configure your Unity Catalog Volumes for data landing.
3. Run notebooks in sequence: 01_Bronze -> 02_Silver -> 03_Gold.
