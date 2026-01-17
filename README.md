# Cloud_ETL_Marketing_analytics
Built an end-to-end Azure-based marketing analytics ETL pipeline using Lakehouse (Medallion) architecture. Orchestrated with Apache Airflow, it transforms raw CSV data into Parquet fact and dimension tables and powers Power BI dashboards for data-driven decisions.


# 📊 Marketing Analytics Data Pipeline (Azure Lakehouse)

## 📌 Project Overview

Built an end-to-end Azure-based marketing analytics ETL pipeline using Lakehouse (Medallion) architecture. Orchestrated with Apache Airflow, it transforms raw CSV data into Parquet fact and dimension tables and powers Power BI dashboards for data-driven decisions.

---

## 🏗️ Architecture Overview

### Medallion Architecture (Lakehouse)

- **Bronze Layer** – Raw data ingestion  
- **Silver Layer** – Cleaned and standardized data  
- **Gold Layer** – Business-ready analytics tables  

### High-Level Flow

1. Marketing CSV files are uploaded to the **Bronze container**
2. Raw files are validated and cleaned
3. Processed raw files are archived
4. Clean data is stored as **Parquet** in the Silver layer
5. Historical + incremental Silver data is used to generate Gold tables
6. Gold layer is connected to **Power BI** for visualization
---

## 🧰 Tech Stack

| Category | Tools |
|--------|------|
| Cloud Platform | Microsoft Azure |
| Storage | Azure Data Lake Gen2 |
| Orchestration | Apache Airflow |
| Data Processing | Python, Pandas, PyArrow |
| File Format | CSV → Parquet |
| Architecture | Medallion (Lakehouse) |
| Analytics | Power BI |

---

## 📁 Data Layers Breakdown

### 🥉 Bronze Layer
- Stores raw CSV marketing data  
- No transformations applied  
- Acts as a single source of truth  
- Processed files moved to `bronze-processed`

### 🥈 Silver Layer
- Data cleaning and standardization  
- Output format: Parquet  
- Supports historical and incremental data  
- Partitioned by `load_date`

### 🥇 Gold Layer
- Business logic applied  
- Fact and dimension tables created  
- Optimized for BI consumption  

---

## 🔄 Orchestration with Apache Airflow

- Task dependency management  
- Idempotent runs  
- Retry and failure handling  
- Historical backfilling support  

---

## 📈 Analytics & Visualization

- Gold layer connected to Power BI  
- Campaign performance and ROI insights  
- Trend analysis over historical data  

---

## 🎯 Business Use Case

Enables marketing teams to monitor campaign effectiveness, identify high-performing channels, track KPIs, and optimize marketing spend using data-driven insights.

---

## 🧠 Author

**Naveen**  
_Data Engineering / Analytics Enthusiast_
