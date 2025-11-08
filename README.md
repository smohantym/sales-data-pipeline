# 🧠 Sales Data ETL Pipeline (Apache Spark + Docker Compose)

A complete, containerized **ETL pipeline** built using **Apache Spark** (3.5.1) and **Docker Compose**.

This project extracts CSV-based sales data, cleans and normalizes it, removes duplicates, and performs simple analytics such as **monthly revenue** and **top customers**.

---

## 🚀 Features

✅ Extracts sales data (CSV) from a mounted input directory  
✅ Cleans duplicates, standardizes column names, and normalizes data  
✅ Parses flexible date formats and cleans numeric columns (currency symbols, etc.)  
✅ Writes **partitioned Parquet outputs** (by month)  
✅ Generates **monthly revenue** and **top customers** analytics in both Parquet & CSV  
✅ Runs locally using Docker — no Spark installation needed  

---

## 🧩 Architecture Overview

