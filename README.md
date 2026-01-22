# Hybrid Lakehouse Platform: Streaming & Batch Analytics

![Architecture](https://img.shields.io/badge/Architecture-Hybrid%20Lambda-blue)
![Stack](https://img.shields.io/badge/Stack-Spark%20%7C%20Kafka%20%7C%20Airflow%20%7C%20Delta%20Lake-orange)

## 🚀 Project Overview
This project implements a production-grade **Hybrid Data Lakehouse** that ingests real-time user clickstream data (Streaming) and joins it with product inventory data (Batch) to derive actionable insights on ad performance.

It demonstrates a "MAANG-tier" data engineering capability by solving the **Stream-Static Join** problem using **Apache Spark Structured Streaming** and **Delta Lake**.

## 🏗️ Architecture
1.  **Ingestion Layer**: A Python producer simulates high-velocity click events (`2 events/sec`) pushing to **Apache Kafka**. Daily Product Catalogs are uploaded to **MinIO (S3)**.
2.  **Bronze Layer (Raw)**: Spark Structured Streaming consumes Kafka and writes raw JSON events to **Delta Lake** (partitioned by event type).
3.  **Silver Layer (Enriched)**: A second Spark job performs a **Stream-Static Join** between the real-time Clicks and the static Product Catalog to enrich events with pricing and category data.
4.  **Gold Layer (Aggregated)**: Hourly aggregations (Fact Table) calculating "Revenue per Category" for BI consumption.
5.  **Orchestration**: **Apache Airflow** schedules the batch maintenance and gold layer aggregations.

## 🛠️ Tech Stack
-   **Languages**: Python 3.9, SQL
-   **Streaming**: Apache Kafka, Spark Structured Streaming
-   **Processing**: Apache Spark (PySpark), Delta Lake
-   **Storage**: MinIO (S3 Compatible), Parquet/Delta
-   **Orchestration**: Apache Airflow 2.7
-   **Infrastructure**: Docker, Docker Compose

## ⚡ How to Run
### 1. Start the Platform
```bash
docker-compose up -d --build
```
*Wait for all containers to be healthy (approx 2-3 mins first time).*

### 2. Generate Data
### 2. Generate Data
**Step A: Upload Batch Data (Product Catalog)**
```bash
# Uploads products.csv to MinIO (Run this once)
docker-compose exec jupyter python3 work/src/ingestion/s3_upload.py
```

**Step B: Start Streaming Data (User Clicks)**
```bash
# Runs the producer in the background
docker-compose exec -d jupyter python3 work/src/ingestion/kafka_producer.py
```

### 3. Run the Pipeline
Trigger the Spark Jobs from the Jupyter container:

**Trigger Bronze (Ingestion)**
```bash
docker-compose exec jupyter spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  work/src/processing/bronze_ingest.py
```

**Trigger Silver (Enrichment)**
```bash
docker-compose exec jupyter spark-submit \
  --packages io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  work/src/processing/silver_enrichment.py
```

### 4. Monitor & Analyze
-   **Airflow UI**: [http://localhost:8080](http://localhost:8080) (user: `admin`, pass: `admin`)
-   **MinIO Console**: [http://localhost:9001](http://localhost:9001) (user: `admin`, pass: `password`)
-   **Spark Master**: [http://localhost:8081](http://localhost:8081)

## 🧪 Simulation Scenarios
-   **Latency Test**: Measure the time from `kafka_producer` emission to appearance in `s3a://lakehouse/silver`.
-   **Schema Evolution**: Add a new field to the producer and verify Delta Lake handles it.
