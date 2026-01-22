from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'hybrid_lakehouse_pipeline',
    default_args=default_args,
    description='Orchestrates the Hybrid Lakehouse Pipeline (Gold Layer)',
    schedule_interval=timedelta(hours=1), # Run hourly
    catchup=False
)

# Operator to submit the Gold Aggregation Job
# In a real environment, we would also trigger Silver if it wasn't streaming
gold_job = SparkSubmitOperator(
    task_id='gold_aggregation_job',
    conn_id='spark_default', # We need to configure this connection in Airflow UI
    application='/opt/airflow/src/processing/gold_aggregation.py',
    packages='io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4',
    conf={
        "spark.master": "spark://spark-master:7077"
    },
    dag=dag
)

# Dependencies
# In a full pipeline: sensor_s3 -> bronze_job -> silver_job -> gold_job
# Since Bronze/Silver are streaming, we only verify Gold here.
gold_job
