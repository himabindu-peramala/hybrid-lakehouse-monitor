from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, sum, avg

# Initialize Spark
spark = SparkSession.builder \
    .appName("GoldAggregation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. Read Silver Layer (Enriched Clicks)
print("Reading Silver Layer...")
df_silver = spark.read \
    .format("delta") \
    .load("s3a://lakehouse/silver/enriched_clicks")

# 2. Daily Aggregation
# Business Question: "Which Categories trigger the most clicks/interest per hour?"
print("Performing Aggregations...")
df_gold = df_silver \
    .groupBy(
        window(col("ingestion_time"), "1 hour"),
        col("category"),
        col("event_type")
    ) \
    .agg(
        count("event_id").alias("total_events"),
        avg("price").alias("avg_product_price")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("category"),
        col("event_type"),
        col("total_events"),
        col("avg_product_price")
    )

# 3. Write to Gold (Delta)
# This table is what you would expose to Tableau/Superset/dbt
print("Writing to Gold Layer...")
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://lakehouse/gold/category_performance")

print("Gold Layer refresh complete.")
