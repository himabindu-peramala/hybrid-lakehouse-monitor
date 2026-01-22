from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

# Initialize Spark with Delta Lake support
spark = SparkSession.builder \
    .appName("ClickstreamIngestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka Configuration
MSG_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("url", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("city", StringType(), True)
])

# 1. Read Stream from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "user_clicks") \
    .option("startingOffsets", "earliest") \
    .load()

# 2. Parse JSON
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), MSG_SCHEMA).alias("data")) \
    .select("data.*") \
    .withColumn("ingestion_time", current_timestamp())

# 3. Write to Delta Lake (Bronze)
# Partition by event_type for optimized querying
query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/bronze_clicks") \
    .option("mergeSchema", "true") \
    .partitionBy("event_type") \
    .start("s3a://lakehouse/bronze/clicks")

print("Streaming to s3a://lakehouse/bronze/clicks has started...")
query.awaitTermination()
