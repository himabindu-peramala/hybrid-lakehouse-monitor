from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Initialize Spark
spark = SparkSession.builder \
    .appName("SilverEnrichment") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema for Product Catalog (Batch Source)
PRODUCT_SCHEMA = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("updated_at", StringType(), True)
])

# 1. Read the Batch Data (Product Catalog)
# In production, this would be a Delta Table too, but for now we read CSV from ingestion
print("Reading Product Catalog from S3...")
df_products = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(PRODUCT_SCHEMA) \
    .load("s3a://lakehouse/raw/products/")

# 2. Read the Bronze Stream (Delta Lake)
print("Reading Bronze Stream from Delta...")
df_bronze = spark.readStream \
    .format("delta") \
    .load("s3a://lakehouse/bronze/clicks")

# 3. Stream-Static Join
# enriching the clickstream with product details (Name, Price, Category)
df_joined = df_bronze.join(
    df_products,
    df_bronze.product_id == df_products.product_id,
    how="inner" # Inner join drops clicks for unknown products
).drop(df_products.product_id) # Drop duplicate column

# 4. Write to Silver (Delta)
print("Writing to Silver Layer...")
query = df_joined.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/silver_enrichment") \
    .option("mergeSchema", "true") \
    .start("s3a://lakehouse/silver/enriched_clicks")

query.awaitTermination()
