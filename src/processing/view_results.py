from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ViewGold") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n\n" + "="*50)
print("             GOLD LAYER RESULTS")
print("="*50)
try:
    df = spark.read.format("delta").load("s3a://lakehouse/gold/category_performance")
    df.show(truncate=False)
    print("="*50 + "\n")
except Exception as e:
    print(f"Error reading Gold table: {e}")
