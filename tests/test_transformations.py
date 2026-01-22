import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from datetime import datetime

class TestGoldAggregation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a local SparkSession for testing
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("UnitTest") \
            .config("spark.sql.shuffle.partitions", "1") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_aggregation_logic(self):
        # 1. Prepare Mock Data (Silver Layer Schema)
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("event_type", StringType(), True),
            StructField("ingestion_time", TimestampType(), True)
        ])

        data = [
            ("1", "Electronics", 100.0, "purchase", datetime(2024, 1, 1, 10, 0, 0)),
            ("2", "Electronics", 100.0, "purchase", datetime(2024, 1, 1, 10, 30, 0)),
            ("3", "Books", 20.0, "view_item", datetime(2024, 1, 1, 10, 15, 0))
        ]

        df_silver = self.spark.createDataFrame(data, schema)

        # 2. Apply Aggregation Logic (Logic mirroring gold_aggregation.py)
        # Note: In a real project, we would refactor the logic into a function to import it here.
        # For now, we replicate it to test the *concept*.
        from pyspark.sql.functions import window, col, count, avg

        df_gold = df_silver \
            .groupBy(
                window(col("ingestion_time"), "1 hour"),
                col("category"),
                col("event_type")
            ) \
            .agg(
                count("event_id").alias("total_events"),
                avg("price").alias("avg_product_price")
            )

        # 3. Assertions
        results = df_gold.collect()
        
        # We expect 2 rows: 1 for Electronics/purchase, 1 for Books/view_item
        self.assertEqual(len(results), 2)
        
        for row in results:
            if row['category'] == 'Electronics':
                self.assertEqual(row['total_events'], 2)
                self.assertEqual(row['avg_product_price'], 100.0)
            elif row['category'] == 'Books':
                self.assertEqual(row['total_events'], 1)

if __name__ == '__main__':
    unittest.main()
