import boto3
import csv
import io
from faker import Faker
import random

# Configuration
MINIO_ENDPOINT = 'http://minio:9000'
ACCESS_KEY = 'admin'
SECRET_KEY = 'password'
BUCKET_NAME = 'lakehouse'
OBJECT_KEY = 'raw/products/products_2024-01-01.csv'

fake = Faker()

def get_s3_client():
    return boto3.client('s3',
                        endpoint_url=MINIO_ENDPOINT,
                        aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY)

def generate_product_data(num_records=100):
    """Generates a CSV string of product data."""
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Header
    writer.writerow(['product_id', 'product_name', 'category', 'price', 'updated_at'])
    
    categories = ['Electronics', 'Home', 'Clothing', 'Books', 'Beauty']
    
    for i in range(1, num_records + 1):
        writer.writerow([
            i,
            fake.catch_phrase(),
            random.choice(categories),
            round(random.uniform(10.0, 500.0), 2),
            fake.date_this_year()
        ])
    
    return output.getvalue()

def upload_to_s3(client, data):
    try:
        # Check if bucket exists, create if not
        try:
            client.head_bucket(Bucket=BUCKET_NAME)
        except:
            print(f"Bucket {BUCKET_NAME} does not exist. Creating...")
            client.create_bucket(Bucket=BUCKET_NAME)
            
        # Upload
        client.put_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY, Body=data)
        print(f"Successfully uploaded {len(data)} bytes to s3://{BUCKET_NAME}/{OBJECT_KEY}")
    except Exception as e:
        print(f"Upload failed: {e}")

if __name__ == "__main__":
    print("Generating Product Catalog...")
    csv_data = generate_product_data()
    
    print("Uploading to MinIO...")
    s3 = get_s3_client()
    upload_to_s3(s3, csv_data)
