#!/usr/bin/env python3
import os
import boto3
import pyspark.pandas as pd
from botocore.exceptions import ClientError
import json
import smart_open
import pyarrow

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

# MinIO configurations
MINIO_ENDPOINT = "http://localhost:9000" 
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "raw-review-data"

SOURCE_REVIEW_DATA = "Clothing_Shoes_and_Jewelry"
SOURCE_META_DATA = "meta_Clothing_Shoes_and_Jewelry"

def initialize_s3_client():
    """Initialize S3 client for MinIO"""
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1',
        config=boto3.session.Config(signature_version='s3v4'),
        verify=False  # Disable SSL verification (only for local use)
    )
    return s3_client

def check_bucket_exists(s3_client, bucket_name):
    """Check if the bucket already exists"""
    try:
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
        return bucket_name in buckets
    except ClientError as e:
        print(f"Error while checking bucket: {e}")
        return False
def get_next_year(s3):
    """Get next year to process from S3"""
    try:
        # Get Object from S3
        response = s3.get_object(Bucket=BUCKET_NAME, Key="last_process.txt")
        content = response['Body'].read().decode('utf-8')
        # Split this document to lines list
        lines = content.strip().splitlines()
        # Get current year
        current_year = lines[-1]
        if int(current_year) > 2023:
            return "End of data"
        # Calculate next year
        next_year = str(int(current_year) + 1)
        # Append next year after lines list
        updated_content = content.strip() + '\n' + next_year
        # Put list on S3
        s3.put_object(Bucket=BUCKET_NAME, Key="last_process.txt", Body=updated_content.encode('utf-8'))
        return current_year

    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            print(f"Tracker file not found. Creating 'last_process.txt' with value 2005.")
            s3.put_object(Bucket=BUCKET_NAME, Key="last_process.txt", Body="2006".encode())
            return "2005"
        else:
            print(f"Error getting file: {e}")
            return "Some error found"
def read_process_data():
    s3_client = initialize_s3_client()
    current_year = get_next_year(s3_client)
    file_name = f"Grocery_and_Gourmet_Food_{current_year}.jsonl.gz"
    source_uri = f"s3://{BUCKET_NAME}/Grocery_and_Gourmet_Food/{file_name}"
    json_list = []
    try:
        for json_line in smart_open.open(source_uri, transport_params={'client': s3_client}):
            json_list.append(json.loads(json_line))
        df = pd.DataFrame(json_list)
        return df
    except Exception as e:
        print(f"Error reading data from {file_name}: {e}")
        return pd.DataFrame() 
def read_meta_data():
    s3_client = initialize_s3_client()
    file_name = f"meta_Grocery_and_Gourmet_Food.jsonl.gz"
    source_uri = f"s3://{BUCKET_NAME}/meta_Grocery_and_Gourmet_Food/{file_name}"
    json_list = []
    try:
        for json_line in smart_open.open(source_uri, transport_params={'client': s3_client}):
            json_list.append(json.loads(json_line))
        df = pd.DataFrame(json_list)
        return df
    except Exception as e:
        print(f"Error reading data from {file_name}: {e}")
        return pd.DataFrame() 
def process_review(raw_df)
def process_metadata(metd_data_df)
def merge_review_metadata(raw_df, metd_data_df)
def process_textual_features(review_metadata_df)
def process_numerical_features(review_metadata_df)
def split_features(review_metadata_df)
def embed_features(review_metadata_df)
def main()

if __name__ == "__main__":
    main()