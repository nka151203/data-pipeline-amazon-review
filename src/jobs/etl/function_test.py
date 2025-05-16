#!/usr/bin/env python3
import os
import boto3
import pyspark.pandas as pd
from botocore.exceptions import ClientError
import json
import smart_open
import pyarrow
from pyspark.sql import SparkSession
import pandas as pdo



os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

# MinIO configurations
MINIO_ENDPOINT = "http://localhost:9000" 
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "raw-review-data"


# Corresponding target directories in the bucket
TARGET_DIR_1 = "Clothing_Shoes_and_Jewelry"
TARGET_DIR_2 = "meta_Clothing_Shoes_and_Jewelry"

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
    


def read_meta_data():
    """
    Read metadata using smart_open and process with pandas
    """
    s3_client = initialize_s3_client()
    file_name = "meta_Grocery_and_Gourmet_Food.jsonl"
    source_uri = f"s3a://{BUCKET_NAME}/meta_Grocery_and_Gourmet_Food/{file_name}"
    
    df = spark.read.json(source_uri, multiLine=True)
    return df


spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
rdd = sc.parallelize(range(1, 1000000), 100)
print("Số partition:", rdd.getNumPartitions())
print("Tổng:", rdd.sum())

