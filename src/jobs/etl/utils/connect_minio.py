import os
import boto3
from botocore.exceptions import ClientError
from pyspark.sql.types import *
from dotenv import load_dotenv

load_dotenv("./utils/env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")

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

def create_bucket(s3_client, bucket_name):
    """Create a new bucket if it does not exist"""
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' has been created.")
        return True
    except ClientError as e:
        print(f"Error while creating bucket: {e}")
        if e.response['Error']['Code'] in ['BucketAlreadyExists', 'BucketAlreadyOwnedByYou']:
            print(f"Bucket '{bucket_name}' already exists.")
            return True
        return False
    
def get_next_part(s3_client, bucket):
    folder_prefix = f"/check_num"
    check_file_key = f"{folder_prefix}/check.txt"
    try:
        s3_client.head_object(Bucket=bucket, Key=check_file_key)
        response = s3_client.get_object(Bucket=bucket, Key=check_file_key)
        content = response['Body'].read().decode('utf-8').strip()
        last_number = int(content.split()[-1])
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            last_number = 1
            s3_client.put_object(Bucket=bucket, Key=check_file_key, Body="2\n")
            return last_number
        else:
            raise

    new_number = last_number + 1
    new_content = f"{content}\n{new_number}\n"
    s3_client.put_object(Bucket=bucket, Key=check_file_key, Body=new_content)
    print(f"Update new start point: {new_number}")
    return last_number

def get_folder_name(num, is_raw_bucket = True):
    if bucket == True:
        if num < 10:
            return f"Grocery_and_Gourmet_Food_part_00000{num}_merge.jsonl.gz"
        elif num >= 10 and num < 100:
            return f"Grocery_and_Gourmet_Food_part_0000{num}_merge.jsonl.gz"
        else:
            return f"Grocery_and_Gourmet_Food_part_000{num}_merge.jsonl.gz"
    else:
        if num < 10:
            return f"Grocery_and_Gourmet_Food_part_00000{num}_cleaned"
        elif num >= 10 and num < 100:
            return f"Grocery_and_Gourmet_Food_part_0000{num}_cleaned"
        else:
            return f"Grocery_and_Gourmet_Food_part_000{num}_cleaned"
        
    

