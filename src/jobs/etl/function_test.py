import os
import os
from pyspark.sql import SparkSession
import boto3
from botocore.exceptions import ClientError
from pyspark.sql.types import *
import json
from dotenv import load_dotenv
from utils.connect_minio import *
import pyspark.pandas as ps

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

load_dotenv("./utils/env")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")


import io
import os
def get_next_year(s3_client, bucket):

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
            s3_client.put_object(Bucket=bucket, Key=check_file_key, Body="11\n")
            return last_number
        else:
            raise

    new_number = last_number + 10
    new_content = f"{content}\n{new_number}\n"
    s3_client.put_object(Bucket=bucket, Key=check_file_key, Body=new_content)
    print(f"✅ Đã cập nhật check.txt với giá trị mới: {new_number}")
    return last_number

print(get_next_year(initialize_s3_client(), BUCKET_NAME))
