import os
from pyspark.sql import SparkSession
import boto3
from botocore.exceptions import ClientError
import shutil
from pyspark.sql.types import *
import json
from dotenv import load_dotenv
from utils.connect_minio import *

check = load_dotenv("src/jobs/etl/utils/env")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
print("Load env variables: ",check)


