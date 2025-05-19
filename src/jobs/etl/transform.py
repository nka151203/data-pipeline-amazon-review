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

# Initilize SparkSession
spark = SparkSession.builder \
    .appName("Transform Reviews-Metadata") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.jars", "/opt/spark-3.4.1-bin-hadoop3/jars/hadoop-aws-3.3.1.jar,/opt/spark-3.4.1-bin-hadoop3/jars/aws-java-sdk-bundle-1.11.1026.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark-3.4.1-bin-hadoop3/jars/hadoop-aws-3.3.1.jar:/opt/spark-3.4.1-bin-hadoop3/jars/aws-java-sdk-bundle-1.11.1026.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark-3.4.1-bin-hadoop3/jars/hadoop-aws-3.3.1.jar:/opt/spark-3.4.1-bin-hadoop3/jars/aws-java-sdk-bundle-1.11.1026.jar") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()
    
spark.conf.set('spark.sql.caseSensitive', True)

def read_data(s3_client):
    start_point = get_next_year(s3_client, BUCKET_NAME)
    if start_point > 220:
        return
    df = spark.createDataFrame([], StructType([]))
    follow = 0
    for i in range(start_point, start_point + 5):
        folder_data_file = get_folder_name(i)
        input_path = f"s3a://{BUCKET_NAME}/merged-data/{folder_data_file}"
        review_files = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(input_path)
        review_files_fs = review_files.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        review_files_list = review_files_fs.listStatus(review_files)
        for file_status in review_files_list:
            file_path = file_status.getPath()
            filename = file_path.getName()
            if filename.endswith(".json.gz"):
                review_path = str(file_path)
                print(f"\nĐang xử lý: {filename}")
                try:
                    if follow == 0:
                        df = spark.read.json(review_path)
                        follow += 1
                    else:
                        df_read = spark.read.json(review_path)
                        df = df.union(df_read)
                        follow += 1
                except Exception as e:
                    print(f"Lỗi khi đọc {filename}: {e}")
                    continue
    return df
def process_review_metadata(raw_df, s3_client):
    raw_df = raw_df.to_pandas_on_spark()
    """
    Process for Review side
    """
    raw_df['reviewTime'] = ps.to_datetime(raw_df['timestamp'], unit='ns')
    # Extract the year and month from the `reviewTime`, and save those values in new columns named `year` and `month`
    # You can apply `.dt.year` and `.dt.month` methods to `raw_df['reviewTime']` to do that
    raw_df['year'] = raw_df['reviewTime'].dt.year
    raw_df['month'] = raw_df['reviewTime'].dt.month
    """
    Process for Metadata side
    """ 
    raw_df = raw_df.dropna(subset = ["asin", "price"], how = "any")
    raw_df = raw_df.drop("features")
    
    return raw_df
    
def main():
    df_sample = spark.read.json("s3a://raw-review-data/merged-data/Grocery_and_Gourmet_Food_part_000004_merge.jsonl.gz")
    df_sample = df_sample.limit(5)
    s3_client = initialize_s3_client()
    df_sample = process_review_metadata(df_sample, s3_client)
    print(df_sample.head(3))

if __name__ == "__main__":
    main()