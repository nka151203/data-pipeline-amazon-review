import os
from pyspark.sql import SparkSession
import boto3
from botocore.exceptions import ClientError
import shutil
from pyspark.sql.types import *
import json
from dotenv import load_dotenv
from utils.connect_minio import *


load_dotenv("/opt/src/jobs/etl/utils/env")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
print("Check load env variables: ",BUCKET_NAME)

# SparkSession Initializion
spark = SparkSession.builder \
    .appName("Merge Reviews with Metadata") \
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
    

spark.conf.set('spark.sql.caseSensitive', "true")

def main():
    
    #Schema for Metadata
    schema = StructType([
        StructField("main_category", StringType(), True),
        StructField("title", StringType(), True),
        StructField("average_rating", DoubleType(), True),
        StructField("rating_number", LongType(), True),
        StructField("features", ArrayType(StringType()), True),
        StructField("description", ArrayType(StringType()), True),
        StructField("price", StringType(), True),
        StructField("images", ArrayType(
            StructType([
                StructField("thumb", StringType(), True),
                StructField("large", StringType(), True),
                StructField("variant", StringType(), True),
                StructField("hi_res", StringType(), True)
            ])
        ), True),
        StructField("videos", ArrayType(StringType()), True),
        StructField("store", StringType(), True),
        StructField("categories", ArrayType(StringType()), True),
        StructField("details", StructType([
            StructField("Package Dimensions", StringType(), True),
            StructField("Item Weight", StringType(), True),
            StructField("Manufacturer", StringType(), True),
            StructField("Date First Available", StringType(), True),
            StructField("Brand", StringType(), True),
            StructField("Color", StringType(), True),
            StructField("Age Range (Description)", StringType(), True)
        ]), True),
        StructField("parent_asin", StringType(), True),
        StructField("bought_together", StringType(), True)
    ])
    
    # Read metadaya MinIO    
    metadata_path = f"s3a://{BUCKET_NAME}/meta_Grocery_and_Gourmet_Food/meta_Grocery_and_Gourmet_Food.jsonl.gz"
    metadata_df = spark.read.schema(schema).json(metadata_path)
    metadata_df = metadata_df.drop("images", "videos", "details") 
    

    # Read review data from  MinIO
    input_path = f"s3a://{BUCKET_NAME}/Grocery_and_Gourmet_Food"
    review_files = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(input_path)
    review_files_fs = review_files.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    review_files_list = review_files_fs.listStatus(review_files)
    
    for file_status in review_files_list:
        file_path = file_status.getPath()
        filename = file_path.getName()
        
        if filename.endswith(".jsonl.gz"):
            review_path = str(file_path)
            print(f"\nExtract Raw: {filename}")

            try:
                reviews_df = spark.read.json(review_path)
                reviews_df = reviews_df.drop("title", "images")
            except Exception as e:
                print(f"Error {filename}: {e}")
                continue


            merged_df = reviews_df.join(metadata_df, on="parent_asin", how="inner")
            print(f"Merged file length: {merged_df.count()}")

            output_filename = filename.replace(".jsonl.gz", "_merge.jsonl.gz")
            local_output_path = os.path.join("s3a://raw-review-data/merged-data", output_filename)
            merged_df.write \
                .mode("overwrite") \
                .option("compression", "gzip") \
                .json(local_output_path)

            print(f"Saved: {local_output_path}, all functions are OK")



if __name__ == "__main__":
    main()
