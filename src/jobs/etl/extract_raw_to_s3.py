import os
from pyspark.sql import SparkSession
import boto3
from botocore.exceptions import ClientError
import shutil

MINIO_ENDPOINT = "http://spark-master:9000/" 
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "raw-review-data"

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("Merge Reviews with Metadata") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.jars", "/opt/spark-3.4.1-bin-hadoop3/jars/hadoop-aws-3.3.1.jar,/opt/spark-3.4.1-bin-hadoop3/jars/aws-java-sdk-bundle-1.11.1026.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark-3.4.1-bin-hadoop3/jars/hadoop-aws-3.3.1.jar:/opt/spark-3.4.1-bin-hadoop3/jars/aws-java-sdk-bundle-1.11.1026.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark-3.4.1-bin-hadoop3/jars/hadoop-aws-3.3.1.jar:/opt/spark-3.4.1-bin-hadoop3/jars/aws-java-sdk-bundle-1.11.1026.jar") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()
    

spark.conf.set('spark.sql.caseSensitive', True)

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

def upload_directory(s3_client, source_dir, bucket_name, target_dir):
    """Upload all contents from a local directory to the bucket"""
    if not os.path.exists(source_dir):
        print(f"Source directory '{source_dir}' not found.")
        return

    for root, dirs, files in os.walk(source_dir):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, source_dir)
            s3_key = f"{target_dir}/{relative_path}"

            try:
                print(f"Uploading: {local_path} -> s3://{bucket_name}/{s3_key}")
                s3_client.upload_file(local_path, bucket_name, s3_key)
            except ClientError as e:
                print(f"Error while uploading {local_path}: {e}")


def main():
    s3_client = initialize_s3_client()
    if check_bucket_exists(s3_client, BUCKET_NAME) == False:
        create_bucket(s3_client, BUCKET_NAME)
    
    # Đọc metadata từ MinIO    
    metadata_path = f"s3a://{BUCKET_NAME}/meta_Grocery_and_Gourmet_Food/meta_Grocery_and_Gourmet_Food.jsonl.gz"
    metadata_df = spark.read.json(metadata_path)
    metadata_df = metadata_df.drop("images", "videos") 
    
    print(f"Metadata đã được tải: {metadata_df.count()} dòng.")

    temp_output_dir = "temp_output"
    os.makedirs(temp_output_dir, exist_ok=True)

    # Đọc review files từ MinIO
    input_path = f"s3a://{BUCKET_NAME}/Grocery_and_Gourmet_Food"
    review_files = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(input_path)
    review_files_fs = review_files.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    review_files_list = review_files_fs.listStatus(review_files)
    
    for file_status in review_files_list:
        file_path = file_status.getPath()
        filename = file_path.getName()
        
        if filename.endswith(".jsonl.gz"):
            review_path = str(file_path)
            print(f"\nĐang xử lý: {filename}")

            try:
                reviews_df = spark.read.json(review_path)
                reviews_df = reviews_df.drop("title")
            except Exception as e:
                print(f"Lỗi khi đọc {filename}: {e}")
                continue

            print(f"Số dòng review: {reviews_df.count()}")

            merged_df = reviews_df.join(metadata_df, on="parent_asin", how="inner")
            print(f"Số dòng sau khi merge: {merged_df.count()}")

            output_filename = filename.replace(".jsonl.gz", "_merge.jsonl.gz")
            local_output_path = os.path.join(temp_output_dir, output_filename)

            print("Số partition:", merged_df.rdd.getNumPartitions())
            merged_df.write \
                .mode("overwrite") \
                .option("compression", "gzip") \
                .json(local_output_path)

            print(f"Đã lưu tạm vào: {local_output_path}")

    # Upload toàn bộ temp_output_dir lên S3
    upload_directory(s3_client, temp_output_dir, BUCKET_NAME, target_dir="merged-data")

    # Tùy chọn: xóa thư mục tạm sau khi upload
    shutil.rmtree(temp_output_dir)

if __name__ == "__main__":
    main()