import os
import boto3
from botocore.exceptions import ClientError

# MinIO config
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "raw-review-data"

def initialize_s3_client():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=boto3.session.Config(signature_version="s3v4"),
        verify=False
    )
    return s3

def upload_directory_to_minio(s3_client, local_dir, bucket_name, prefix=""):
    if not os.path.exists(local_dir):
        print(f"❌ Thư mục không tồn tại: {local_dir}")
        return

    for root, dirs, files in os.walk(local_dir):
        for file in files:
            # End with .jsonl.gz
            if file.endswith('.jsonl.gz'):
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, local_dir)
                s3_key = os.path.join(prefix, relative_path).replace("\\", "/")

                try:
                    print(f"📤 Uploading {local_path} → s3://{bucket_name}/{s3_key}")
                    s3_client.upload_file(local_path, bucket_name, s3_key)
                except ClientError as e:
                    print(f"❌ Lỗi khi upload {local_path}: {e}")
    print("Done upload raw file to S3")

def main():
    s3 = initialize_s3_client()

    # Tạo bucket nếu chưa tồn tại
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        print(f"✅ Bucket '{BUCKET_NAME}' đã tồn tại.")
    except ClientError:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"✅ Bucket '{BUCKET_NAME}' đã được tạo.")

    # Upload 2 thư mục
    upload_directory_to_minio(s3, "/opt/dataset/Grocery_and_Gourmet_Food", BUCKET_NAME, "Grocery_and_Gourmet_Food")
    upload_directory_to_minio(s3, "/opt/dataset/meta_Grocery_and_Gourmet_Food", BUCKET_NAME, "meta_Grocery_and_Gourmet_Food")

if __name__ == "__main__":
    main()
