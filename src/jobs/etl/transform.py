import os
import re
from pyspark.sql import SparkSession
import boto3
from botocore.exceptions import ClientError
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
from dotenv import load_dotenv
from utils.connect_minio import *
from utils.get_text_embedding import *
import pyspark.pandas as ps
import pandas as pd
from sklearn.preprocessing import StandardScaler, OneHotEncoder


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
    """
    Read data from raw data bucket
    """
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
def process_review_metadata(raw_df):
    """
    Process review data side
    """
    raw_df = raw_df.dropna(subset=["timestamp"], how = "any")
    
    # Convert Unix timestamp to timestamp type
    raw_df = raw_df.withColumn("timestamp", from_unixtime((col("timestamp") / 1000).cast("long")).cast("timestamp"))
    raw_df = raw_df.withColumn("year", year("timestamp")) \
       .withColumn("month", month("timestamp"))
       
    """
    Process review metadata side
    """
    # Drop rows that have null cells
    raw_df = raw_df.dropna(subset=["asin", "price"], how = "any")
    # Transform description column: fill or convert array String list to String
    raw_df = raw_df.withColumn(
        "description",
        when((col("description").isNotNull()) & (size(col("description")) > 0), col("description")[0])
        .otherwise(lit("No description"))
        )
    # Fill null values
    raw_df = raw_df.fillna({
        'title': 'No title',
        'store': 'Unknown Store',
        'main_category': 'unspecified',
        'text': 'No comment'
    })
    # Drop unused columns
    raw_df = raw_df.drop("features", "categories", "timestamp")
    return raw_df

def clean_text(text):
    """
    Clean input string using the following steps:
    1. Convert text to lowercase for consistency.
    2. Remove punctuation characters (e.g., .,!?).
    3. Remove special characters that are not alphanumeric or whitespace.
    4. Strip leading/trailing whitespace and collapse multiple spaces to a single space.

    Parameters:
        text (str): Input string to be cleaned.

    Returns:
        str: Cleaned and normalized string.
    """
    text = text.lower()
    text = re.sub(r'[^a-zA-Z\s]', '', text) # Remove special characters
    text = re.sub(r"\s+", " ", text) # Remove redundant spaces
    return text.strip()

def clean_text_review_metadata(df):
    clean_text_udf = udf(clean_text, StringType()) # Register UDF
    columns_to_clean = ['text', 'description', 'title', 'store', 'main_category']
    # Apply cleaning function
    for column in columns_to_clean:
        df = df.withColumn(column, clean_text_udf(col(column)))
    
    # Use the `fillna()` dataframe method to fill the missing values from the `title` with those from `description` column of the dataframe `reviews_product_metadata_df`
    """
    coalesce returns the first non-null value in the list of columns.
    If title has a value (not null), it is used; otherwise it is taken from description.
    """
    df = df.withColumn("product_information", coalesce(col("title"), col("description")))

    # Drop `title` and `description` columns of the dataframe `reviews_product_metadata_df`
    df = df.drop("title", "description")
    return df

def process_numerical_category_features(df):
    """
    Process Numerical and Category Features
    1. Standardize numerical columns including Price and Rating number Column
    2. Encode category columns with One Hot Encoder
    3. Encode verified_purchase
    
    """
    df_pd = df.toPandas()
    ## 1. Standardize numerical columns
    reviews_num_columns = ["price", "rating_number"]
    df_std = df_pd[reviews_num_columns]
    new_reviews_num_columns = ["std_price", "std_rating_number"]
    # Create a `StandardScaler` instance
    reviews_num_std_scaler = StandardScaler()
    # Standardize the numerical columns
    df_std = reviews_num_std_scaler.fit_transform(df_std[reviews_num_columns])
    df_std = pd.DataFrame(df_std, columns=new_reviews_num_columns)
    
    ## 2. Encode category columns with One Hot Encoder
    sales_category_ohe = OneHotEncoder(handle_unknown="ignore")
    # Copy the column `sales_category` of the dataframe `reviews_product_metadata_df` with the method `copy()`
    # You will need to use double square brackets to output it as a dataframe, not a series
    sales_category_df = df_pd[["main_category"]].copy()
    # Convert string categories into lowercase (the code line is complete)
    sales_category_df["main_category"] = sales_category_df["main_category"].map(lambda x: x.strip().lower())  

    # Fit your encoder `sales_category_ohe` to the `sales_category_df` dataframe with the `fit()` method
    sales_category_ohe.fit(sales_category_df[["main_category"]])
    # Apply the transformation using the same encoder over the same column. You will need to use the `transform()` method
    # Chain `todense()` method to create a dense matrix for the encoded data
    encoded_sales_category = sales_category_ohe.transform(sales_category_df[["main_category"]]).todense()
    # Convert the result to DataFrame
    encoded_sales_category_df = pd.DataFrame(
        encoded_sales_category, 
        columns=sales_category_ohe.get_feature_names_out(["main_category"]),
        index=df_pd.index
    )
    
    ## 3. Encode verified_purchase
    verified_purchase_df = df_pd[["verified_purchase"]].copy()
    verified_purchase_df["verified_purchase"] = verified_purchase_df["verified_purchase"].map(lambda x: 1 if str(x).lower() == "true" == "true" else 0)
    
    ## 4. Covert back to spark dataframe
    df_pd = df_pd.drop(reviews_num_columns, axis=1)
    df_pd = df_pd.drop(["main_category", "verified_purchase"], axis=1)
    df_pd = pd.concat([df_pd, df_std, encoded_sales_category_df, verified_purchase_df], axis=1)
    df = spark.createDataFrame(df_pd)
    return df

def split_data(df):
    """
    Splits the original DataFrame into three parts:
    - review_text_df: user_id, asin, and text (duplicates removed)
    - product_infor_df: asin and product_information (duplicates removed)
    - df: original DataFrame with 'text' and 'product_information' columns dropped
    """
    review_text_df = df.select("user_id", "asin", "text")
    review_text_df = review_text_df.drop_duplicates()
    product_infor_df = df.select("asin", "product_information")
    product_infor_df =  product_infor_df.drop_duplicates()
    df = df.drop("text", "product_information")
    
    return review_text_df, product_infor_df, df

def embedding
def main():
    df_sample = spark.read.json("s3a://raw-review-data/merged-data/Grocery_and_Gourmet_Food_part_000005_merge.jsonl.gz")
    df_sample = process_review_metadata(df_sample)
    df_sample = clean_text_review_metadata(df_sample)
    df_sample = process_numerical_category_features(df_sample)
    df1, df2, df3 = split_data(df_sample)
    df1.show(5)
    # df_sample.printSchema()
    # print(df_sample.select("main_category").distinct().count())
    # df_sample.write \
    # .mode("overwrite") \
    # .option("compression", "gzip") \
    # .json("s3a://raw-review-data/check_num")

    #print(df_sample.count())

if __name__ == "__main__":
    main()