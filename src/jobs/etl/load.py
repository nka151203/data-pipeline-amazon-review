import os
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
from pgvector.psycopg2 import register_vector
import psycopg2

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

load_dotenv("/opt/src/jobs/etl/utils/env")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
DB_USER = os.getenv("DB_USER")
DB_PORT = os.getenv("DB_PORT")

BUCKET_NAME = "cleaned-review-data"
PROUCT_FOLDER = "cleaned-product-data"
REVIEW_FOLDER = "cleaned-review-data"
REST_FOLDER = "cleaned-rest-data"
ORIGINAL_FOLDER = "cleaned-original-data"

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


def split_dataframe(df, chunk_size=200):
    chunks = list()
    num_chunks = (len(df) + chunk_size - 1) // chunk_size 
    
    for i in range(num_chunks):
        chunk = df[i*chunk_size:(i+1)*chunk_size]
        if not chunk.empty:
            chunks.append(chunk)
    return chunks

def foward_encode_product_data(df_have_text, cursor, num):
    """
    Send available dataset to Postgres for ML work, including product information and embeddings
    """
    df_have_text = df_have_text.toPandas()
    if num == 1:
        cursor.execute('CREATE EXTENSION IF NOT EXISTS vector')
        cursor.execute('DROP TABLE IF EXISTS product_embeddings')
        cursor.execute('CREATE TABLE product_embeddings (asin VARCHAR(15) PRIMARY KEY, product_information TEXT, product_embedding vector(384))')
    
    list_df = split_dataframe(df_have_text)
    for id, df_chunk in enumerate(list_df):
    
        # Convert the `asin` column from the `df_chunk` dataframe into a list with the `to_list()` method
        asin_list = df_chunk['asin'].to_list()

        # Convert the `product_information` column from the `df_chunk` dataframe into a list with the `to_list()` method
        text_list = df_chunk['product_information'].to_list()

        # Perform an API call through the `get_text_embeddings` function
        # Pass the `ENDPOINT_URL` variable and the chunk of texts stored in the list `text_list` as parameters to that function
        embedding_response = get_text_embedding(text=text_list)
        
        # Inserting the data    
        insert_statement = f'INSERT INTO product_embeddings (asin, product_information, product_embedding) VALUES'
        
        value_array = [] 
        for asin, text, embedding in zip(asin_list, text_list, embedding_response):
            value_array.append(f"('{asin}', '{text}', '{embedding}')")
                    
        value_str = ",".join(value_array)
        insert_statement = f"{insert_statement} {value_str};"
        
        cursor.execute(insert_statement)
    print("Finished forward product data embeddings")

def foward_encode_review_data(df_have_text, cursor, num):
    """
    Send available dataset to Postgres for ML work, including review information and embeddings
    """
    df_have_text = df_have_text.toPandas()
    if num == 1:
        cursor.execute('DROP TABLE IF EXISTS review_embeddings')
        cursor.execute('CREATE TABLE review_embeddings (reviewerid VARCHAR(30), asin VARCHAR(15), reviewtext TEXT, review_embedding vector(384), PRIMARY KEY(reviewerid, asin))')
    list_df = split_dataframe(df=df_have_text, chunk_size=200)
    for id, df_chunk in enumerate(list_df):
    
        # Convert the `reviewerid`, `asin` and `reviewtext` columns from the `df_chunk` dataframe into a list with the `to_list()` method
        reviewer_list = df_chunk['user_id'].to_list()
        asin_list = df_chunk['asin'].to_list()
        text_list = df_chunk['text'].to_list()

        # Perform an API call through the `get_text_embeddings` function
        # Pass the `ENDPOINT_URL` variable and the chunk of texts stored in the list `text_list` as parameters to that function
        embedding_response = get_text_embedding(text=text_list)

        ### END CODE HERE ###
        
        # Insert the data
        insert_statement = f'INSERT INTO review_embeddings (reviewerid, asin, reviewtext, review_embedding) VALUES'
        value_array = [] 
        
        for reviewer, asin, text, embedding in zip(reviewer_list, asin_list, text_list, embedding_response):
            value_array.append(f"('{reviewer}', '{asin}', '{text}', '{embedding}')")
            
        value_str = ",".join(value_array)
        insert_statement = f"{insert_statement} {value_str};"
        
        cursor.execute(insert_statement) 
    print("Finished forward review data embeddings")
def foward_review_product_metadata(df, cursor, num):
    """
    Send the rest dataset to Postgres for ML work, including review metadata. 
    This dataset will be used for another model and analytics.
    """
    df.printSchema()
    df = df.toPandas()
    if num == 1:
        cursor.execute('DROP TABLE IF EXISTS review_product_metadata')
        cursor.execute("""
                       CREATE TABLE review_product_metadata (
                        asin TEXT,
                        average_rating FLOAT,
                        helpful_vote INTEGER,
                        parent_asin TEXT,
                        rating FLOAT,
                        store TEXT,
                        user_id TEXT,
                        year INTEGER,
                        month INTEGER,
                        std_price FLOAT,
                        std_rating_number FLOAT,
                        main_category_all_beauty FLOAT,
                        main_category_all_electronics FLOAT,
                        main_category_amazon_fashion FLOAT,
                        main_category_amazon_home FLOAT,
                        main_category_arts_crafts_sewing FLOAT,
                        main_category_cell_phones_accessories FLOAT,
                        main_category_computers FLOAT,
                        main_category_grocery FLOAT,
                        main_category_health_personal_care FLOAT,
                        main_category_industrial_scientific FLOAT,
                        main_category_movies_tv FLOAT,
                        main_category_office_products FLOAT,
                        main_category_pet_supplies FLOAT,
                        main_category_sports_outdoors FLOAT,
                        main_category_tools_home_improvement FLOAT,
                        main_category_toys_games FLOAT,
                        main_category_unspecified FLOAT,
                        verified_purchase INTEGER,
                        PRIMARY KEY (user_id, asin)
                    );
        """)
    insert_query = """
        INSERT INTO review_product_metadata (
            asin, average_rating, helpful_vote, parent_asin, rating, store, user_id, year, month,
            std_price, std_rating_number, main_category_all_beauty, main_category_all_electronics,
            main_category_amazon_fashion, main_category_amazon_home, main_category_arts_crafts_sewing,
            main_category_cell_phones_accessories, main_category_computers, main_category_grocery,
            main_category_health_personal_care, main_category_industrial_scientific, main_category_movies_tv,
            main_category_office_products, main_category_pet_supplies, main_category_sports_outdoors,
            main_category_tools_home_improvement, main_category_toys_games, main_category_unspecified,
            verified_purchase
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for i, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))
        flag = flag + 1
    print("Finished forward metadata data embeddings")
    
def main():
    
    s3_client = initialize_s3_client() #S3 client for MinIO
    num = get_next_part(s3_client, BUCKET_NAME) #Get part number
    num = int(num)
    file_name = get_folder_name(num, False)  #Get full name of a part data
    # num = 1
    # file_name = "Grocery_and_Gourmet_Food_part_000221_cleaned"  
    conn = psycopg2.connect( 
        database="postgres", user="postgres", 
        password="", host="172.18.0.2", port="5432"
    ) 
    df_product = spark.read.parquet(f"s3a://{BUCKET_NAME}/cleaned-product-data/{file_name}/*.snappy.parquet")
    df_review = spark.read.parquet(f"s3a://{BUCKET_NAME}/cleaned-review-data/{file_name}/*.snappy.parquet")
    df_rest_review_metadata = spark.read.parquet(f"s3a://{BUCKET_NAME}/cleaned-rest-data/{file_name}/*.snappy.parquet")
    
    # Set autocommit to true
    conn.autocommit = True
    # Create cursor
    cursor = conn.cursor()
    cursor.execute('CREATE EXTENSION IF NOT EXISTS vector')
    register_vector(conn)
    
    #Send Encode product data to Postgres
    foward_encode_product_data(df_product, cursor, num)
    #Send Encode review data to Postgres
    foward_encode_review_data(df_review, cursor, num)
    #Send other transformed data to Postgres for another model and analytics.
    foward_review_product_metadata(df_rest_review_metadata, cursor, num)
    
    print("CONGRATS, YOU HAVE FINISHED TRANSFORM PROCESS !")

if __name__ == "__main__":
    main()