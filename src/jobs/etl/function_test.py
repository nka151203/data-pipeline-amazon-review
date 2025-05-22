import psycopg2


conn = psycopg2.connect( 
    database="postgres", user="postgres", 
    password="", host="spark-master", port="5432"
)