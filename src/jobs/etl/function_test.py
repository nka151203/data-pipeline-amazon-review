import psycopg2

# Thay vì "spark-master", hãy thử IP trực tiếp
conn = psycopg2.connect( 
    database="postgres", user="postgres", 
    password="", host="172.18.0.2", port="5432"
)