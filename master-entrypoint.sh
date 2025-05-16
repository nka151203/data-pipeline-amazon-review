#!/bin/bash
set -e

echo "===== KHỞI ĐỘNG CONTAINER MASTER ====="
echo "Kiểm tra thư mục hiện tại: $(pwd)"
echo "Liệt kê nội dung thư mục SPARK_HOME: ${SPARK_HOME}"
ls -la ${SPARK_HOME}

# Tạo các thư mục cần thiết
mkdir -p /data/minio /var/log ${AIRFLOW_HOME} /opt/jupyter /opt/metabase

# Khởi động MinIO trong background
echo "Khởi động MinIO Server..."
nohup minio server /data/minio --console-address ":9001" > /var/log/minio.log 2>&1 &
echo "MinIO PID: $!"

# Khởi động PostgreSQL
echo "Khởi động PostgreSQL Server..."
service postgresql start
echo "Chờ PostgreSQL sẵn sàng..."
until pg_isready; do
    echo "Đang chờ PostgreSQL..."
    sleep 2
done
echo "PostgreSQL status: $(service postgresql status)"

# Cài đặt các phụ thuộc cần thiết
echo "Cài đặt flask-session và connexion..."
pip install flask-session==0.4.0 connexion==2.14.2

# Khởi tạo Airflow
echo "Khởi tạo Airflow..."
cd ${AIRFLOW_HOME}

# Sử dụng biến môi trường
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@localhost/airflow
export AIRFLOW__CORE__EXECUTOR=LocalExecutor

# Khởi tạo database Airflow nếu chưa có
if [ ! -f "${AIRFLOW_HOME}/airflow.cfg" ]; then
    echo "Tạo file cấu hình và database Airflow mới..."
    airflow db init

    echo "Tạo user Airflow..."
    airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com
else
    if ! airflow db check >/dev/null 2>&1; then
        echo "Khởi tạo lại database Airflow..."
        airflow db init

        echo "Tạo user Airflow..."
        airflow users create \
            --username admin \
            --password admin \
            --firstname Admin \
            --lastname User \
            --role Admin \
            --email admin@example.com
    else
        echo "Database Airflow đã tồn tại..."
    fi
fi

# Khởi động Airflow Webserver & Scheduler
echo "Khởi động Airflow Webserver & Scheduler..."
nohup airflow webserver -p 8081 > /var/log/airflow-webserver.log 2>&1 &
echo "Airflow Webserver PID: $!"
nohup airflow scheduler > /var/log/airflow-scheduler.log 2>&1 &
echo "Airflow Scheduler PID: $!"

# Khởi động Metabase
echo "Khởi động Metabase..."
cd /opt/metabase
nohup java -jar metabase.jar > /var/log/metabase.log 2>&1 &
echo "Metabase PID: $!"

# Khởi động Jupyter Notebook
echo "Khởi động Jupyter Notebook..."
cd /opt/jupyter
nohup jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password='' > /var/log/jupyter.log 2>&1 &
echo "Jupyter PID: $!"

# Khởi động Spark Master
echo "Khởi động Spark Master..."
cd ${SPARK_HOME}
./sbin/start-master.sh
echo "Spark Master đã khởi động"

echo "Khởi động PySpark Shell kết nối Spark Master..."
pyspark --master spark://spark-master:7077

# Liên kết dataset nếu có
if [ ! -L "${SPARK_HOME}/dataset" ] && [ -d "/opt/dataset" ]; then
    echo "Liên kết thư mục dataset..."
    ln -s /opt/dataset ${SPARK_HOME}/dataset
fi

echo "===== TẤT CẢ DỊCH VỤ ĐÃ KHỞI ĐỘNG ====="
echo "Kiểm tra các process đang chạy:"
ps -ef



# Giữ container chạy
echo "Container đang chạy. Đang xem logs..."
tail -f /var/log/minio.log /var/log/airflow-webserver.log /var/log/airflow-scheduler.log /var/log/metabase.log /var/log/jupyter.log ${SPARK_HOME}/logs/*