#!/bin/bash
set -e

# Khởi động MinIO trong background
echo "Khởi động MinIO Server..."
mkdir -p /data/minio
nohup minio server /data/minio > /var/log/minio.log 2>&1 &

# Khởi động Spark Worker
echo "Khởi động Spark Worker..."
cd ${SPARK_HOME}
./sbin/start-worker.sh ${SPARK_MASTER_URL}

# Giữ container chạy
tail -f /dev/null