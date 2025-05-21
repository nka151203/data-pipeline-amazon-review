from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Cấu hình DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='transform_load_every_5_min',
    default_args=default_args,
    description='ETL pipeline chạy mỗi 10 phút',
    schedule_interval='*/5 * * * *',  # chạy mỗi 10 phút
    start_date=datetime(2025, 5, 21),
    catchup=False,
    tags=['etl'],
) as dag:

    transform_task = BashOperator(
        task_id='run_transform_data',
        bash_command='python3 /opt/src/jobs/etl/transform.py',
    )

    load_task = BashOperator(
        task_id='run_load_data',
        bash_command='python3 /opt/src/jobs/etl/load.py',
    )

    # Thiết lập thứ tự chạy
    transform_task >> load_task
