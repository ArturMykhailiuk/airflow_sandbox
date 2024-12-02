from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

# Визначення базового шляху до проекту
BASE_PATH = os.getenv('BASE_PATH', '/opt/airflow/dags')

# Визначення DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='A simple data pipeline',
    schedule_interval='@daily',
)

# Завдання для запуску landing_to_bronze.py
landing_to_bronze = BashOperator(
    task_id='landing_to_bronze',
    bash_command=f'python {BASE_PATH}/p2_1_landing_to_bronze.py',
    dag=dag,
)

# Завдання для запуску bronze_to_silver.py
bronze_to_silver = BashOperator(
    task_id='bronze_to_silver',
    bash_command=f'python {BASE_PATH}/p2_2_bronze_to_silver.py',
    dag=dag,
)

# Завдання для запуску silver_to_gold.py
silver_to_gold = BashOperator(
    task_id='silver_to_gold',
    bash_command=f'python {BASE_PATH}/p2_3_silver_to_gold.py',
    dag=dag,
)

# Визначення послідовності завдань
landing_to_bronze >> bronze_to_silver >> silver_to_gold