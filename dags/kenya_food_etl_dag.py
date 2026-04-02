import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

real_file_path = os.path.realpath(__file__)
current_dir = os.path.dirname(real_file_path)
root_path = os.path.abspath(os.path.join(current_dir, '..'))

if root_path not in sys.path:
    sys.path.insert(0, root_path)

from config import Config
from main import run_pipeline

DAG_ID = 'kenya_food_prices_pipeline'

def etl_process():
    print(f'Connecting to Windows Postgres at {Config.DB_HOST}...')
    print(f'Mirroring data to Snowflake account: {Config.SNOW_ACCOUNT}...')
    run_pipeline(full_refresh=True) #Change to true for testing purposes to load new data upon refresh


default_args = {'owner': 'data_engineer', 'depends_on_past': False, 'retries': 2, 'retry_delay': timedelta(minutes=5)}
with DAG(
    dag_id=DAG_ID, 
    default_args=default_args, 
    description='ETL Pipeline for Kenya Food Prices (Req #6, #7)', 
    schedule='@monthly', 
    start_date=datetime(2026, 3, 1), 
    catchup=False
) as dag:
    task_etl = PythonOperator(task_id='run_python_etl', python_callable=etl_process)
    dbt_dir = os.path.join(root_path, 'dbt_project', 'kenya_food_analytics')
    task_dbt = BashOperator(task_id='run_dbt_models', bash_command=f'cd "{dbt_dir}" && dbt run')
    task_etl >> task_dbt


if __name__ == '__main__':
    etl_process()