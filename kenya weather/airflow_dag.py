# airflow_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.append('/path/to/your/project')

from kenya_weather_dashboard import KenyaWeatherDashboard

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def run_pipeline():
    dashboard = KenyaWeatherDashboard()
    dashboard.run_pipeline()

dag = DAG(
    'kenya_weather_dashboard',
    default_args=default_args,
    description='Kenya Weather Dashboard Pipeline',
    schedule_interval='0 6 * * *',
    catchup=False
)

run_task = PythonOperator(
    task_id='run_pipeline',
    python_callable=run_pipeline,
    dag=dag
)