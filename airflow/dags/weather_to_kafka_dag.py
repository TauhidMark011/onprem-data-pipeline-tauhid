from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'commander_tauhid',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'weather_to_kafka_dag',
    default_args=default_args,
    description='Fetch weather from API and push to Kafka',
    schedule_interval='*/5 * * * *',  # Every 5 minute
    catchup=False,
)

weather_to_kafka = BashOperator(
    task_id='fetch_weather_and_send_to_kafka',
    bash_command='python /opt/airflow/kafka_producer/weather_producer.py',
    dag=dag,
)
