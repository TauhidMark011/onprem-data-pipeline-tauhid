from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'tauhid',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='batch_etl_dag',
    default_args=default_args,
    description='Trigger Spark batch ETL job to join weather and device data',
    schedule_interval='@once',  #Or'@once' for one-time test
    start_date=datetime(2025, 6, 20),
    catchup=False,
    tags=['batch', 'spark', 'etl']
) as dag:

    run_batch_etl = BashOperator(
    task_id='run_spark_batch_etl',
    bash_command="""
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages mysql:mysql-connector-java:8.0.33 \
    /opt/spark-app/spark_batch_etl.py
    """
)

    run_batch_etl
