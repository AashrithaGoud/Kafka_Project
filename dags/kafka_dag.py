import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
sys.path.append('/opt/airflow')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.producer import kafka_stream
from src.consumer import kafka_consumer

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'schedule_interval': '*/5 * * * *'
}

with DAG(dag_id='kafka_dag', default_args=default_args) as dag:

    produce_kafka_data=PythonOperator(
        task_id='produce_kafka_data',
        python_callable=kafka_stream,
        provide_context=True,
        dag=dag
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=kafka_consumer,
        provide_context=True,
        dag=dag
    )
    produce_kafka_data >> load_data







