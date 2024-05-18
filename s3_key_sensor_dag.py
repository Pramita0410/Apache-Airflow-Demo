"""
https://registry.astronomer.io/providers/amazon/versions/latest/modules/s3keysensor
"""
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.operators.email import EmailOperator

default_args = {
    "owner": 'Pramita',
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}


with DAG(
    dag_id = "s3_sensor_example_dag",
    description = "Trying out s3 sensor",
    start_date =datetime(2024,5,15,2),
    schedule_interval = '@daily',
    default_args=default_args
) as dag:

    task_1 = S3KeySensor(
        task_id= "check_file_on_S3",
        bucket_name='my-airflow-key-sensor-bucket',
        bucket_key="data.csv",
        aws_conn_id='my-aws-conn',
        poke_interval=30,
        timeout=60*5,
        mode= "reschedule",
        soft_fail=True
    )


    task_2 = EmailOperator(
        task_id= "send_email_when_file_exists",
        to="sandhyan.p@northeastern.edu",
        cc="raju.d@northeastern.edu",
        subject="Your s3 file exists-checked by Airflow",
        html_content="<h2> Congrats! </ h2>"
    )


    task_1 >> task_2