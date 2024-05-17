"""
https://registry.astronomer.io/providers/apache-airflow/versions/latest/modules/filesensor
"""
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.operators.email import EmailOperator

default_args = {
    "owner": 'Pramita',
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}


def read_csv():
    df =  pd.read_csv(Variable.get("airflow_data_path", default_var=None) + "data.csv")
    print(df)

with DAG(
    dag_id = "file_sensor_example_dag",
    description = "Trying out file sensor",
    start_date =datetime(2024,5,15,2),
    schedule_interval = '@daily',
    default_args=default_args
) as dag:

    task_1 = FileSensor(
        task_id= "check_file",
        filepath=Variable.get("airflow_data_path", default_var=None) + "data.csv",
        poke_interval=30,
        timeout=60*5,
        mode= "reschedule",
        soft_fail=True
    )


    task_2 = EmailOperator(
        task_id= "send_email_when_file_exists",
        to="sandhyan.p@northeastern.edu",
        cc="raju.d@northeastern.edu",
        subject="Your file exists-checked by Airflow",
        html_content="<h2> COngrats! </ h2>"
    )


    task_3= PythonOperator(
        task_id="read_csv",
        python_callable=read_csv
    )

    task_1 >> [task_2, task_3]