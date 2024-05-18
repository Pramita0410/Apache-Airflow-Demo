from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os

default_args = {
    "owner": 'Pramita',
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}

def fetch_employees_table(ti):
    table = ti.xcom_pull(task_ids = "read_employees_table", key = "return_value")
    
    with open('employees.txt', 'w') as file:
        for row in table:
            line = ','.join(map(str, row))
            file.write(line + '\n')

def push_to_s3():
    s3_hook = S3Hook(aws_conn_id="my-aws-conn")
    s3_hook.load_file(
        filename='employees.txt',
        key='employees.txt',
        bucket_name='my-airflow-key-sensor-bucket',
        replace=True
    )

def delete_file_from_local():
    os.remove('employees.txt')

with DAG(
    dag_id = "mysql_to_s3_push_dag",
    description = "Trying out above",
    start_date =datetime(2024,5,15,2),
    schedule_interval = '@daily',
    default_args=default_args
) as dag:
    
    task_1 = MySqlOperator(
        task_id = "read_employees_table",
        sql = "SELECT * FROM employees",
        mysql_conn_id = 'mysql-localhost'
    )

    task_2 = PythonOperator(
        task_id = "fetch_employees_table",
        python_callable=fetch_employees_table
    )

    task_3 =PythonOperator(
        task_id='read_txt_and_push_to_S3',
        python_callable=push_to_s3
    )

    task_4 = PythonOperator(
        task_id="delete_file_from_local",
        python_callable=delete_file_from_local
    )

    task_1 >> task_2 >> task_3 >> task_4