from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

default_args = {
    "owner": 'Pramita',
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}

def say_my_name():
    print("Walter_White")

def say_my_details(age, address, profession):
    print(f'My age is {age} and I reside in {address}. I work as a {profession}') 

def say_my_spouse_name(ti):
    # max size of xcom is 48KB
    ti.xcom_push(key= "name", value= "Skylar")
    ti.xcom_push(key= "age", value= "43")
    ti.xcom_push(key= "profession", value= "House wife")

def who_is_my_wife(ti):
    name = ti.xcom_pull(task_ids = "say_my_spouse_name", key = "name")
    age = ti.xcom_pull(task_ids = "say_my_spouse_name", key = "age")
    profession = ti.xcom_pull(task_ids = "say_my_spouse_name", key = "profession")
    print(f"My wife's name is {name}. She is {age} years old. She is a {profession}")

def check_pandas_working():
    print(pd.__version__)

with DAG(
    dag_id = "python_operator_example_dag",
    description = "Trying out python operator",
    start_date =datetime(2024,5,15,2),
    schedule_interval = '@daily',
    default_args=default_args
) as dag:
    task_1 = PythonOperator(
        task_id = "say_my_name",
        python_callable=say_my_name
    )

    task_2 = PythonOperator(
        task_id = "say_my_details",
        python_callable=say_my_details,
        op_kwargs= {"address": "Albuquerque, New Mexico", "age": "45", "profession": "Teacher"}
    )

    task_3 = PythonOperator(
        task_id = "say_my_spouse_name",
        python_callable=say_my_spouse_name
    )

    task_4 = PythonOperator(
        task_id = "who_is_my_wife",
        python_callable=who_is_my_wife
    )

    task_5 = PythonOperator(
        task_id = "check_pandas_working",
        python_callable=check_pandas_working
    )

    task_1 >> task_2 >> task_3 >> task_4 >> task_5