from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": 'Pramita',
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}

def fetch_employees_table(ti):
    table = ti.xcom_pull(task_ids = "read_employees_table", key = "return_value")
    # print(table)
    # print(type(table))

    for el in table:
        name =  el[1]
        email = el[3]
        print(f'My name is {name} and my email is {email}')


with DAG(
    dag_id = "mysql_operator_example_dag",
    description = "Trying out mysql operator",
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

    task_1 >> task_2