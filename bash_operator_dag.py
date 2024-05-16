from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    "owner": 'Pramita',
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}

with DAG(
    dag_id = "bash_operator_example_dag",
    description = "Trying out bash operator",
    start_date =datetime(2024,5,15,2),
    schedule_interval = '@daily',
    default_args=default_args
) as dag:
    
    task_1 = BashOperator(
        task_id = "Execute_Name_1",
        bash_command = "echo deril"
    )

    task_2 = BashOperator(
        task_id = "Execute_Name_2",
        bash_command = "echo pramita"
    )

    task_3 = BashOperator(
        task_id = "Execute_Name_3",
        bash_command = "echo derilpramita"
    )

    task_1 >> [task_2, task_3]