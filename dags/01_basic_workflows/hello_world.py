from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Аргументы по умолчанию
default_args = {
    "owner": "roman-mescherjakov",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Описание DAG
dag = DAG(
    "hello_world",
    default_args=default_args,
    description="Basic Hello World DAG",
    schedule_interval="@daily",
    start_date=days_ago(2),
    tags=["basic", "hello-world"],
)

# Таск 1: Вывод приветствия
def print_hello_func():
    return "Hello, World!"

print_hello = PythonOperator(
    task_id="print_hello",
    python_callable=print_hello_func,
    dag=dag
)