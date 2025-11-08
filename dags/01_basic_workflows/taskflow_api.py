from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

# Аргументы по умолчанию
default_args = {
    "owner": "roman-mescherjakov",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="taskflow_api",
    default_args=default_args,
    description="DAG using TaskFlow API",
    schedule_interval="@daily",
    start_date=days_ago(2),
    tags=["basic", "taskflow"],
)
def task_flow():
    @task()
    def get_date():
        return 'Hello World'
    
    get_date()

task_flow()