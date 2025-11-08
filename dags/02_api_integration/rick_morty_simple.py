from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests

# URL для получения данных о персонажах Rick & Morty
URL = "https://rickandmortyapi.com/api/character"

# Аргументы по умолчанию
default_args = {
    "owner": "roman-mescherjakov",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Описание DAG
dag = DAG(
    "rick_morty_simple",
    default_args=default_args,
    description="DAG for Rick & Morty API - get character count",
    schedule_interval="@daily",
    start_date=days_ago(2),
    tags=["api", "rickandmorty"],
    max_active_tasks=1,
)

# Таск: Получение количества персонажей
def get_character_count():
    response = requests.get(URL)      
    data = response.json()
    count = data["info"]["count"]
    return count

print_count = PythonOperator(
    task_id='get_character_count',
    python_callable=get_character_count,
    dag=dag
)