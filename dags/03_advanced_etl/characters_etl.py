from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
import requests

# Аргументы по умолчанию (единые для всех DAG'ов)
default_args = {
    "owner": "roman-mescherjakov",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    'start_date': days_ago(1),
}

def get_characters():
    """
    Extract: Получение данных о персонажах из Rick & Morty API
    Обрабатывает пагинацию для получения всех персонажей
    """
    try:
        char_list = []
        url = 'https://rickandmortyapi.com/api/character'
        
        while url:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            for character in data['results']:
                character_data = {
                    'id': character['id'],
                    'name': character['name'],
                    'status': character['status'],
                    'species': character['species'],
                    'type': character['type'],
                    'gender': character['gender']
                }
                char_list.append(character_data)
            
            url = data['info']['next']
        
        return char_list
        
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при получении данных: {e}")
        raise

def save_characters_to_postgres(**kwargs):
    """
    Transform & Load: Сохранение данных о персонажах в PostgreSQL
    Использует UPSERT для идемпотентности
    """
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try: 
        characters_data = get_characters()
        
        # Transform: подготовка данных для вставки
        records = [
            (char['id'], char['name'], char['status'], 
             char['species'], char['type'], char['gender'])
            for char in characters_data
        ]

        # Load: UPSERT операция
        insert_query = """
            INSERT INTO roman_mescherjakov.characters (
                id, name, status, species, type, gender
            ) 
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                status = EXCLUDED.status,
                species = EXCLUDED.species,
                type = EXCLUDED.type,
                gender = EXCLUDED.gender
        """
        
        cursor.executemany(insert_query, records)
        conn.commit()
        print(f"Успешно обработано {len(records)} персонажей")

    except Exception as e:
        conn.rollback()
        print(f"Ошибка при сохранении данных: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# определение DAG
with DAG(
    "characters_etl",
    default_args=default_args,
    description="ETL пайплайн для загрузки персонажей Rick & Morty в PostgreSQL",
    schedule_interval="@daily",
    max_active_runs=1,
    concurrency=1,
    tags=["rickandmorty", "etl", "postgresql", "advanced"],
    catchup=False,
) as dag:
    
    create_table = PostgresOperator(
        task_id="create_characters_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS roman_mescherjakov.characters (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255),
                status VARCHAR(50), 
                species VARCHAR(100),
                type VARCHAR(100),
                gender VARCHAR(50)
            )
        """,
    )

    truncate_table = PostgresOperator(
        task_id="truncate_characters_table",
        postgres_conn_id="postgres_default",
        sql="TRUNCATE TABLE roman_mescherjakov.characters",
    )

    load_data = PythonOperator(
        task_id="load_characters_data",
        python_callable=save_characters_to_postgres,
    )

    # Зависимости
    create_table >> truncate_table >> load_data