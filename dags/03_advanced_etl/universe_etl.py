from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import requests

# Аргументы по умолчанию
default_args = {
    "owner": "roman-mescherjakov",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    'start_date': days_ago(1),
}

def get_api_data(api_url, columns):
    try:
        result_list = []
        url = api_url
        
        while url:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            # Extract: извлечение нужных полей
            for item in data['results']:
                item_data = {}
                for col in columns:
                    item_data[col] = item.get(col, None)
                result_list.append(item_data)
            
            url = data['info'].get('next')

        return result_list
        
    except requests.exceptions.RequestException as e:
        print(f"Ошибка API: {e}")
        raise

def save_to_postgres(fixed_list, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try: 
        api_data = get_api_data(fixed_list['url'], fixed_list['columns'])
        
        # Transform: подготовка данных
        records = []
        for item in api_data:
            record = tuple(item[column] for column in fixed_list['columns'])
            records.append(record)

        # Динамическое создание SQL запроса
        placeholders = ', '.join(['%s'] * len(fixed_list['columns']))
        columns_str = ', '.join(fixed_list['columns'])
        
        # UPDATE часть для UPSERT (все поля кроме ID)
        update_columns = ', '.join([
            f"{col} = EXCLUDED.{col}" 
            for col in fixed_list['columns'] 
            if col != 'id'
        ])
        
        insert_query = f"""
            INSERT INTO {fixed_list['table_name']} ({columns_str}) 
            VALUES ({placeholders})
            ON CONFLICT (id) DO UPDATE SET
                {update_columns}
        """
        
        cursor.executemany(insert_query, records)
        conn.commit()
        print(f"Успешно загружено {len(records)} записей в {fixed_list['table_name']}")

    except Exception as e:
        conn.rollback()
        print(f"Ошибка при загрузке в {fixed_list['table_name']}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# Конфигурация API
API_CONFIGS = [
    {
        'name': 'characters',
        'url': 'https://rickandmortyapi.com/api/character',
        'table_name': "roman_mescherjakov.characters",
        'ddl': """
            CREATE TABLE IF NOT EXISTS roman_mescherjakov.characters (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255),
                status VARCHAR(50), 
                species VARCHAR(100),
                type VARCHAR(100),
                gender VARCHAR(50)
            )
        """,
        'columns': ['id', 'name', 'status', 'species', 'type', 'gender']
    },
    {
        'name': 'locations',
        'url': 'https://rickandmortyapi.com/api/location',
        'table_name': "roman_mescherjakov.locations",
        'ddl': """
            CREATE TABLE IF NOT EXISTS roman_mescherjakov.locations (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255),
                type VARCHAR(100),
                dimension VARCHAR(100)
            )
        """,
        'columns': ['id', 'name', 'type', 'dimension']
    },
    {
        'name': 'episodes', 
        'url': 'https://rickandmortyapi.com/api/episode',
        'table_name': "roman_mescherjakov.episodes",
        'ddl': """
            CREATE TABLE IF NOT EXISTS roman_mescherjakov.episodes (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255),
                air_date VARCHAR(50),
                episode VARCHAR(20)
            )
        """,
        'columns': ['id', 'name', 'air_date', 'episode']
    }
]

# описание DAG
with DAG(
    "universe_etl",
    default_args=default_args,
    description="Комплексный ETL пайплайн для всей вселенной Rick & Morty",
    schedule_interval="@daily", 
    max_active_runs=1,
    concurrency=1,
    tags=["rickandmorty", "etl", "postgresql", "advanced", "taskgroup"],
    catchup=False,
) as dag:

    # Создание TaskGroup
    for api_config in API_CONFIGS:
        with TaskGroup(group_id=api_config['name']) as entity_group:

            create_table = PostgresOperator(
                task_id=f"create_{api_config['name']}_table",
                postgres_conn_id="postgres_default",
                sql=api_config['ddl']
            )

            truncate_table = PostgresOperator(
                task_id=f"truncate_{api_config['name']}_table",
                postgres_conn_id="postgres_default",
                sql=f"TRUNCATE TABLE {api_config['table_name']}",
            )

            load_data = PythonOperator(
                task_id=f"load_{api_config['name']}_data",
                python_callable=save_to_postgres,
                op_kwargs={'fixed_list': api_config},
            )

            # Зависимости
            create_table >> truncate_table >> load_data