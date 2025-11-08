from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pendulum

# Аргументы по умолчанию
default_args = {
    "owner": "roman-mescherjakov",
    'depends_on_past': False,
    'start_date': days_ago(7),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def check_weekday_and_branch(**kwargs):
    """
    Проверяет, является ли execution_date будним днем и возвращает соответствующий task_id
    Если будний день - возвращает task_id для отправки в XCom
    Если выходной - возвращает None (пропускает следующий таск)
    """
    execution_date = kwargs['execution_date']
    
    # Приводим execution_date к объекту datetime с учетом временной зоны
    if isinstance(execution_date, str):
        exec_dt = pendulum.parse(execution_date)
    else:
        exec_dt = execution_date
    
    # Проверяем день недели (0-пн, 1-вт, 2-ср, 3-чт, 4-пт, 5-сб, 6-вс)
    if exec_dt.weekday() < 5:  # 0-4 = понедельник-пятница
        return 'send_execution_date_task'
    else:
        # Возвращаем None, чтобы пропустить следующий таск
        return None

def send_execution_date(**kwargs):
    """Отправляет execution_date в XCom"""
    execution_date = kwargs['execution_date']
    task_instance = kwargs['ti']
    
    # Форматируем дату для удобства чтения
    formatted_date = execution_date.strftime('%Y-%m-%d %H:%M:%S')
    
    # Отправляем в XCom
    task_instance.xcom_push(key='execution_date', value=formatted_date)
    
    print(f"Execution date {formatted_date} отправлен в XCom")
    return f"Execution date {formatted_date} успешно отправлен"

with DAG(
    dag_id='conditional_execution',
    default_args=default_args,
    description='DAG with conditional execution based on weekday',
    schedule_interval=timedelta(days=1),
    catchup=True,
    max_active_runs=7,
    tags=['branch', 'weekday_check'],
) as dag:

    # Первый таск - Branch оператор для проверки дня недели
    check_weekday_task = BranchPythonOperator(
        task_id='check_weekday_task',
        python_callable=check_weekday_and_branch,
        provide_context=True,
    )

    # Второй таск - отправляет execution_date в XCom (выполняется только в будни)
    send_execution_date_task = PythonOperator(
        task_id='send_execution_date_task',
        python_callable=send_execution_date,
        provide_context=True,
    )

    # Определяем зависимости - только 2 таска в DAG
    check_weekday_task >> send_execution_date_task

# Документация
check_weekday_task.doc_md = """
### Check Weekday Task

Этот Branch оператор проверяет, является ли execution_date будним днем:
- Понедельник-пятница (0-4): возвращает 'send_execution_date_task' для выполнения
- Суббота-воскресенье (5-6): возвращает None, пропуская следующий таск
"""

send_execution_date_task.doc_md = """
### Send Execution Date Task

Отправляет execution_date в XCom для использования в последующих тасках.
Выполняется только в будние дни благодаря Branch оператору.
"""