from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_astrocloud_dag',
    default_args=default_args,
    description='A simple test DAG for AstroCloud',
    schedule_interval=timedelta(days=1),
)

def print_hello():
    print("Hello from AstroCloud!")

def print_date():
    print(f"Current date is: {datetime.now()}")

t1 = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

t2 = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag,
)

t1 >> t2