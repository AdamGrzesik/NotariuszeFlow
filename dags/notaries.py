from datetime import datetime

from airflow.decorators import dag
from airflow.operators.python import PythonOperator

from include.notaries.tasks import scrap_all_notaries


@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@weekly',
    catchup=False,
    tags=['notaries']
)
def notaries():
    scrap_notaries = PythonOperator(
        task_id='test_task',
        python_callable=scrap_all_notaries
    )


notaries()
