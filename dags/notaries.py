from datetime import datetime

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from include.notaries.tasks import scrap_all_notaries, transform_notaries_data

default_args = {
    'email': 'a.grzesik@wasko.pl',
    'email_on_failure': True,
    'email_on_retry': True
}


@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@weekly',
    catchup=False,
    tags=['notaries'],
    # default_args=default_args
)
def notaries():
    scrap_notaries = PythonOperator(
        task_id='scrap_notaries',
        python_callable=scrap_all_notaries,

    )

    transform_notaries = PythonOperator(
        task_id='transform_notaries',
        python_callable=transform_notaries_data
    )

    load_to_db = EmptyOperator(
        task_id='load_to_db'
    )

    scrap_notaries >> transform_notaries >> load_to_db


notaries()
