from datetime import datetime

from airflow.decorators import dag
from airflow.operators.python import PythonOperator

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata

from include.notaries.tasks import scrap_all_notaries, transform_notaries_data, prepare_for_db_upload

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
    default_args=default_args
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

    prepare_for_upload = PythonOperator(
        task_id='prepare_for_upload',
        python_callable=prepare_for_db_upload
    )

    load_to_db = aql.load_file(
        task_id='load_to_dw',
        input_file=File(
            path=f's3://notaries-bucket/loading_into_db/transformed_notaries_comma.csv',
            conn_id='minio',
        ),
        output_table=Table(
            name='test_notariusze',
            conn_id='postgres_notaries',
            metadata=Metadata(
                schema='public'
            )
        ),
    )

    scrap_notaries >> transform_notaries >> prepare_for_upload >> load_to_db


notaries()
