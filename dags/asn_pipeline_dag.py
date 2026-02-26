from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

from enrich.peering_db import enrich_asn
from extract.fetch_delegated import fetch_asn
from load.load_postgres import load_asn

with DAG(
    dag_id="asn_pipeline",
    start_date=datetime(2026, 2, 25),
    schedule="@daily",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_asn",
        python_callable=fetch_asn,
        retries=0,
        retry_delay=timedelta(minutes=1),
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_asn,
        retries=0,
        retry_delay=timedelta(minutes=1),
    )
    enrich_task = PythonOperator(
    task_id="enrich_asn",
    python_callable=enrich_asn,
    retries=0,
    retry_delay=timedelta(minutes=1),
)
    extract_task >> enrich_task