from datetime import datetime

from airflow.sdk import dag
import tasks.ecom_tasks as et

@dag(
    dag_id="products_ingestion_pipeline",
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    catchup=False
)
def products_ingestion_pipeline():
    et.copy_products_to_snowflake() >> et.dbt_transform(['tag:products'])

products_ingestion_pipeline()