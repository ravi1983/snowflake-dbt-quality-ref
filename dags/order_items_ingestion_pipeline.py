from datetime import datetime

from airflow.sdk import dag
import tasks.ecom_tasks as et

@dag(
    dag_id="order_items_ingestion_pipeline",
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    catchup=False
)
def order_items_ingestion_pipeline():
    et.copy_order_items_to_snowflake() >> et.dbt_transform(['tag:orders'])

order_items_ingestion_pipeline()