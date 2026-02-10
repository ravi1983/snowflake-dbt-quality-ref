import os
from datetime import datetime

from airflow.sdk import dag
import tasks.ecom_tasks as et

@dag(
    dag_id="products_ingestion_pipeline",
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    catchup=False,
    schedule='*/10 * * * *'
)
def products_ingestion_pipeline():
    pull_task = et.poll_for_messages(os.environ.get("GCS_PRODUCT_SUBSCRIPTION"))
    check_task = et.check_for_files(pull_task.output)
    copy_task = et.copy_products_to_snowflake(pull_task.output)

    pull_task >> check_task >> copy_task >> et.dbt_transform(['tag:products'])

products_ingestion_pipeline()