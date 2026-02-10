import os
from datetime import datetime

from airflow.sdk import dag
import tasks.ecom_tasks as et

@dag(
    dag_id="orders_ingestion_pipeline",
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    catchup=False,
    schedule='*/10 * * * *'
)
def orders_ingestion_pipeline():
    pull_task = et.poll_for_messages(os.environ.get("GCS_ORDER_SUBSCRIPTION"))
    check_task = et.check_for_files(pull_task.output)
    copy_task = et.copy_orders_to_snowflake(pull_task.output)

    pull_task >> check_task >> copy_task >> et.dbt_transform(['tag:orders'])

orders_ingestion_pipeline()