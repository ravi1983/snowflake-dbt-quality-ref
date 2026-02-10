import os
import logging
from typing import Any

from airflow.sdk import task, Variable

from airflow.operators.python import ShortCircuitOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.google.cloud.operators.pubsub import PubSubPullOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig


def _parse_files(messages: list, table) -> str:
    files = [
        f"'{m['message']['attributes']['objectId']}'"
        for m in messages if m.get('message', {}).get('attributes', {}).get('objectId')
    ]
    file_names = ",".join(files) if files else ""
    logging.info(f'Files to be loaded for {table}: {file_names}')

    return file_names


def poll_for_messages(subscription=os.environ.get("GCS_PRODUCT_SUBSCRIPTION")):
    return PubSubPullOperator(
        task_id="poll_for_messages",
        gcp_conn_id="google_cloud_default",
        project_id=os.environ.get("GCP_PROJECT"),
        subscription=subscription,
        max_messages=10,
        ack_messages=True
    )


def check_for_files(message):
    return ShortCircuitOperator(
        task_id="check_for_files",
        python_callable=lambda x: len(x) > 0,
        op_args=[message]
    )


@task
def copy_products_to_snowflake(messages):
    files = _parse_files(messages, 'products')

    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    return hook.run(
        f"""
        COPY INTO RAW.PRODUCTS
        FROM (
            SELECT
                $1 AS PRODUCT_ID,
                $2 AS PRODUCT_NAME,
                $3 AS CATEGORY,
                $4 AS BRAND,
                $5 AS PRICE,
                $6 AS RATING,
                SYSDATE() AS LOADED_AT
            FROM @RAW.ECOM_STAGE
        )
        FILE_FORMAT = RAW.CSV_FILE_FORMAT
        FILES = ({files});
        """
    )


@task
def copy_users_to_snowflake(**context):
    files = _parse_files(context, 'users')

    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    return hook.run(
        f"""
        COPY INTO RAW.USERS
        FROM (
            SELECT
                $1 AS USER_ID,
                $2 AS NAME,
                $3 AS EMAIL,
                $4 AS GENDER,
                $5 AS CITY,
                $6 AS SIGNUP_DATE,
                SYSDATE() AS LOADED_AT
            FROM @RAW.ECOM_STAGE
        )
        FILE_FORMAT = RAW.CSV_FILE_FORMAT
        FILES = ({files})
        """
    )


@task
def copy_orders_to_snowflake(**context):
    files = _parse_files(context, 'orders')

    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    return hook.run(
        f"""
        COPY INTO RAW.ORDERS
        FROM (
            SELECT
                $1 AS ORDER_ID,
                $2 AS USER_ID,
                $3 AS ORDER_DATE,
                $4 AS ORDER_STATUS,
                $5 AS TOTAL_AMOUNT,
                SYSDATE() AS LOADED_AT
            FROM @RAW.ECOM_STAGE
        )
        FILE_FORMAT = RAW.CSV_FILE_FORMAT
        FILES = ({files})
        """
    )


@task
def copy_order_items_to_snowflake(**context):
    files = _parse_files(context, 'order_items')

    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    return hook.run(
        f"""
        COPY INTO RAW.ORDER_ITEMS
        FROM (
            SELECT
                $1 AS ORDER_ITEM_ID,
                $2 AS ORDER_ID,
                $3 AS PRODUCT_ID,
                $4 AS USER_ID,
                $5 AS QUANTITY,
                $6 AS ITEM_PRICE,
                $6 AS ITEM_TOTAL,
                SYSDATE() AS LOADED_AT
            FROM @RAW.ECOM_STAGE
        )
        FILE_FORMAT = RAW.CSV_FILE_FORMAT
        FILES = ({files});
        """
    )


def dbt_transform(tags):
    logging.info(f'Running dbt transform for {tags}...')

    root_path = Variable.get('DBT_PROJECT')
    project_path = os.path.join(root_path, 'ecom_dbt')

    return DbtTaskGroup(
        group_id='dbt_transform_task',
        project_config=ProjectConfig(
            dbt_project_path=project_path,
            manifest_path=os.path.join(project_path, 'target/manifest.json')
        ),
        render_config=RenderConfig(
            select=tags
        ),
        profile_config=ProfileConfig(
            profile_name='ecom_dbt',
            profiles_yml_filepath=os.path.join(project_path, "profiles.yml"),
            target_name='dev'
        )
        # If separate venv is needed for dbt
        # execution_config=ExecutionConfig(
        #     dbt_executable_path=(os.path.join(root_path, '.venv/bin/dbt'))
        # )
    )
