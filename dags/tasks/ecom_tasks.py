from airflow.sdk import task

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

@task
def copy_to_snowflake_task():
    pass