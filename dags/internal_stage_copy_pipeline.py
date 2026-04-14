"""
Internal Snowflake Stage → Table → Internal Stage pipeline
"""

from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime


@dag(
    dag_id="internal_stage_copy_pipeline",
    start_date=datetime(2026, 4, 13),
    schedule=None,
    catchup=False,
    tags=["snowflake", "internal-stage", "copy"],
)
def internal_stage_copy_pipeline():

    # 1️⃣ Create table (raw landing table)
    create_table = SQLExecuteQueryOperator(
        task_id="create_raw_table",
        conn_id="snowflake_default",
        sql="""
        CREATE TABLE IF NOT EXISTS raw_users (
            user_id INT,
            user_name STRING,
            loaded_at TIMESTAMP
        );
        """
    )

    # 2️⃣ Load data from INTERNAL stage into table
    copy_stage_to_table = SQLExecuteQueryOperator(
        task_id="copy_internal_stage_to_table",
        conn_id="snowflake_default",
        sql="""
        COPY INTO raw_users
        FROM @raw_stage
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
        );
        """
    )

    # 3️⃣ Copy data from table to another INTERNAL stage
    copy_table_to_stage = SQLExecuteQueryOperator(
        task_id="copy_table_to_internal_stage",
        conn_id="snowflake_default",
        sql="""
        COPY INTO @archive_stage
        FROM raw_users
        FILE_FORMAT = (
            TYPE = CSV
        )
        OVERWRITE = TRUE;
        """
    )

    # ✅ Task dependencies
    create_table >> copy_stage_to_table >> copy_table_to_stage


internal_stage_copy_pipeline()