"""
Snowflake Warehouse Usage Monitoring Pipeline
"""

from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime


@dag(
    dag_id="snowflake_warehouse_usage_pipeline",
    start_date=datetime(2026, 4, 13),
    schedule=None,       # later you can make this daily
    catchup=False,
    tags=["snowflake", "monitoring", "audit"],
)
def snowflake_warehouse_usage_pipeline():

    # 1️⃣ Create audit table
    create_audit_table = SQLExecuteQueryOperator(
        task_id="create_audit_table",
        conn_id="snowflake_default",
        sql="""
        CREATE TABLE IF NOT EXISTS warehouse_usage_audit (
            warehouse_name STRING,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            credits_used NUMBER,
            extracted_at TIMESTAMP
        );
        """
    )

    # 2️⃣ Capture warehouse usage metrics
    capture_usage = SQLExecuteQueryOperator(
        task_id="capture_warehouse_usage",
        conn_id="snowflake_default",
        sql="""
        INSERT INTO warehouse_usage_audit
        SELECT
            warehouse_name,
            start_time,
            end_time,
            credits_used,
            CURRENT_TIMESTAMP
        FROM snowflake.account_usage.warehouse_metering_history
        WHERE start_time >= DATEADD(day, -1, CURRENT_TIMESTAMP);
        """
    )

    create_audit_table >> capture_usage


snowflake_warehouse_usage_pipeline()
