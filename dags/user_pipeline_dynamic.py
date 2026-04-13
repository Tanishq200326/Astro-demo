"""
Dynamic user pipeline – Python to Snowflake
"""

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime


@dag(
    dag_id="user_pipeline_dynamic",
    start_date=datetime(2026, 4, 13),
    schedule=None,
    catchup=False,
    tags=["practice", "python", "snowflake"],
)
def user_pipeline_dynamic():

    @task
    def generate_users():
        users = [
            {"user_id": 1, "user_name": "Alice"},
            {"user_id": 2, "user_name": "Bob"},
            {"user_id": 3, "user_name": "Charlie"},
        ]
        return users

    create_users_table = SQLExecuteQueryOperator(
        task_id="create_users_table",
        conn_id="snowflake_default",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            user_id INT,
            user_name STRING,
            updated_at TIMESTAMP
        );
        """
    )

    load_users = SQLExecuteQueryOperator(
        task_id="load_users",
        conn_id="snowflake_default",
        sql="""
        {% set users = ti.xcom_pull(task_ids='generate_users') %}
        {% for user in users %}
        MERGE INTO users t
        USING (
            SELECT {{ user.user_id }} AS user_id,
                   '{{ user.user_name }}' AS user_name
        ) s
        ON t.user_id = s.user_id
        WHEN MATCHED THEN
          UPDATE SET
            t.user_name = s.user_name,
            t.updated_at = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
          INSERT (user_id, user_name, updated_at)
          VALUES (s.user_id, s.user_name, CURRENT_TIMESTAMP);
        {% endfor %}
        """
    )

    users = generate_users()
    users >> create_users_table >> load_users


user_pipeline_dynamic()