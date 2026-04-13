"""
Load CSV from GitHub into Snowflake
"""

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime
import requests
import tempfile
import csv


@dag(
    dag_id="github_csv_to_snowflake",
    start_date=datetime(2026, 4, 13),
    schedule=None,
    catchup=False,
    tags=["github", "csv", "snowflake"],
)
def github_csv_to_snowflake():
    # 1️⃣ Download CSV from GitHub
    @task
    def download_csv():
        url = "https://raw.githubusercontent.com/Tanishq200326/Astro-demo/tree/main/data/users.csv"

        response = requests.get(url)
        response.raise_for_status()

        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            temp_file.write(response.content)

        return temp_file.name

    # 2️⃣ Parse CSV and create SQL VALUES
    @task
    def parse_csv(csv_path: str):
        rows = []

        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(f"({row['user_id']}, '{row['user_name']}')")

        return ",\n".join(rows)

    # 3️⃣ Create table in Snowflake
    create_table = SQLExecuteQueryOperator(
        task_id="create_users_table",
        conn_id="snowflake_default",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            user_id INT,
            user_name STRING,
            loaded_at TIMESTAMP
        );
        """,
    )

    # 4️⃣ Load data into Snowflake
    load_data = SQLExecuteQueryOperator(
        task_id="load_csv_data",
        conn_id="snowflake_default",
        sql="""
        {% set values = ti.xcom_pull(task_ids='parse_csv') %}
        INSERT INTO users (user_id, user_name, loaded_at)
        VALUES
        {{ values }};
        """,
    )

    csv_file = download_csv()
    parsed_rows = parse_csv(csv_file)

    csv_file >> parsed_rows >> create_table >> load_data


github_csv_to_snowflake()
