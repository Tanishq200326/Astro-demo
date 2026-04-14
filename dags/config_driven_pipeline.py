"""
Config-driven GitHub → Snowflake pipeline
"""

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime
import requests
import yaml
import csv
import tempfile


@dag(
    dag_id="config_driven_pipeline",
    start_date=datetime(2026, 4, 13),
    schedule=None,
    catchup=False,
    tags=["config", "github", "snowflake"],
)
def config_driven_pipeline():

    # 1️⃣ Read config from GitHub
    @task
    def read_config():
        config_url = "https://raw.githubusercontent.com/<YOUR_USERNAME>/<YOUR_REPO>/main/config/pipeline_config.yaml"
        response = requests.get(config_url)
        response.raise_for_status()
        return yaml.safe_load(response.text)

    # 2️⃣ Download CSV defined in config
    @task
    def download_csv(config: dict):
        csv_url = config["dataset"]["csv_url"]
        response = requests.get(csv_url)
        response.raise_for_status()

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        tmp.write(response.content)
        tmp.close()
        return tmp.name

    # 3️⃣ Convert CSV to SQL VALUES
    @task
    def build_sql_values(csv_path: str):
        values = []
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                values.append(f"({row['user_id']}, '{row['user_name']}')")
        return ",\n".join(values)

    # 4️⃣ Create table (Snowflake)
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="snowflake_default",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            user_id INT,
            user_name STRING,
            loaded_at TIMESTAMP
        );
        """
    )

    # 5️⃣ Load data
    load_data = SQLExecuteQueryOperator(
        task_id="load_data",
        conn_id="snowflake_default",
        sql="""
        {% set values = ti.xcom_pull(task_ids='build_sql_values') %}
        INSERT INTO users (user_id, user_name, loaded_at)
        VALUES
        {{ values }};
        """
    )

    config = read_config()
    csv_file = download_csv(config)
    values = build_sql_values(csv_file)

    config >> csv_file >> values >> create_table >> load_data


config_driven_pipeline()
