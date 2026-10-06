"""Phase 3: extract changed rows from the source Postgres and land them in the UC Volume."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.extract_land import TABLES, extract_and_land

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="walmart_extract_land",
    description="Postgres -> Parquet -> Unity Catalog Volume (watermark on updated_timestamp)",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # manual trigger while learning; switch to "@hourly" later
    catchup=False,
    default_args=default_args,
    tags=["walmart", "bronze"],
) as dag:
    for table in TABLES:
        PythonOperator(
            task_id=f"extract_land_{table}",
            python_callable=extract_and_land,
            # op_kwargs is templated: the file name is tied to the run, so a retry overwrites, never duplicates
            op_kwargs={"table": table, "run_id": "{{ ts_nodash }}"},
        )