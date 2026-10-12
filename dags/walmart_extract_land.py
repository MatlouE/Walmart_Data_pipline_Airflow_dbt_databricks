"""Phase 3: extract changed rows from the source Postgres and land them in the UC Volume.

Flow:
  init_control_table  ->  extract_land_<table>  (one task per table, run in parallel)

Each extract task reads its watermark, lands one Parquet file for the rows changed
since then, and only then advances the watermark. The file name is built from the
run's logical date ({{ ts_nodash }}), so a retry overwrites the same file instead
of duplicating it.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.extract_land import TABLES, extract_and_land, init_control_table

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,          # 1 min, then roughly 2 min, ...
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),  # a hung upload fails instead of hanging forever
}

with DAG(
    dag_id="walmart_extract_land",
    description="Postgres -> Parquet -> Unity Catalog Volume (watermark on updated_timestamp)",
    doc_md=__doc__,
    start_date=datetime(2026, 1, 1),
    schedule=None,  # manual trigger while learning; switch to "@hourly" later
    catchup=False,
    max_active_runs=1,  # two overlapping runs would read the same watermark and land duplicates
    default_args=default_args,
    tags=["walmart", "bronze"],
) as dag:
    # Create etl_control.watermark ONCE, before the parallel extracts start,
    # so six tasks never race on CREATE SCHEMA / CREATE TABLE.
    init_control = PythonOperator(
        task_id="init_control_table",
        python_callable=init_control_table,
    )

    for table in TABLES:
        extract = PythonOperator(
            task_id=f"extract_land_{table}",
            python_callable=extract_and_land,
            # op_kwargs is templated: the file name is tied to the run, so a retry overwrites, never duplicates.
            # Named run_label (not run_id) so it cannot collide with Airflow's own run_id context variable.
            op_kwargs={"table": table, "run_label": "{{ ts_nodash }}"},
        )
        init_control >> extract