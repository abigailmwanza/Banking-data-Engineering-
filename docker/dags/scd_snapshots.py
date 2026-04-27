# Airflow DAG that orchestrates dbt staging -> snapshot -> marts for SCD2 tracking.
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Defaults applied to every task in the DAG (retry once after a 1-minute wait).
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="SCD2_snapshots",
    default_args=default_args,
    description="Run dbt snapshots for SCD2",
    schedule_interval="@daily",     # or "@hourly" depending on your needs
    start_date=datetime(2025, 9, 1),
    catchup=False,                  # only run going forward, skip historical backfills
    tags=["dbt", "snapshots"],
) as dag:

    # Step 1: refresh staging models so snapshots see the latest source data.
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/banking_dbt_v2 && dbt run --select staging --profiles-dir /home/airflow/.dbt",
    )

    # Step 2: capture point-in-time history of staging tables (SCD2).
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/airflow/banking_dbt_v2 && dbt snapshot --profiles-dir /home/airflow/.dbt",
    )

    # Step 3: build marts on top of the freshly snapshotted history.
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/banking_dbt_v2 && dbt run --select marts --profiles-dir /home/airflow/.dbt",
    )

    # Task order: staging must finish before snapshot, snapshot before marts.
    dbt_run_staging >> dbt_snapshot >> dbt_run_marts