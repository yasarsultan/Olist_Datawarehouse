from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'Admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 5),
    'retries': 1,
}

with DAG("etl_dag", default_args=default_args, schedule_interval="0 0 1 */2 *") as dag:
    preprocess = BashOperator(
        task_id="run_preprocess",
        bash_command='python /scripts/preprocess.py'
    )

    staging_load = BashOperator(
        task_id="run_staging_pipeline",
        bash_command='python /scripts/staging_pipeline.py'
    )

    production_load = BashOperator(
        task_id="final_load",
        bash_command='python /scripts/production_pipeline.py'
    )

    # Task dependencies
    preprocess >> staging_load >> production_load
