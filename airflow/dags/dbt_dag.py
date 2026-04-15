from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

# =========================
# DEFAULT ARGS
# =========================
default_args = {
    'owner': 'retail_platform',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

# Paths
DBT_PROJECT_DIR = "/Users/narayanprasai/Documents/projects/retail-intelligence-platform/dbt"
DBT_PROFILES_DIR = "/Users/narayanprasai/.dbt"
DBT_VENV = "/Users/narayanprasai/Documents/projects/dbt-env/bin/dbt"

# =========================
# DAG DEFINITION
# =========================
with DAG(
    dag_id='dbt_transformation',
    default_args=default_args,
    description='dbt models on Redshift',
    schedule='0 4 * * *',  # 4am daily — after Glue jobs
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dbt', 'redshift', 'retail'],
) as dag:

    # Task 1 — dbt run
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"{DBT_VENV} run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
    )

    # Task 2 — dbt test
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f"{DBT_VENV} test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
    )

    # Dependencies
    dbt_run >> dbt_test