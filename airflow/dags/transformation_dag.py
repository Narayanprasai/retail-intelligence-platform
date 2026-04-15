from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import time

# =========================
# DEFAULT ARGS
# =========================
default_args = {
    'owner': 'retail_platform',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

# =========================
# HELPER — Run Glue Job
# =========================
def run_glue_job(job_name: str):
    """Trigger a Glue job and wait for completion"""
    client = boto3.client('glue', region_name='ap-southeast-2')

    # Start job
    response = client.start_job_run(JobName=job_name)
    run_id = response['JobRunId']
    print(f"Started {job_name} — run_id: {run_id}")

    # Wait for completion
    while True:
        response = client.get_job_run(
            JobName=job_name,
            RunId=run_id
        )
        state = response['JobRun']['JobRunState']
        print(f"{job_name} state: {state}")

        if state == 'SUCCEEDED':
            print(f"✅ {job_name} succeeded")
            return run_id
        elif state in ['FAILED', 'ERROR', 'TIMEOUT']:
            error = response['JobRun'].get('ErrorMessage', 'Unknown error')
            raise Exception(f"❌ {job_name} failed: {error}")
        else:
            time.sleep(30)

# =========================
# DAG DEFINITION
# =========================
with DAG(
    dag_id='transformation',
    default_args=default_args,
    description='Glue PySpark transformation jobs',
    schedule='0 3 * * *',  # 3am daily — after ingestion
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['transformation', 'glue', 'retail'],
) as dag:

    # Task 1 — CSV to Parquet
    csv_to_parquet = PythonOperator(
        task_id='csv_to_parquet',
        python_callable=lambda: run_glue_job('retail-platform-csv-to-parquet'),
    )

    # Task 2 — Star Schema Builder
    star_schema = PythonOperator(
        task_id='star_schema_builder',
        python_callable=lambda: run_glue_job('retail-platform-star-schema-builder'),
    )

    # Dependencies — star schema runs after csv to parquet
    csv_to_parquet >> star_schema