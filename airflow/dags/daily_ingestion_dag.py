from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import boto3
import sys
import os

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
# DAG DEFINITION
# =========================
with DAG(
    dag_id='daily_ingestion',
    default_args=default_args,
    description='Daily ingestion of Olist data to S3 raw zone',
    schedule='0 2 * * *',  # 2am daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ingestion', 'retail'],
) as dag:

    # =========================
    # TASK 1 — Run ingestion script
    # =========================
    def run_ingestion():
        """Run config-driven ingestion script"""
        import subprocess
        project_path = "/Users/narayanprasai/Documents/projects/retail-intelligence-platform"
        result = subprocess.run(
            ["python", f"{project_path}/ingestion/ingest.py"],
            capture_output=True,
            text=True,
            cwd=f"{project_path}/ingestion"
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f"Ingestion failed: {result.stderr}")
        return "Ingestion complete"

    ingestion_task = PythonOperator(
        task_id='run_ingestion',
        python_callable=run_ingestion,
    )

    # =========================
    # TASK 2 — Run Glue Crawler
    # =========================
    def run_crawler():
        """Trigger Glue crawler to update catalog"""
        client = boto3.client('glue', region_name='ap-southeast-2')

        # Start crawler
        client.start_crawler(Name='retail-platform-raw-crawler')

        # Wait for completion
        import time
        while True:
            response = client.get_crawler(Name='retail-platform-raw-crawler')
            state = response['Crawler']['State']
            print(f"Crawler state: {state}")
            if state == 'READY':
                break
            elif state == 'STOPPING':
                time.sleep(10)
            else:
                time.sleep(30)

        return "Crawler complete"

    crawler_task = PythonOperator(
        task_id='run_glue_crawler',
        python_callable=run_crawler,
    )

    # =========================
    # TASK DEPENDENCIES
    # =========================
    ingestion_task >> crawler_task