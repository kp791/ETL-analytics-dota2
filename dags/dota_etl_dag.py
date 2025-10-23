from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.operators.python import PythonOperator
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'dota-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def log_pipeline_start():
    logger.info("Starting Dota 2 ETLA Pipeline")
    print("=" * 60)
    print("DOTA 2 ETL + ANALYSIS PIPELINE STARTING")
    print("=" * 60)

def log_pipeline_end():
    logger.info("Dota 2 ETLA Pipeline completed")
    print("=" * 60)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)

with DAG(
    'dota_etla_pipeline',
    default_args=default_args,
    description='Extract-Transform-Load-Analyze pipeline for Dota 2 with HDFS and PostgreSQL',
    schedule_interval='@daily',
    catchup=False,
    tags=['dota', 'etl', 'pyspark', 'hdfs', 'postgresql'],
) as dag:

    start_log = PythonOperator(
        task_id='log_start',
        python_callable=log_pipeline_start,
    )

    extract = PapermillOperator(
        task_id='extract_data',
        input_nb='/opt/airflow/notebooks/extract.ipynb',
        output_nb='/opt/airflow/notebooks/output/extract_{{ ds }}.ipynb',
        parameters={
            'execution_date': '{{ ds }}',
            'data_dir': '/opt/airflow/data/raw'
        },
    )

    transform = PapermillOperator(
        task_id='transform_data',
        input_nb='/opt/airflow/notebooks/transform.ipynb',
        output_nb='/opt/airflow/notebooks/output/transform_{{ ds }}.ipynb',
        parameters={
            'execution_date': '{{ ds }}',
            'input_dir': '/opt/airflow/data/raw',
            'output_dir': '/opt/airflow/data/processed'
        },
    )

    load = PapermillOperator(
        task_id='load_data',
        input_nb='/opt/airflow/notebooks/load.ipynb',
        output_nb='/opt/airflow/notebooks/output/load_{{ ds }}.ipynb',
        parameters={
            'execution_date': '{{ ds }}',
            'input_dir': '/opt/airflow/data/processed'
        },
    )

    analyze = PapermillOperator(
        task_id='analyze_data',
        input_nb='/opt/airflow/notebooks/analyze.ipynb',
        output_nb='/opt/airflow/notebooks/output/analyze_{{ ds }}.ipynb',
        parameters={
            'execution_date': '{{ ds }}',
            'output_dir': '/opt/airflow/data/insights'
        },
    )

    end_log = PythonOperator(
        task_id='log_end',
        python_callable=log_pipeline_end,
    )

    start_log >> extract >> transform >> load >> analyze >> end_log

