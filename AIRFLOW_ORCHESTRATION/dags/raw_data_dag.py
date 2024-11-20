from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="An ETL pipeline for Country REST API data",
    schedule_interval="@daily",
    start_date=datetime(2024, 11, 17),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        templates_dict={
            "s3_key": "data/country_data_{{ ds }}.parquet",  # Jinja template for dynamic date substitution
        },
        provide_context=True,
    )

    # Task dependencies
    extract_task >> transform_task >> load_task
