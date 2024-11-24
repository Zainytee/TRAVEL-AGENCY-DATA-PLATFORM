from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.extract import extract_data, extract_data_s3, output_file
from scripts.transform import transform_data, run_transformation
from scripts.load import load_data, load_data_to_snowflake

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
# table name
TABLE_NAME = "country_data"
SCHEMA_NAME = "staging"
SNOWFLAKE_CONN_ID = 'SNOWFLAKE_CONN_ID'
BUCKET_NAME = "zainycap-bucket"
KEY = "data/country_data_2024-11-18.parquet"
# Define the DAG
with DAG(
    dag_id="etl_pipeline_travel_agency",
    default_args=default_args,
    description="An ETL pipeline for travel_agency_pipeline",
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
            # Jinja template for dynamic date substitution
            "s3_key": "data/country_data_{{ ds }}.parquet",
        },
        provide_context=True,
    )
    # Extract task2(extraction from s3 Bucket)
    extract_task2 = PythonOperator(
        task_id="extract_data_to_snowflake",
        python_callable=extract_data_s3,
        op_args=[BUCKET_NAME, KEY],
        op_kwargs={'output_path': output_file},
        provide_context=True
    )
    # Transform_task2
    transform_and_load_task = PythonOperator(
        task_id='run_data_transformation',
        python_callable=run_transformation,
        op_args=[
            load_data_to_snowflake,
            TABLE_NAME,
            SCHEMA_NAME,
            SNOWFLAKE_CONN_ID
        ],
    )
    # Task dependencies
    extract_task >> transform_task >> load_task
    load_task >> extract_task2 >> transform_and_load_task
