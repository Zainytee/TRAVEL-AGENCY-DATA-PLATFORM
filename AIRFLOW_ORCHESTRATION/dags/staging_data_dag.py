from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.extract import extract_data_s3
from scripts.transform import run_transformation
from scripts.load import load_data_to_snowflake
from scripts.extract import output_file
# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
# Static table name (e.g., country_data)
TABLE_NAME = "country_data"
SCHEMA_NAME = "staging"
SNOWFLAKE_CONN_ID = 'SNOWFLAKE_CONN_ID'
BUCKET_NAME = "zainycap-bucket"
KEY = "data/country_data_2024-11-18.parquet"
# DAG definition 
with DAG(
    dag_id="etl_pipeline_to_snowflake",
    default_args=default_args,
    description="ETL pipeline for loading data into Snowflake",
    schedule_interval="@daily",  # Change this to any schedule you want
    start_date=datetime(2024, 11, 17),
    catchup=False,
) as dag:
    # Extract task
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data_s3,
        op_args=[BUCKET_NAME,KEY],  # Replace with actual values
        op_kwargs={'output_path': output_file},  # Ensure the file is saved locally for transformation
        provide_context=True
    )
    # Task to run the transformation function
    transform_and_load_task = PythonOperator(
    task_id='run_data_transformation',
    python_callable=run_transformation,
    op_args=[
        load_data_to_snowflake,  # Pass the load function
        TABLE_NAME,              # Snowflake table name
        SCHEMA_NAME,             # Snowflake schema name
        SNOWFLAKE_CONN_ID        # Snowflake connection ID
    ],
)
    # Task dependencies
    extract_task >> transform_and_load_task

