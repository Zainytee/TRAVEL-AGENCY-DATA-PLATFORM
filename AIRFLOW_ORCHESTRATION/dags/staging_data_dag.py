from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from scripts.extract import extract_data_s3
from scripts.transform import run_transformation
from scripts.load import load_data_to_snowflake
from airflow.exceptions import AirflowException
from scripts.extract import output_file

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Static table name (e.g., country_data)
table_name = "country_data"
schema_name = "staging"
snowflake_conn_id = 'snowflake_conn_id'
bucket_name = "zainycap-bucket"
key = "data/country_data_2024-11-18.parquet"




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
        op_args=[bucket_name,key],  # Replace with actual values
        op_kwargs={'output_path': output_file},  # Ensure the file is saved locally for transformation
        provide_context=True
    )


    # Task to run the transformation function
    transform_and_load_task = PythonOperator(
    task_id='run_data_transformation',
    python_callable=run_transformation,
    op_args=[
        load_data_to_snowflake,  # Pass the load function
        table_name,              # Snowflake table name
        schema_name,             # Snowflake schema name
        snowflake_conn_id        # Snowflake connection ID
    ],
)



    # Load task
    load_task = PythonOperator(
    task_id="load_data_to_snowflake",
    python_callable=load_data_to_snowflake,
    op_args=[
        '{{ task_instance.xcom_pull(task_ids="run_data_transformation") }}',  # Pull the transformed data
        table_name,
        schema_name,
        snowflake_conn_id
    ],
    provide_context=True,
    )

    # Task dependencies
    extract_task >> transform_and_load_task

