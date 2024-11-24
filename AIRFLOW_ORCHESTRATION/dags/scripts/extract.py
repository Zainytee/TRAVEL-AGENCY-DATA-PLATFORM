import requests
import pyarrow.parquet as pq
from io import BytesIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException
import os


# Extracting Data from REST API to S3 Bucket
def extract_data(**kwargs):
    """
    Extract data from the Country REST API.
    """
    api_url = "https://restcountries.com/v3.1/all"
    response = requests.get(api_url)
    response.raise_for_status()  # Raise an error for bad HTTP status codes
    data = response.json()
    return data


BUCKET_NAME = "zainycap-bucket"
KEY = "data/country_data_2024-11-18.parquet"
# Define a temporary output file path
output_file = (
    "/mnt/c/Users/zaina/Documents/Core_Data_Engineering"
    "/capstone_project/AIRFLOW_ORCHESTRATION/dags/tmp/data.parquet"
)
# Extracting Data from S3 bucket to Snowflake Database (Parquet file)


def extract_data_s3(BUCKET_NAME, KEY, OUTPUT_PATH, AWS_CONN_ID="aws_default"):
    """
    Extracts Parquet data from an S3 bucket,
    saves it locally, and returns the file path.
    Args:
        BUCKET_NAME (str): Name of the S3 bucket.
        KEY (str): KEY (file path) of the Parquet file in S3.
        OUTPUT_PATH (str): Local path where the Parquet file should be saved.
        AWS_CONN_ID (str): Airflow connection ID for AWS (default: 'aws_default').
    Returns:
        str: Path to the saved local Parquet file.
    """
    try:
        # Use S3Hook to interact with AWS S3
        s3_hook = S3Hook(AWS_CONN_ID=AWS_CONN_ID)
        # Download file from S3 to memory
        file_obj = s3_hook.get_key(KEY, BUCKET_NAME)
        if not file_obj:
            raise AirflowException(f"File {KEY} not found in bucket {BUCKET_NAME}")
        file_content = file_obj.get()["Body"].read()
        buffer = BytesIO(file_content)
        # Read Parquet data using PyArrow
        table = pq.read_table(buffer)
        # Ensure output directory exists
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        # Save the Parquet file locally
        pq.write_table(table, OUTPUT_PATH)
        return OUTPUT_PATH  # Return the path to the saved Parquet file
    except Exception as e:
        raise AirflowException(f"Failed to extract data from S3: {str(e)}")
