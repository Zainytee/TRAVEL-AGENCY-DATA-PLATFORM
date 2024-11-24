import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_data(ti, **kwargs):
    """
    Load the transformed data to S3.
    """
    # Get the transformed data file path from XCom 
    transformed_file_path = ti.xcom_pull(task_ids="transform_data")
    # Use the templated s3_key passed from the DAG
    s3_file_path = kwargs["templates_dict"]["s3_key"]
    BUCKET_NAME = 'zainycap-bucket'
    try:
        # Create an S3Hook instance
        s3_hook = S3Hook(aws_conn_id='aws_default')
        # Upload the file to S3 with dynamic file path
        s3_hook.load_file(
            filename=transformed_file_path,  # Path to the transformed file
            KEY=s3_file_path,  # Path in S3, dynamically created with Jinja
            BUCKET_NAME=BUCKET_NAME,
            replace=True
        )
        logging.info(
    f"File successfully uploaded to S3: s3://{BUCKET_NAME}/{s3_file_path}"
)
    except Exception as e:
        logging.error(f"Failed to upload file to S3: {e}")
        raise
# Loading file from S3 Bucket to Snowflake Database
# Set up logging


def load_data_to_snowflake(
    transformed_data, TABLE_NAME, SCHEMA_NAME, 
    SNOWFLAKE_CONN_ID, UNIQUE_COLUMN
):
    """
    Loads transformed data into a Snowflake table, avoiding duplicates.
    Args:
        transformed_data (DataFrame): Transformed data to be loaded.
        TABLE_NAME (str): Target Snowflake table name.
        SCHEMA_NAME (str): Snowflake schema name.
        SNOWFLAKE_CONN_ID (str): Airflow connection ID for Snowflake.
        UNIQUE_COLUMN (str): The unique column name 
        for identifying duplicate records.
    Returns:
        None
    """
    try:
        # Establish Snowflake connection
        snowflake_hook = SnowflakeHook(SNOWFLAKE_CONN_ID=SNOWFLAKE_CONN_ID)
        # Get column names from the transformed data
        columns = list(transformed_data.columns)
        rows = [tuple(record) for record in transformed_data.to_numpy()]
        # Check if the table exists in Snowflake
        check_table_query = f"""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = '{SCHEMA_NAME.upper()}' 
        AND TABLE_NAME = '{TABLE_NAME.upper()}';
        """
        result = snowflake_hook.get_first(check_table_query)
        if result[0] == 0:
            # Table does not exist, create it
            create_table_query = f"""
            CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (
                {', '.join([f"{col} STRING" for col in columns])},
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
            );
            """
            snowflake_hook.run(create_table_query)
            logger.info(
                f"Table {SCHEMA_NAME}.{TABLE_NAME} created successfully."
            )
        # Stage the data into a temporary table
        temp_table = f"{TABLE_NAME}_temp"
        create_temp_table_query = f"""
        CREATE TEMP TABLE {SCHEMA_NAME}.{temp_table} AS
        SELECT * FROM {SCHEMA_NAME}.{TABLE_NAME} LIMIT 0;
        """
        snowflake_hook.run(create_temp_table_query)
        # Load data into the temporary table
        column_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_temp_query = f"""
        INSERT INTO {SCHEMA_NAME}.{temp_table} ({column_str})
        VALUES ({placeholders})
        """
        for row in rows:
            snowflake_hook.run(insert_temp_query, parameters=row)
        # Perform the MERGE to avoid duplicates
        merge_query = f"""
        MERGE INTO {SCHEMA_NAME}.{TABLE_NAME} AS target
        USING {SCHEMA_NAME}.{temp_table} AS source
        ON target.{UNIQUE_COLUMN} = source.{UNIQUE_COLUMN}
        WHEN MATCHED THEN 
            UPDATE SET {', '.join(
                [f"target.{col} = source.{col}" for col in columns]
            )}
        WHEN NOT MATCHED THEN
         INSERT (
    {column_str}
) VALUES (
    {', '.join([f"source.{col}" for col in columns])}
);
        """
        snowflake_hook.run(merge_query)
        logger.info(
            f"Data merged into {SCHEMA_NAME}.{TABLE_NAME} successfully."
        )
    except Exception as e:
        logger.error(f"Error in load_data_to_snowflake: {e}")
        raise
