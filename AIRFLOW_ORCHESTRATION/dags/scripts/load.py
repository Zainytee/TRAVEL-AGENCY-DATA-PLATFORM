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
    BUCKET_NAME = "zainycap-bucket"

    try:
        # Create an S3Hook instance
        s3_hook = S3Hook(aws_conn_id="aws_default")

        # Upload the file to S3 with dynamic file path
        s3_hook.load_file(
            # Path to the transformed file
            filename=transformed_file_path,
            # Path in S3, dynamically created with Jinja
            key=s3_file_path,
            bucket_name=BUCKET_NAME,
            replace=True
        )

        logging.info(
            f"File successfully uploaded to S3: "
            f"s3://{BUCKET_NAME}/{s3_file_path}"
        )
    except Exception as e:
        logging.error(f"Failed to upload file to S3: {e}")
        raise


def load_data_to_snowflake(
    transformed_data, table_name, schema_name,
    snowflake_conn_id, unique_column
):
    """
    Loads transformed data into a Snowflake table, avoiding duplicates.
    Args:
        transformed_data (DataFrame): Transformed data to be loaded.
        table_name (str): Target Snowflake table name.
        schema_name (str): Snowflake schema name.
        snowflake_conn_id (str): Airflow connection ID for Snowflake.
        unique_column (str): The unique column name
        for identifying duplicate records.
    Returns:
        None
    """
    try:
        # Establish Snowflake connection
        snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

        # Get column names from the transformed data
        columns = list(transformed_data.columns)
        rows = [tuple(record) for record in transformed_data.to_numpy()]

        # Check if the table exists in Snowflake
        check_table_query = f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = '{schema_name.upper()}'
        AND table_name = '{table_name.upper()}';
        """
        result = snowflake_hook.get_first(check_table_query)

        if result[0] == 0:
            # Table does not exist, create it
            create_table_query = f"""
            CREATE TABLE {schema_name}.{table_name} (
                {', '.join([f"{col} STRING" for col in columns])},
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
            );
            """
            snowflake_hook.run(create_table_query)
            logger.info(
                f"Table {schema_name}.{table_name} created successfully."
            )

        # Stage the data into a temporary table
        temp_table = f"{table_name}_temp"
        create_temp_table_query = f"""
        CREATE TEMP TABLE {schema_name}.{temp_table} AS
        SELECT * FROM {schema_name}.{table_name} LIMIT 0;
        """
        snowflake_hook.run(create_temp_table_query)

        # Load data into the temporary table
        column_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_temp_query = f"""
        INSERT INTO {schema_name}.{temp_table} ({column_str})
        VALUES ({placeholders})
        """
        for row in rows:
            snowflake_hook.run(insert_temp_query, parameters=row)

        # Perform the MERGE to avoid duplicates
        merge_query = f"""
        MERGE INTO {schema_name}.{table_name} AS target
        USING {schema_name}.{temp_table} AS source
        ON target.{unique_column} = source.{unique_column}
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
            f"Data merged into {schema_name}.{table_name} successfully."
        )
    except Exception as e:
        logger.error(f"Error in load_data_to_snowflake: {e}")
        raise
