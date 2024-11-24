import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
import logging


def transform_data(ti, **kwargs):
    """
    Transform the extracted data into Parquet
    format and store it as a file.
    """
    # Pull data from XCom
    data = ti.xcom_pull(task_ids="extract_data")
    # Convert to Arrow table
    table = pa.Table.from_pylist(data)
    # Define a temporary output file path
    output_file = (
        "/mnt/c/Users/zaina/Documents/Core_Data_Engineering/capstone_project"
        "/AIRFLOW_ORCHESTRATION/dags/tmp/transformed_data.parquet"
    )
    # Write the table to a Parquet file with Snappy compression
    pq.write_table(table, output_file, compression="snappy")
    # Return the file path (to be consumed by the load task)
    return output_file


# Transforming S3 data for snowflake
input_file = (
    "/mnt/c/Users/zaina/Documents/Core_Data_Engineering/"
    "capstone_project/AIRFLOW_ORCHESTRATION/dags/tmp/data.parquet"
    )


# Function to log completion message
def log_transformation_complete():
    logging.info("Transformation complete.")


def transform_data_s3(df):
    # Transformation logic with additional
    # checks for None or empty dictionaries
    transformed_df = pd.DataFrame({
        "Country_Name": df["name"].apply(
            lambda x: x.get("common", "Unknown")
            if isinstance(x, dict) else "Unknown"
        ),
        "Independence": df["independent"],
        "UN_Member": df["unMember"],
        "Start_of_Week": df["startOfWeek"],
        "Official_Name": df["name"].apply(
            lambda x: x.get("official", "Unknown")
            if isinstance(x, dict) else "Unknown"
        ),
        "Common_Native_Name": df["translations"].apply(
            lambda x: x.get(
                "nativeName", {}).get("common", "Unknown")
            if isinstance(x, dict) else "Unknown"
        ),
        "Currency_Code": df["currencies"].apply(
            lambda x: list(x.keys())[0]
            if isinstance(x, dict) and x else None
        ),
        "Currency_Name": df["currencies"].apply(
            lambda x: list(x.values())[0].get("name", "No Currency Name")
        if isinstance(x, dict) and x and list(x.values())
            and isinstance(list(x.values())[0], dict)
        else "No Currency Name"

        ),

        "Currency_Symbol": df["currencies"].apply(
            lambda x: list(x.values())[0].get("symbol", "No Currency Symbol")
        if isinstance(x, dict) and x and list(x.values())
            and isinstance(list(x.values())[0], dict) else "No Currency Symbol"
        ),
        "Country_Code": df["idd"].apply(
            lambda x: f"{x.get('root', '')}{''.join(x.get('suffixes', []))}"
        if isinstance(x, dict) and x.get('root') else "No Country Code"
        ),
        "Capital": df["capital"].apply(
            lambda x: x[0] if isinstance(x, list) and x and x[0] else "No Capital"
        ),
        "Region": df["region"],
        "Sub_Region": df.get(
            "subregion", pd.Series(["No Sub-Region"] * len(df))).apply(
            lambda x: x if pd.notna(x) else "No Sub-Region"
        ),
        "Languages": df["languages"].apply(
        lambda x: ", ".join([v for v in x.values() if isinstance(v, str)])
            if isinstance(x, dict) else None
        ),

        "Area": df["area"],

        "Population": df["population"],

        "Continents": df["continents"].apply(
            lambda x: ", ".join(x) 
            if isinstance(x, list) and x else "No Continents"
        ),
    })
    return transformed_df


def run_transformation(
    load_function, table_name, schema_name, snowflake_conn_id
):
    """
    Transforms the data and loads it into Snowflake.
    Args:
        load_function (callable): The function responsible
        for loading data to Snowflake.
        table_name (str): Name of the Snowflake table.
        schema_name (str): Name of the Snowflake schema.
        snowflake_conn_id (str): Snowflake connection ID.
    """
    # Read the Parquet data
    data = pd.read_parquet(input_file)
    # Perform the transformation
    transformed_data = transform_data_s3(data)
    # Log the completion message
    log_transformation_complete()
    # Call the load function with all required arguments
    load_function(
        transformed_data=transformed_data,
        table_name=table_name,
        schema_name=schema_name,
        snowflake_conn_id=snowflake_conn_id,
        unique_column="Country_Name"
    )
    # Return transformed data for XCom
    return transformed_data


