import os
import pandas as pd  # type: ignore
import duckdb  # type: ignore
import boto3  # type: ignore
from typing import Optional, List, Dict, Any, Union
from pathlib import Path

def get_s3_path(database: str, table: str) -> str:
    """Get the S3 location for a table"""
    # Use a specific bucket name from environment variables
    bucket_name = os.getenv("S3_BUCKET_NAME", "data-lake")
    return f"s3://{bucket_name}/{database}/{table}"

def duck_db_parquet_delete_and_insert(
    database: str,
    table: str,
    date_id: str,
    data: pd.DataFrame,
    schema: Optional[Dict[str, str]] = None
) -> None:
    """
    Store data in partitioned Parquet files using DuckDB, upload to S3, and register in Glue.
    
    Args:
        database: Database name (will be used as a directory)
        table: Table name (will be used as a directory)
        date_id: Date identifier for partitioning (e.g., '2023-04-01')
        data: DataFrame with data to store
        schema: Optional dictionary mapping column names to their data types
    """
    if data.empty:
        print("No data to insert, operation completed")
        return
    
    # Create DuckDB connection
    con = duckdb.connect(database=':memory:')
    
    # Set up AWS credentials for S3 access
    con.execute(f"""
        SET s3_region='{os.getenv("AWS_REGION", "ap-northeast-2")}';
        SET s3_access_key_id='{os.getenv("AWS_ACCESS_KEY_ID")}';
        SET s3_secret_access_key='{os.getenv("AWS_SECRET_ACCESS_KEY")}';
    """)
    
    # Define S3 paths
    s3_base_path = get_s3_path(database, table)
    s3_partition_path = f"{s3_base_path}/date_id={date_id}"
    s3_data_path = f"{s3_partition_path}/data.parquet"
    
    # Print the S3 paths for debugging
    print(f"S3 base path: {s3_base_path}")
    print(f"S3 partition path: {s3_partition_path}")
    print(f"S3 data path: {s3_data_path}")
    
    # Register the DataFrame in DuckDB
    con.register('data_to_insert', data)
    
    # Delete existing partition if it exists
    try:
        # Create S3 client
        s3_client = boto3.client(
            's3',
            region_name=os.getenv("AWS_REGION", "ap-northeast-2"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        
        # Parse the S3 URL to get bucket and prefix
        s3_parts = s3_partition_path.replace("s3://", "").split("/", 1)
        bucket = s3_parts[0]
        prefix = s3_parts[1] if len(s3_parts) > 1 else ""
        
        print(f"Using S3 bucket: {bucket}, prefix: {prefix}")
        
        # List objects in the partition path
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        # If objects exist, delete them
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            print(f"Deleting existing data for {date_id} from S3...")
            s3_client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': objects_to_delete}
            )
    except Exception as e:
        print(f"Warning: Error while checking/deleting existing data: {e}")
    
    # Save data to S3 in Parquet format
    print(f"Saving {len(data)} rows to {s3_data_path}")
    
    # If schema is provided, enforce it
    if schema:
        # Create a temporary table with the desired schema
        columns_def = ", ".join([f"{col} {dtype}" for col, dtype in schema.items()])
        con.execute(f"CREATE TABLE temp_table ({columns_def})")
        
        # Insert data into the temporary table
        con.execute("INSERT INTO temp_table SELECT * FROM data_to_insert")
        
        # Export the temporary table to Parquet in S3
        con.execute(f"COPY temp_table TO '{s3_data_path}' (FORMAT PARQUET)")
    else:
        # Export directly without schema enforcement
        con.execute(f"COPY (SELECT * FROM data_to_insert) TO '{s3_data_path}' (FORMAT PARQUET)")
    
    # Register table in AWS Glue if not already registered
    try:
        # Create Glue client
        glue_client = boto3.client(
            'glue',
            region_name=os.getenv("AWS_REGION", "ap-northeast-2"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        
        # Check if database exists, create if not
        try:
            glue_client.get_database(Name=database)
        except glue_client.exceptions.EntityNotFoundException:
            print(f"Creating Glue database {database}")
            glue_client.create_database(
                DatabaseInput={
                    'Name': database,
                    'Description': f'Database for {database} data'
                }
            )
        
        # Check if table exists
        table_exists = True
        try:
            glue_client.get_table(DatabaseName=database, Name=table)
        except glue_client.exceptions.EntityNotFoundException:
            table_exists = False
        
        if not table_exists and schema:
            print(f"Creating Glue table {database}.{table}")
            
            # Prepare column definitions for Glue
            columns = []
            for col, dtype in schema.items():
                if dtype.upper() == 'VARCHAR':
                    glue_type = 'string'
                elif dtype.upper() == 'INTEGER':
                    glue_type = 'int'
                elif dtype.upper() == 'DOUBLE':
                    glue_type = 'double'
                else:
                    glue_type = 'string'  # Default to string
                
                columns.append({
                    'Name': col,
                    'Type': glue_type
                })
            
            # Create the table in Glue
            glue_client.create_table(
                DatabaseName=database,
                TableInput={
                    'Name': table,
                    'StorageDescriptor': {
                        'Columns': columns,
                        'Location': s3_base_path,
                        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                        }
                    },
                    'PartitionKeys': [
                        {
                            'Name': 'date_id',
                            'Type': 'string'
                        }
                    ],
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'classification': 'parquet',
                        'has_encrypted_data': 'false'
                    }
                }
            )
        
        # Create or update partition in Glue
        print(f"Creating/updating partition for date_id={date_id}")
        glue_client.create_partition(
            DatabaseName=database,
            TableName=table,
            PartitionInput={
                'Values': [date_id],
                'StorageDescriptor': {
                    'Location': s3_partition_path,
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                }
            }
        )
    except Exception as e:
        print(f"Warning: Error registering table in Glue: {e}")
    
    print(f"Successfully saved data to {s3_data_path} and registered in Glue")

# For backward compatibility
duck_db_iceberg_delete_and_insert = duck_db_parquet_delete_and_insert 