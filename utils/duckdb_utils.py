import os
import pandas as pd  # type: ignore
import duckdb  # type: ignore
from typing import Optional, List, Dict, Any, Union
from pathlib import Path

from utils.config import get_s3_path, get_s3_client, get_glue_client, get_aws_region

def get_duckdb_connection(database=':memory:'):
    """
    Create a DuckDB connection with AWS credentials configured
    
    Args:
        database: Database path or ':memory:' for in-memory database
        
    Returns:
        DuckDB connection with AWS credentials configured
    """
    con = duckdb.connect(database=database)
    
    # Set up AWS credentials for S3 access
    con.execute(f"""
        SET s3_region='{get_aws_region()}';
        SET s3_access_key_id='{os.getenv("AWS_ACCESS_KEY_ID")}';
        SET s3_secret_access_key='{os.getenv("AWS_SECRET_ACCESS_KEY")}';
    """)
    
    return con

def delete_partition_data(
    database: str,
    table: str,
    date_id: str,
    s3_partition_path: str
) -> None:
    """
    Delete both S3 data and Glue partition metadata for a specific partition
    
    Args:
        database: Glue database name
        table: Table name
        date_id: Date identifier for the partition to delete
        s3_partition_path: Full S3 path to the partition
    """
    # Create AWS clients
    s3_client = get_s3_client()
    glue_client = get_glue_client()
    
    # 1. Delete S3 data
    try:
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
        print(f"Warning: Error while deleting S3 data: {e}")
    
    # 2. Delete Glue partition
    try:
        # Check if table exists before trying to delete partition
        try:
            glue_client.get_table(DatabaseName=database, Name=table)
            table_exists = True
        except glue_client.exceptions.EntityNotFoundException:
            table_exists = False
            return  # No need to delete partition if table doesn't exist
        
        if table_exists:
            print(f"Attempting to delete existing partition for date_id={date_id}")
            try:
                glue_client.delete_partition(
                    DatabaseName=database,
                    TableName=table,
                    PartitionValues=[date_id]
                )
                print(f"Successfully deleted existing partition for date_id={date_id}")
            except glue_client.exceptions.EntityNotFoundException:
                print(f"No existing partition found for date_id={date_id}")
    except Exception as e:
        print(f"Warning: Error deleting Glue partition: {e}")

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
    con = get_duckdb_connection()
    
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
    
    # Delete existing partition data (both S3 and Glue)
    delete_partition_data(
        database=database,
        table=table,
        date_id=date_id,
        s3_partition_path=s3_partition_path
    )
    
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
        glue_client = get_glue_client()
        
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
        
        # Create new partition in Glue
        print(f"Creating partition for date_id={date_id}")
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
print("duck_db_parquet_delete_and_insert")