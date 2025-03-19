import os
import pandas as pd  # type: ignore
import duckdb  # type: ignore
from typing import Optional, List, Dict, Any, Union

from utils.config import get_s3_path

def duck_db_iceberg_delete_and_insert(
    database: str,
    table: str,
    delete_condition: str,
    data: Union[pd.DataFrame, str],
    partition_column_names: Optional[List[str]] = None,
    schema: Optional[Dict[str, str]] = None
) -> None:
    """
    Delete data from an Iceberg table based on a condition and insert new data using DuckDB.
    
    Args:
        database: Glue database name
        table: Table name
        delete_condition: SQL condition for deletion (e.g., "date_id = '2023-04-01'")
        data: Either a pandas DataFrame with data to insert or an SQL query to execute
        partition_column_names: List of column names to use for partitioning
        schema: Dictionary mapping column names to their data types
    """
    # Create DuckDB connection
    con = duckdb.connect(database=':memory:')
    
    # Load iceberg extension
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")
    
    # Set up AWS credentials
    con.execute(f"""
        SET s3_region='{os.getenv("AWS_REGION", "ap-northeast-2")}';
        SET s3_access_key_id='{os.getenv("AWS_ACCESS_KEY_ID")}';
        SET s3_secret_access_key='{os.getenv("AWS_SECRET_ACCESS_KEY")}';
    """)
    
    # Check if table exists
    table_exists = True
    try:
        con.execute(f"DESCRIBE iceberg_catalog.{database}.{table}")
    except Exception:
        table_exists = False
        
    # If table exists, delete data matching the condition
    if table_exists:
        print(f"Deleting data from {database}.{table} where {delete_condition}")
        try:
            con.execute(f"DELETE FROM iceberg_catalog.{database}.{table} WHERE {delete_condition}")
        except Exception as e:
            print(f"Error during delete operation: {e}")
            # Continue anyway as the table might be empty or the condition might not match
    
    # Register DataFrame if provided as data source
    if isinstance(data, pd.DataFrame):
        if data.empty:
            print("No data to insert, operation completed")
            return
            
        # Register the DataFrame
        con.register('data_to_insert', data)
        
        # Insert data into existing table
        if table_exists:
            print(f"Inserting {len(data)} rows into {database}.{table}")
            con.execute(f"INSERT INTO iceberg_catalog.{database}.{table} SELECT * FROM data_to_insert")
        else:
            # Create new table with schema if table doesn't exist
            print(f"Creating new table {database}.{table}")
            
            # Prepare partition clause
            partition_clause = ""
            if partition_column_names:
                partition_names = ", ".join([f"'{col}'" for col in partition_column_names])
                partition_clause = f"PARTITIONED BY ({partition_names})"
            
            # Create table from DataFrame
            if schema:
                # Create table with specified schema
                columns_def = ", ".join([f"{col} {dtype}" for col, dtype in schema.items()])
                
                table_location = get_table_location(database, table)
                create_query = f"""
                    CREATE TABLE iceberg_catalog.{database}.{table} (
                        {columns_def}
                    ) {partition_clause}
                    WITH (
                        location='{table_location}',
                        catalog_type='glue',
                        AWS_GLUE_AWS_REGION='{os.getenv("AWS_REGION", "ap-northeast-2")}',
                        'glue_database_name'='{database}'
                    )
                """
                con.execute(create_query)
                
                # Insert data into newly created table
                con.execute(f"INSERT INTO iceberg_catalog.{database}.{table} SELECT * FROM data_to_insert")
            else:
                # Create table directly from DataFrame with inferred schema
                table_location = get_table_location(database, table)
                create_query = f"""
                    CREATE TABLE iceberg_catalog.{database}.{table} 
                    {partition_clause}
                    WITH (
                        location='{table_location}',
                        catalog_type='glue',
                        AWS_GLUE_AWS_REGION='{os.getenv("AWS_REGION", "ap-northeast-2")}',
                        'glue_database_name'='{database}'
                    )
                    AS SELECT * FROM data_to_insert
                """
                con.execute(create_query)
    else:
        # Handle SQL query as data source
        if table_exists:
            print(f"Inserting data from query into {database}.{table}")
            con.execute(f"INSERT INTO iceberg_catalog.{database}.{table} {data}")
        else:
            # Create new table with query
            print(f"Creating new table {database}.{table} from query")
            
            # Prepare partition clause
            partition_clause = ""
            if partition_column_names:
                partition_names = ", ".join([f"'{col}'" for col in partition_column_names])
                partition_clause = f"PARTITIONED BY ({partition_names})"
            
            table_location = get_table_location(database, table)
            create_query = f"""
                CREATE TABLE iceberg_catalog.{database}.{table}
                {partition_clause}
                WITH (
                    location='{table_location}',
                    catalog_type='glue',
                    AWS_GLUE_AWS_REGION='{os.getenv("AWS_REGION", "ap-northeast-2")}',
                    'glue_database_name'='{database}'
                )
                AS {data}
            """
            con.execute(create_query)
    
    print(f"Successfully completed operation on {database}.{table}")


def get_table_location(database: str, table: str) -> str:
    """Get the S3 location for a table"""
    s3_path = get_s3_path(database)
    return f"{s3_path}/{table}" 