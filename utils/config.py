import os
import boto3  # type: ignore
from typing import Dict, Optional
from dotenv import load_dotenv
try:
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL
except ImportError:
    print("SQLAlchemy not installed. Please install with: pip install sqlalchemy")

# Keep this file empty for now as we don't need any configuration
# It's being kept as a placeholder for future use 

def get_s3_path(database: str, table: str) -> str:
    """
    Get the full S3 URI for a database/table
    
    Args:
        database: Database name
        table: Table name
        
    Returns:
        Full S3 URI including bucket and protocol
    """
    bucket_name = os.getenv("S3_BUCKET_NAME")
    return f"s3://{bucket_name}/{database}/{table}"

def get_aws_region() -> str:
    """Get AWS region from environment variables with default value"""
    return os.getenv("AWS_REGION", "ap-northeast-2")

def get_s3_client():
    """
    Create an S3 client with credentials from environment variables
    
    Returns:
        boto3 S3 client
    """
    return boto3.client(
        's3',
        region_name=get_aws_region(),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

def get_glue_client():
    """
    Create a Glue client with credentials from environment variables
    
    Returns:
        boto3 Glue client
    """
    return boto3.client(
        'glue',
        region_name=get_aws_region(),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )


def get_postgres_engine():
    """
    Get SQLAlchemy engine for PostgreSQL directly from .env file
    
    Returns:
        SQLAlchemy engine connected to PostgreSQL
    """
    load_dotenv()
    
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    dbname = os.getenv("POSTGRES_DB")
    
    # Try different PostgreSQL drivers in order of preference
    drivers = ["pg8000", "psycopg2"]
    last_exception = None
    
    for driver in drivers:
        try:
            connection_string = f"postgresql+{driver}://{user}:{password}@{host}:{port}/{dbname}"
            
            # Configure engine with specific connect_args if needed
            return create_engine(
                connection_string
            )
        except Exception as e:
            last_exception = e
            print(f"Warning: Could not connect using {driver}: {e}")
            continue
    
    # If we get here, none of the drivers worked
    print("All PostgreSQL drivers failed. Please install pg8000 with: pip install pg8000")
    raise last_exception 