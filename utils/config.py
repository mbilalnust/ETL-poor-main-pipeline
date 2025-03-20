import os
import boto3  # type: ignore
from typing import Dict, Optional

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
    bucket_name = os.getenv("S3_BUCKET_NAME", "data-lake")
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