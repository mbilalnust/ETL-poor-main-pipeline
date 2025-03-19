from typing import Dict

def get_s3_path(database: str = "analytics") -> str:
    """
    Get the S3 path for a database
    
    Args:
        database: Database name (default: "analytics")
        
    Returns:
        Full S3 path for the database
    """
    s3_bucket = "warehouse-billy"
    return f"s3://{s3_bucket}/{database}"

def get_iceberg_config(table_name: str, database: str = "analytics") -> Dict[str, str]:
    """
    Get Iceberg table configuration for AWS Glue and S3.
    
    Args:
        table_name: Table name
        database: Database name (default: "analytics")
        
    Returns:
        Dictionary with Iceberg configuration
    """
    s3_path = get_s3_path(database)
    
    return {
        "glue_database": database,
        "table_name": table_name,
        "location": f"{s3_path}/{table_name}",
        "s3_bucket": "warehouse-billy",
        "s3_prefix": f"{database}/{table_name}"
    }

def get_aws_region() -> str:
    """Return the AWS region for the project"""
    return "ap-northeast-2"  # Default to Seoul region

# AWS configuration for common services
aws_config = {
    "region": get_aws_region(),
    "athena_output_location": "s3://warehouse-billy/athena-query-results/",
} 