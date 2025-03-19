import os
from typing import Dict, Optional

# Keep this file empty for now as we don't need any configuration
# It's being kept as a placeholder for future use 

def get_s3_path(database: str, table_name: Optional[str] = None) -> str:
    """
    Get the S3 path for a database or table
    
    Args:
        database: Database name
        table_name: Optional table name
        
    Returns:
        S3 path string
    """
    if table_name:
        return f"{database}/{table_name}"
    return database 