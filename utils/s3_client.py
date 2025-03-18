import boto3
import pandas as pd
import os
from typing import List, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class S3Client:
    """Client for interacting with AWS S3"""
    
    def __init__(self):
        """Initialize the S3 client with credentials from environment variables"""
        self.aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.bucket_name = os.getenv("S3_BUCKET_NAME")
        
        if not all([self.aws_access_key, self.aws_secret_key, self.bucket_name]):
            raise ValueError("Missing AWS credentials or bucket name in environment variables")
        
        self.client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key
        )
    
    def download_cities_data(self, key: str = "ca_cities.csv") -> Optional[List[str]]:
        """
        Download cities data from S3
        
        Args:
            key: S3 object key for the cities CSV file
            
        Returns:
            List of city names or None if download fails
        """
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            df = pd.read_csv(response['Body'])
            return df['city'].tolist()
        except Exception as e:
            print(f"Failed to download cities data: {e}")
            return None
    
    def upload_data(self, data: pd.DataFrame, key: str) -> bool:
        """
        Upload processed data to S3
        
        Args:
            data: DataFrame to upload
            key: S3 object key for the uploaded file
            
        Returns:
            True if upload succeeds, False otherwise
        """
        try:
            # Convert DataFrame to CSV string
            csv_buffer = data.to_csv(index=False)
            
            # Upload to S3
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=csv_buffer
            )
            return True
        except Exception as e:
            print(f"Failed to upload data: {e}")
            return False 