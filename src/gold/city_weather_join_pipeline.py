import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import our modules
from silver.db_client import PostgresClient
from utils.s3_client import S3Client

# Load environment variables
load_dotenv()

# Configure paths
DATA_DIR = os.getenv("DATA_DIR", "/data")
GOLD_DATA_DIR = f"{DATA_DIR}/gold"
OUTPUT_FILE_PREFIX = "city_weather_joined_"


def ensure_dir_exists(directory: str) -> None:
    """
    Ensure that a directory exists, creating it if necessary
    
    Args:
        directory: Directory path to check/create
    """
    Path(directory).mkdir(parents=True, exist_ok=True)


def run_join_pipeline(
    output_dir: str = None,
    city_data_s3_key: str = "ca_cities.csv",
    upload_to_s3: bool = True
) -> str:
    """
    Join weather data with city demographic data
    
    Args:
        output_dir: Directory to save the output file
        city_data_s3_key: S3 key for the city data file
        upload_to_s3: Whether to upload the joined data to S3
        
    Returns:
        Path to the output CSV file
    """
    # Setup output directory
    if output_dir is None:
        output_dir = GOLD_DATA_DIR
    
    ensure_dir_exists(output_dir)
    
    # Generate output filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{output_dir}/{OUTPUT_FILE_PREFIX}{timestamp}.csv"
    
    # Download city data from S3
    print("Downloading city data from S3...")
    s3_client = S3Client()
    
    # Get city data from S3
    try:
        # This is a placeholder for downloading the actual data
        # In a real implementation, we would download the full demographic data
        cities = s3_client.download_cities_data(city_data_s3_key)
        if not cities:
            raise ValueError("Failed to download city data from S3")
            
        # For this example, we'll create a simple DataFrame with city names
        city_df = pd.DataFrame({
            "city": cities,
            "province": ["CA"] * len(cities),  # Placeholder
            "population": [0] * len(cities),   # Placeholder
            "land_area_sq_mile": [0] * len(cities)  # Placeholder
        })
        
    except Exception as e:
        print(f"Error downloading city data: {e}")
        raise
    
    # Get weather data from the database
    print("Retrieving weather data from database...")
    db_client = PostgresClient()
    weather_df = db_client.query_to_dataframe("""
        SELECT city, description, temperature_fahrenheit, feels_like_fahrenheit,
               humidity, pressure, wind_speed, time_of_record
        FROM weather_data
        ORDER BY time_of_record DESC
    """)
    
    if weather_df.empty:
        raise ValueError("No weather data found in the database")
    
    # Deduplicate weather data to get the latest for each city
    weather_df = weather_df.sort_values("time_of_record", ascending=False)
    latest_weather_df = weather_df.drop_duplicates(subset=["city"])
    
    # Join the datasets
    print("Joining weather and city data...")
    joined_df = pd.merge(latest_weather_df, city_df, on="city", how="inner")
    
    # Save the joined data
    joined_df.to_csv(output_file, index=False)
    print(f"Saved joined data to {output_file}")
    
    # Upload to S3 if requested
    if upload_to_s3:
        print("Uploading joined data to S3...")
        s3_key = f"joined/{os.path.basename(output_file)}"
        s3_client.upload_data(joined_df, s3_key)
        print(f"Uploaded joined data to S3 with key: {s3_key}")
    
    return output_file


if __name__ == "__main__":
    try:
        output_file = run_join_pipeline()
        print(f"Join pipeline completed successfully. Output saved to: {output_file}")
    except Exception as e:
        print(f"Error in join pipeline: {e}")
        raise 