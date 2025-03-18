import os
import json
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Optional
from pathlib import Path

# Import our custom clients
from utils.s3_client import S3Client
from weather_api_client import WeatherAPIClient

# Load environment variables
load_dotenv()

# Configure output paths
DATA_DIR = os.getenv("DATA_DIR", "/data")
RAW_DATA_DIR = f"{DATA_DIR}/raw"
OUTPUT_FILE_PREFIX = "weather_data_"


def ensure_dir_exists(directory: str) -> None:
    """
    Ensure that a directory exists, creating it if necessary
    
    Args:
        directory: Directory path to check/create
    """
    Path(directory).mkdir(parents=True, exist_ok=True)


def run_weather_data_pipeline(
    cities: Optional[List[str]] = None,
    s3_key: str = "ca_cities.csv",
    output_dir: Optional[str] = None
) -> str:
    """
    Main pipeline function to collect weather data
    
    Args:
        cities: Optional list of cities. If None, will download from S3
        s3_key: Key of the cities file in S3 bucket
        output_dir: Directory to save the output file
        
    Returns:
        Path to the output file
    """
    # Setup output directory
    if output_dir is None:
        output_dir = RAW_DATA_DIR
    
    ensure_dir_exists(output_dir)
    
    # Generate output filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{output_dir}/{OUTPUT_FILE_PREFIX}{timestamp}.json"
    
    # Get list of cities
    if cities is None:
        print("Downloading cities from S3...")
        s3_client = S3Client()
        cities = s3_client.download_cities_data(s3_key)
        if cities is None or len(cities) == 0:
            raise ValueError("Failed to download cities or city list is empty")
    
    print(f"Processing {len(cities)} cities...")
    
    # Fetch weather data for cities
    weather_client = WeatherAPIClient()
    weather_data = weather_client.get_batch_weather_data(cities)
    
    # Extract relevant metrics
    processed_data = [
        weather_client.extract_weather_metrics(data)
        for data in weather_data
    ]
    
    # Save data to file
    print(f"Saving data to {output_file}...")
    with open(output_file, 'w') as f:
        json.dump(processed_data, f, indent=2)
    
    print(f"Successfully processed {len(processed_data)} cities")
    return output_file


if __name__ == "__main__":
    try:
        output_file = run_weather_data_pipeline()
        print(f"Data pipeline completed successfully. Output saved to: {output_file}")
    except Exception as e:
        print(f"Error in weather data pipeline: {e}")
        raise 