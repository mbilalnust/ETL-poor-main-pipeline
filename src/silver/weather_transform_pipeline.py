import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd

# Add root directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import our modules
from silver.weather_transformers import WeatherTransformer
from silver.db_client import PostgresClient

# Load environment variables
load_dotenv()

# Configure paths
DATA_DIR = os.getenv("DATA_DIR", "/data")
RAW_DATA_DIR = f"{DATA_DIR}/raw"
PROCESSED_DATA_DIR = f"{DATA_DIR}/processed"
OUTPUT_FILE_PREFIX = "weather_transformed_"


def ensure_dir_exists(directory: str) -> None:
    """
    Ensure that a directory exists, creating it if necessary
    
    Args:
        directory: Directory path to check/create
    """
    Path(directory).mkdir(parents=True, exist_ok=True)


def run_transform_pipeline(
    input_file: str = None,
    output_dir: str = None,
    input_units: str = "metric",
    load_to_db: bool = True
) -> str:
    """
    Main transformation pipeline function
    
    Args:
        input_file: Path to input JSON file. If None, will use latest file in raw data dir
        output_dir: Directory to save the output file. If None, will use processed data dir
        input_units: Units of input data (metric, imperial, kelvin)
        load_to_db: Whether to load the transformed data into the database
        
    Returns:
        Path to the output CSV file
    """
    # Setup directories
    if output_dir is None:
        output_dir = PROCESSED_DATA_DIR
    
    ensure_dir_exists(output_dir)
    
    # Find latest file if not specified
    if input_file is None:
        transformer = WeatherTransformer()
        input_file = transformer.find_latest_data_file(RAW_DATA_DIR)
        if input_file is None:
            raise ValueError(f"No input files found in {RAW_DATA_DIR}")
    
    print(f"Processing input file: {input_file}")
    
    # Generate output filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{output_dir}/{OUTPUT_FILE_PREFIX}{timestamp}.csv"
    
    # Transform data
    transformer = WeatherTransformer(input_units=input_units)
    raw_data = transformer.load_json_data(input_file)
    transformed_df = transformer.transform_data(raw_data)
    
    # Save to CSV
    transformer.save_csv(transformed_df, output_file)
    
    # Load to database if requested
    if load_to_db:
        print("Loading data to database...")
        db_client = PostgresClient()
        db_client.create_weather_table()
        rows_inserted = db_client.load_dataframe(transformed_df, "weather_data")
        print(f"Loaded {rows_inserted} rows into database")
    
    return output_file


if __name__ == "__main__":
    try:
        output_file = run_transform_pipeline()
        print(f"Transformation pipeline completed successfully. Output saved to: {output_file}")
    except Exception as e:
        print(f"Error in transformation pipeline: {e}")
        raise 