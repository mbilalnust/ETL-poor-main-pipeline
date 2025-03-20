import os
import time
import duckdb
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, Optional

from utils.duckdb_utils import duck_db_parquet_delete_and_insert, get_duckdb_connection

# Load environment variables
load_dotenv()

def process_us_weather(date_id: str) -> None:
    """
    Process world weather data to extract and transform Canadian cities data
    
    Args:
        date_id: Date identifier in YYYY-MM-DD format
    """
    print(
        time.strftime("%H:%M:%S"),
        "process_us_weather starts",
        f"date_id: {date_id}"
    )
    
    # Create DuckDB connection
    con = get_duckdb_connection()
    
    # Define S3 path for the source table
    bucket_name = os.getenv("S3_BUCKET_NAME")
    source_s3_path = f"s3://{bucket_name}/analytics/world_weather"
    
    # SQL to extract and transform Canadian weather data
    query = f"""
    SELECT
        city,
        country,
        temperature,
        feels_like,
        weather,
        weather_code,
        wind_speed,
        timestamp,
        '{date_id}' as date_id,
        CASE
            WHEN temperature < 0 THEN 'Freezing'
            WHEN temperature < 10 THEN 'Cold'
            WHEN temperature < 20 THEN 'Mild'
            ELSE 'Warm'
        END as temperature_category
    FROM '{source_s3_path}/date_id={date_id}/data.parquet'
    WHERE country = 'US'
    """
    
    # Execute the query and get the result as a DataFrame
    print("Executing transformation query...")
    df = con.execute(query).fetchdf()
    
    # Check if we have data
    if df.empty:
        print(f"No Canadian cities data found for date_id: {date_id}")
        return
    
    print(f"Transformed {len(df)} rows of Canadian weather data")
    
    # Database and table configuration
    database = "analytics"
    table_name = "us_weather"
    
    # Schema definition for the table
    US_WEATHER_SCHEMA = {
        "city": "VARCHAR",
        "country": "VARCHAR",
        "temperature": "DOUBLE",
        "feels_like": "DOUBLE",
        "weather": "VARCHAR",
        "weather_code": "INTEGER",
        "wind_speed": "DOUBLE",
        "timestamp": "VARCHAR",
        "date_id": "VARCHAR",
        "temperature_category": "VARCHAR"
    }
    
    # Save data to Parquet files
    duck_db_parquet_delete_and_insert(
        database=database,
        table=table_name,
        date_id=date_id,
        data=df,
        schema=US_WEATHER_SCHEMA
    )
    
    print(
        time.strftime("%H:%M:%S"),
        "process_US_weather ends",
        f"date_id: {date_id}"
    )


if __name__ == "__main__":
    try:
        # Use today's date as default
        today = datetime.now().strftime('%Y-%m-%d')
        process_us_weather(today)
    except Exception as e:
        print(f"Error in us weather data pipeline: {e}")
        raise 