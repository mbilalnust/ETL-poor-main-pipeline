import os
import time
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, Optional
from sqlalchemy import text

from utils.duckdb_utils import get_duckdb_connection
from utils.config import get_postgres_engine
from utils.postgres_utils import upload_to_postgres

# Load environment variables
load_dotenv()

def process_north_america_weather(date_id: str) -> None:
    """
    Combine Canadian and US weather data into a North America weather table in PostgreSQL
    
    Args:
        date_id: Date identifier in YYYY-MM-DD format
    """
    print(
        time.strftime("%H:%M:%S"),
        "process_north_america_weather starts",
        f"date_id: {date_id}"
    )
    
    # Create DuckDB connection to read source data
    con = get_duckdb_connection()
    
    # Define S3 path for the source tables
    bucket_name = os.getenv("S3_BUCKET_NAME")
    canada_s3_path = f"s3://{bucket_name}/analytics/canada_weather/date_id={date_id}/data.parquet"
    us_s3_path = f"s3://{bucket_name}/analytics/us_weather/date_id={date_id}/data.parquet"
    
    # SQL to combine Canadian and US weather data using UNION ALL
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
        temperature_category,
        'North America' as region
    FROM '{canada_s3_path}'
    
    UNION ALL
    
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
        temperature_category,
        'North America' as region
    FROM '{us_s3_path}'
    """
    
    # Execute the query and get the result as a DataFrame
    print("Executing UNION ALL transformation query...")
    df = con.execute(query).fetchdf()
    
    # Check if we have data
    if df.empty:
        print(f"No North American weather data found for date_id: {date_id}")
        return
    
    print(f"Combined {len(df)} rows of North American weather data")
    
    # Get PostgreSQL engine (single connection for all operations)
    engine = get_postgres_engine()
    
    try:
        # Delete existing data for this date_id
        with engine.begin() as connection:
            print(f"Deleting existing data from dashboard.north_america_weather where date_id='{date_id}'")
            delete_query = text(f"DELETE FROM dashboard.north_america_weather WHERE date_id = :date_id")
            connection.execute(delete_query, {"date_id": date_id})
        
        # Insert data using pandas method
        upload_to_postgres(
            df=df,
            engine=engine,
            table_name="north_america_weather"
        )
    except Exception as e:
        print(f"Error in PostgreSQL operations: {e}")
        raise
    
    print(
        time.strftime("%H:%M:%S"),
        "process_north_america_weather ends",
        f"date_id: {date_id}"
    )


if __name__ == "__main__":
    try:
        # Use today's date as default
        today = datetime.now().strftime('%Y-%m-%d')
        process_north_america_weather(today)
    except Exception as e:
        print(f"Error in North America weather data pipeline: {e}")
        raise 