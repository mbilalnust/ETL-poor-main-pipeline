import os
import json
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import List, Dict, Optional, Any, Union
from pathlib import Path
# NOTE: The following packages need to be installed:
#   pip install duckdb pandas pyarrow
import pandas as pd  # type: ignore

from utils.duckdb_utils import duck_db_parquet_delete_and_insert, get_duckdb_connection

# Load environment variables
load_dotenv()

# List of major world cities from 6 countries
WORLD_CITIES = [
    # United States (US)
    "New York,US", "Los Angeles,US", "Chicago,US", "Houston,US", "Phoenix,US",
    # United Kingdom (GB)
    "London,GB", "Manchester,GB", "Birmingham,GB", "Glasgow,GB", "Liverpool,GB", 
    # Japan (JP)
    "Tokyo,JP", "Osaka,JP", "Yokohama,JP", "Nagoya,JP", "Sapporo,JP",
    # Australia (AU)
    "Sydney,AU", "Melbourne,AU", "Brisbane,AU", "Perth,AU", "Adelaide,AU",
    # Germany (DE)
    "Berlin,DE", "Munich,DE", "Hamburg,DE", "Frankfurt,DE", "Cologne,DE",
    # Canada (CA)
    "Toronto,CA", "Vancouver,CA", "Montreal,CA", "Calgary,CA", "Ottawa,CA"
]

class WeatherAPIClient:
    """Client for the OpenWeather API"""
    
    def __init__(self):
        """Initialize the Weather API client with API key from environment variables"""
        self.api_key = os.getenv("OPENWEATHER_API_KEY")
        if not self.api_key:
            raise ValueError("Missing OPENWEATHER_API_KEY in .env file")
            
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"
        self.units = "metric"
        
    def get_weather_data(self, city: str) -> Optional[Dict]:
        """
        Fetch current weather data for a city
        
        Args:
            city: City name and country code (e.g., "London,UK")
            
        Returns:
            Dictionary containing weather data or None on failure
        """
        try:
            params = {
                "q": city,
                "units": self.units,
                "appid": self.api_key
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Add metadata
            data["retrieved_at"] = datetime.now().isoformat()
            
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"API Request failed for {city}: {e}")
            return None
        except KeyError as e:
            print(f"Unexpected response format for {city}: {e}")
            return None
    
    def get_batch_weather_data(self, cities: List[str]) -> List[Dict]:
        """
        Fetch weather data for multiple cities
        
        Args:
            cities: List of city names
            
        Returns:
            List of weather data dictionaries
        """
        results = []
        for city in cities:
            data = self.get_weather_data(city)
            if data:
                results.append(data)
        return results
        
    def extract_weather_metrics(self, data: Dict) -> Dict:
        """
        Extract key weather metrics from API response
        
        Args:
            data: Raw API response
            
        Returns:
            Dictionary with extracted weather metrics
        """
        try:
            return {
                "city": data["name"],
                "country": data["sys"]["country"],
                "temperature": data["main"]["temp"],
                "feels_like": data["main"]["feels_like"],
                "humidity": data["main"]["humidity"],
                "pressure": data["main"]["pressure"],
                "weather": data["weather"][0]["description"],
                "weather_code": data["weather"][0]["id"],
                "wind_speed": data["wind"]["speed"],
                "timestamp": data["retrieved_at"]
            }
        except KeyError as e:
            print(f"Error extracting metrics: {e}")
            return {}


def insert_world_weather_daily(date_id: str):
    """
    Process and insert world weather data for a specific date
    
    Args:
        date_id: Date identifier in YYYY-MM-DD format
    """
    print(
        time.strftime("%H:%M:%S"),
        "insert_world_weather_daily starts",
        f"date_id: {date_id}"
    )
    
    
    # Fetch weather data for world cities
    weather_client = WeatherAPIClient()
    weather_data = weather_client.get_batch_weather_data(WORLD_CITIES)
    
    # Process weather data
    processed_data = []
    for data in weather_data:
        metrics = weather_client.extract_weather_metrics(data)
        metrics['date_id'] = date_id
        processed_data.append(metrics)
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(processed_data)
    
    # Database and table configuration
    database = "analytics"
    # Table name
    TABLE_NAME = "world_weather"

    # Schema definition for the table
    WEATHER_SCHEMA = {
        "city": "VARCHAR",
        "country": "VARCHAR",
        "temperature": "DOUBLE",
        "feels_like": "DOUBLE",
        "humidity": "INTEGER",
        "pressure": "INTEGER",
        "weather": "VARCHAR",
        "weather_code": "INTEGER",
        "wind_speed": "DOUBLE",
        "timestamp": "VARCHAR",
        "date_id": "VARCHAR"
    }
    
    # Save data to Parquet files
    duck_db_parquet_delete_and_insert(
        database=database,
        table=TABLE_NAME,
        date_id=date_id,
        data=df,
        schema=WEATHER_SCHEMA
    )
    
    print(
        time.strftime("%H:%M:%S"),
        "insert_world_weather_daily ends",
        f"date_id: {date_id}"
    )


if __name__ == "__main__":
    try:
        # Use today's date as default
        today = datetime.now().strftime('%Y-%m-%d')
        insert_world_weather_daily(today)
    except Exception as e:
        print(f"Error in weather data pipeline: {e}")
        raise 