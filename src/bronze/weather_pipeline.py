import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import List, Dict, Optional, Any
from pathlib import Path


# Load environment variables
load_dotenv()

# Configure output paths
DATA_DIR = os.getenv("DATA_DIR", "/data")
RAW_DATA_DIR = f"{DATA_DIR}/raw"
OUTPUT_FILE_PREFIX = "weather_data_"

# List of major Korean cities
KOREAN_CITIES = [
    "Seoul,KR", "Busan,KR", "Incheon,KR", "Daegu,KR", "Daejeon,KR", 
    "Gwangju,KR", "Suwon,KR", "Ulsan,KR", "Seongnam,KR", "Goyang,KR",
    "Bucheon,KR", "Cheongju,KR", "Jeonju,KR", "Ansan,KR", "Anyang,KR",
    "Changwon,KR", "Jeju,KR", "Pohang,KR", "Gimhae,KR", "Chuncheon,KR"
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


def ensure_dir_exists(directory: str) -> None:
    """
    Ensure that a directory exists, creating it if necessary
    
    Args:
        directory: Directory path to check/create
    """
    Path(directory).mkdir(parents=True, exist_ok=True)


def run_weather_data_pipeline(
    cities: List[str],
    output_dir: Optional[str] = None,
    date_id: Optional[str] = None
) -> str:
    """
    Main pipeline function to collect current weather data
    
    Args:
        cities: List of cities to get weather data for
        output_dir: Directory to save the output file
        date_id: Date identifier in YYYY-MM-DD format (for Airflow integration)
        
    Returns:
        Path to the output file
    """
    # Setup output directory
    if output_dir is None:
        output_dir = RAW_DATA_DIR
    
    ensure_dir_exists(output_dir)
    
    # Use provided date_id or generate from current date
    if date_id is None:
        date_id = datetime.now().strftime("%Y-%m-%d")
    
    # Generate output filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{output_dir}/{OUTPUT_FILE_PREFIX}{timestamp}.json"
    
    if not cities or len(cities) == 0:
        raise ValueError("City list is empty")
    
    print(f"Processing {len(cities)} cities for date_id: {date_id}...")
    
    # Fetch weather data for cities
    weather_client = WeatherAPIClient()
    weather_data = weather_client.get_batch_weather_data(cities)
    
    # Extract relevant metrics and add date_id
    processed_data = []
    for data in weather_data:
        metrics = weather_client.extract_weather_metrics(data)
        metrics['date_id'] = date_id
        processed_data.append(metrics)
    
    # Save data to file
    print(f"Saving data to {output_file}...")
    with open(output_file, 'w') as f:
        json.dump(processed_data, f, indent=2)
    
    print(f"Successfully processed {len(processed_data)} cities")
    return output_file


if __name__ == "__main__":
    try:
        # Use a fixed list of 10 Korean cities
        korean_cities = ["Seoul,KR", "Busan,KR", "Incheon,KR", "Daegu,KR", "Daejeon,KR", 
                         "Gwangju,KR", "Suwon,KR", "Ulsan,KR", "Seongnam,KR", "Goyang,KR"]
        print(f"Processing cities: {', '.join(korean_cities)}")
        
        # Use current date as date_id if not provided as an argument
        # In production, this would be passed from Airflow
        date_id = datetime.now().strftime("%Y-%m-%d")
        
        output_file = run_weather_data_pipeline(korean_cities, date_id=date_id)
        print(f"Data pipeline completed successfully. Output saved to: {output_file}")
    except Exception as e:
        print(f"Error in weather data pipeline: {e}")
        raise 