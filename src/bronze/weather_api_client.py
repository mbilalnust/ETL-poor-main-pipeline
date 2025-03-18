from datetime import datetime
import requests
from typing import Dict, List, Optional
from dotenv import load_dotenv
import os
import time
import json

# Load environment variables from .env file
load_dotenv()

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
    
    def get_batch_weather_data(self, cities: List[str], delay: float = 1.0) -> List[Dict]:
        """
        Fetch weather data for multiple cities with rate limiting
        
        Args:
            cities: List of city names
            delay: Delay between API calls in seconds (to avoid rate limiting)
            
        Returns:
            List of weather data dictionaries (only successful responses)
        """
        results = []
        
        for city in cities:
            data = self.get_weather_data(city)
            if data:
                results.append(data)
                
            # Sleep to avoid hitting API rate limits
            if delay > 0 and city != cities[-1]:
                time.sleep(delay)
                
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
    
    def save_to_json(self, data: List[Dict], filename: str) -> bool:
        """
        Save weather data to JSON file
        
        Args:
            data: List of weather data dictionaries
            filename: Output JSON filename
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception as e:
            print(f"Error saving data to {filename}: {e}")
            return False 