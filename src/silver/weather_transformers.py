import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Union
import os


def kelvin_to_fahrenheit(temp_in_kelvin: float) -> float:
    """
    Convert temperature from Kelvin to Fahrenheit
    
    Args:
        temp_in_kelvin: Temperature in Kelvin
        
    Returns:
        Temperature in Fahrenheit
    """
    return round((temp_in_kelvin - 273.15) * 9/5 + 32, 3)


def celsius_to_fahrenheit(temp_in_celsius: float) -> float:
    """
    Convert temperature from Celsius to Fahrenheit
    
    Args:
        temp_in_celsius: Temperature in Celsius
        
    Returns:
        Temperature in Fahrenheit
    """
    return round((temp_in_celsius * 9/5) + 32, 3)


class WeatherTransformer:
    """Transform raw weather data into structured format"""
    
    def __init__(self, input_units: str = "metric"):
        """
        Initialize the transformer
        
        Args:
            input_units: Units of the input data ("metric", "imperial", "kelvin")
        """
        self.input_units = input_units
    
    def convert_temperature(self, temp: float) -> float:
        """
        Convert temperature to Fahrenheit based on input units
        
        Args:
            temp: Temperature in input units
            
        Returns:
            Temperature in Fahrenheit
        """
        if self.input_units == "metric":
            return celsius_to_fahrenheit(temp)
        elif self.input_units == "kelvin":
            return kelvin_to_fahrenheit(temp)
        else:  # Already in imperial/Fahrenheit
            return temp
    
    def transform_data(self, data: Union[str, List[Dict], pd.DataFrame]) -> pd.DataFrame:
        """
        Transform raw weather data into standardized format
        
        Args:
            data: Raw weather data (file path, JSON string, list of dicts, or DataFrame)
            
        Returns:
            DataFrame with transformed data
        """
        # Load data if it's a file path
        if isinstance(data, str):
            if os.path.isfile(data):
                with open(data, 'r') as f:
                    data = json.load(f)
            else:
                # Assume it's a JSON string
                data = json.loads(data)
        
        # Convert list of dicts to DataFrame if needed
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data
        
        # Ensure required columns exist
        required_columns = ["city", "temperature", "feels_like", "humidity", "timestamp"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Apply transformations
        transformed_df = pd.DataFrame()
        transformed_df["city"] = df["city"]
        transformed_df["description"] = df["weather"]
        transformed_df["temperature_fahrenheit"] = df["temperature"].apply(self.convert_temperature)
        transformed_df["feels_like_fahrenheit"] = df["feels_like"].apply(self.convert_temperature)
        
        # Copy other metrics directly
        if "humidity" in df.columns:
            transformed_df["humidity"] = df["humidity"]
        if "pressure" in df.columns:
            transformed_df["pressure"] = df["pressure"]
        if "wind_speed" in df.columns:
            transformed_df["wind_speed"] = df["wind_speed"]
        
        # Convert timestamp if it's a string
        if df["timestamp"].dtype == object:
            transformed_df["time_of_record"] = pd.to_datetime(df["timestamp"])
        else:
            transformed_df["time_of_record"] = df["timestamp"]
        
        return transformed_df
    
    @staticmethod
    def load_json_data(file_path: str) -> List[Dict]:
        """
        Load data from JSON file
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            List of dictionaries with weather data
        """
        with open(file_path, 'r') as f:
            return json.load(f)
    
    def save_csv(self, df: pd.DataFrame, output_path: str) -> None:
        """
        Save transformed data to CSV
        
        Args:
            df: DataFrame with transformed data
            output_path: Path to save the CSV file
        """
        df.to_csv(output_path, index=False)
        print(f"Saved transformed data to {output_path}")
    
    @staticmethod
    def find_latest_data_file(directory: str, prefix: str = "weather_data_") -> Optional[str]:
        """
        Find the latest data file in a directory
        
        Args:
            directory: Directory to search
            prefix: Prefix of the files to search for
            
        Returns:
            Path to the latest file or None if no files found
        """
        files = [os.path.join(directory, f) for f in os.listdir(directory) 
                if f.startswith(prefix) and f.endswith(".json")]
        
        if not files:
            return None
            
        return max(files, key=os.path.getctime) 