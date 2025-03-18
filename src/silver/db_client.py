import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from typing import Dict, List, Optional, Union, Tuple
import io

# Load environment variables
load_dotenv()

class PostgresClient:
    """Client for interacting with PostgreSQL database"""
    
    def __init__(self):
        """Initialize database connection parameters from environment variables"""
        self.db_params = {
            "host": os.getenv("DB_HOST", "localhost"),
            "database": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "port": os.getenv("DB_PORT", "5432")
        }
        
        # Validate required parameters
        missing_params = [k for k, v in self.db_params.items() if v is None]
        if missing_params:
            raise ValueError(f"Missing database parameters: {missing_params}")
    
    def get_connection(self) -> psycopg2.extensions.connection:
        """
        Get a database connection
        
        Returns:
            PostgreSQL connection object
        """
        return psycopg2.connect(**self.db_params)
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> None:
        """
        Execute a SQL query
        
        Args:
            query: SQL query to execute
            params: Query parameters (optional)
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
            conn.commit()
    
    def create_weather_table(self) -> None:
        """Create the weather data table if it doesn't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            city TEXT NOT NULL,
            description TEXT,
            temperature_fahrenheit NUMERIC,
            feels_like_fahrenheit NUMERIC,
            humidity NUMERIC,
            pressure NUMERIC,
            wind_speed NUMERIC,
            time_of_record TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.execute_query(create_table_sql)
        print("Created weather_data table (if it didn't exist)")
    
    def load_dataframe(self, df: pd.DataFrame, table_name: str) -> int:
        """
        Load DataFrame data into a database table
        
        Args:
            df: DataFrame with data to load
            table_name: Target table name
            
        Returns:
            Number of rows inserted
        """
        # Get column names from DataFrame
        columns = list(df.columns)
        
        # Build the SQL INSERT statement
        insert_stmt = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES %s
        """
        
        # Convert DataFrame to list of tuples
        values = [tuple(row) for row in df.values]
        
        # Execute the insert
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_stmt, values)
            conn.commit()
        
        return len(values)
    
    def load_csv(self, csv_file: str, table_name: str) -> int:
        """
        Load data from a CSV file into a database table using COPY
        
        Args:
            csv_file: Path to the CSV file
            table_name: Target table name
            
        Returns:
            Number of rows inserted
        """
        # Count number of lines in file to return rows inserted
        with open(csv_file, 'r') as f:
            row_count = sum(1 for _ in f) - 1  # Subtract 1 for header
        
        # Use COPY to load the data
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                with open(csv_file, 'r') as f:
                    next(f)  # Skip header
                    cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", f)
            conn.commit()
        
        return row_count
    
    def truncate_table(self, table_name: str) -> None:
        """
        Truncate a database table
        
        Args:
            table_name: Name of the table to truncate
        """
        self.execute_query(f"TRUNCATE TABLE {table_name};")
        print(f"Truncated table {table_name}")
    
    def query_to_dataframe(self, query: str, params: Optional[Tuple] = None) -> pd.DataFrame:
        """
        Execute a query and return results as a DataFrame
        
        Args:
            query: SQL query to execute
            params: Query parameters (optional)
            
        Returns:
            DataFrame with query results
        """
        with self.get_connection() as conn:
            return pd.read_sql_query(query, conn, params=params) 