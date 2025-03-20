import io
import pandas as pd  # type: ignore
from typing import Optional
from sqlalchemy import text

def upload_to_postgres(df: pd.DataFrame, engine, table_name: str, schema: str = "dashboard"):
    """
    Simple data loading using pandas to_sql method
    
    Args:
        df: DataFrame with data to insert
        engine: SQLAlchemy engine
        table_name: PostgreSQL table name
        schema: PostgreSQL schema name (default: dashboard)
    """
    if df.empty:
        print("No data to insert, operation completed")
        return
    
    print(f"Inserting {len(df)} rows into {schema}.{table_name} using pandas to_sql")
    
    # Ensure date_id is string and no longer than 10 characters
    if 'date_id' in df.columns:
        df['date_id'] = df['date_id'].astype(str).str[:10]
    
    # Get a fresh connection from the engine pool
    with engine.begin() as connection:
        try:
            # Use pandas to_sql method for simple data insertion
            df.to_sql(
                name=table_name,
                con=connection,
                schema=schema,
                if_exists='append',
                index=False,
                method='multi'
            )
            print(f"Successfully inserted data into {schema}.{table_name}")
        except Exception as e:
            print(f"Error inserting data: {e}")
            raise 