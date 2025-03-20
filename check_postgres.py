from utils.config import get_postgres_engine
import pandas as pd

try:
    engine = get_postgres_engine()
    connection = engine.connect()
    print('Successfully connected to PostgreSQL!')
    
    # Get schemas
    print('\nSchemas:')
    schemas = pd.read_sql('SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname !~ \'^pg_\' AND nspname <> \'information_schema\';', connection)
    print(schemas)
    
    # Get tables
    print('\nTables:')
    tables = pd.read_sql('SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname NOT IN (\'pg_catalog\', \'information_schema\');', connection)
    print(tables)
    
    # Get table columns for each table
    for index, row in tables.iterrows():
        schema = row['schemaname']
        table = row['tablename']
        print(f"\nColumns for {schema}.{table}:")
        columns_query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table}' ORDER BY ordinal_position;"
        columns = pd.read_sql(columns_query, connection)
        print(columns)
        
        # Get sample data
        print(f"\nSample data from {schema}.{table} (first 5 rows):")
        sample_query = f"SELECT * FROM {schema}.{table} LIMIT 5;"
        try:
            sample_data = pd.read_sql(sample_query, connection)
            print(sample_data)
        except Exception as e:
            print(f"Error getting sample data: {e}")
    
    connection.close()
except Exception as e:
    print(f'Error connecting to PostgreSQL: {e}') 