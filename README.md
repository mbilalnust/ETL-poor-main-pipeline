# ETL Pipeline for Weather Data

This project provides a modular ETL (Extract, Transform, Load) pipeline for weather data, following a medalion architecture with bronze, silver, and gold layers.

## Architecture

- **Bronze Layer**: Raw data extraction from OpenWeather API and S3
- **Silver Layer**: Data transformation and loading into PostgreSQL
- **Gold Layer**: Joining weather data with city demographic data

## Setup

1. Clone this repository
2. Create a `.env` file with the following environment variables:

```
# API Keys
OPENWEATHER_API_KEY=your_api_key_here

# AWS Credentials
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
S3_BUCKET_NAME=your_bucket_name

# Database Connection
DB_HOST=localhost
DB_PORT=5432
DB_NAME=weatherdb
DB_USER=postgres
DB_PASSWORD=your_db_password

# Data paths
DATA_DIR=/data
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Running with Docker

Build the Docker image:

```bash
docker build -t weather-etl .
```

Run the bronze layer (data extraction):

```bash
docker run -v $(pwd)/data:/data --env-file .env weather-etl python -m src.bronze.weather_data_pipeline
```

Run the silver layer (transformation):

```bash
docker run -v $(pwd)/data:/data --env-file .env weather-etl python -m src.silver.weather_transform_pipeline
```

Run the gold layer (joining data):

```bash
docker run -v $(pwd)/data:/data --env-file .env weather-etl python -m src.gold.city_weather_join_pipeline
```

### Running without Docker

Run the bronze layer (data extraction):

```bash
python -m src.bronze.weather_data_pipeline
```

Run the silver layer (transformation):

```bash
python -m src.silver.weather_transform_pipeline
```

Run the gold layer (joining data):

```bash
python -m src.gold.city_weather_join_pipeline
```

## Integration with Airflow

This codebase is designed to be containerized and run as tasks in an Airflow DAG. In the Airflow repository, you can define DAG tasks that run these containerized processes.

Example Airflow task:

```python
extract_weather_data = DockerOperator(
    task_id='extract_weather_data',
    image='weather-etl:latest',
    command='python -m src.bronze.weather_data_pipeline',
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    environment={
        'OPENWEATHER_API_KEY': '{{ var.value.openweather_api_key }}',
        'AWS_ACCESS_KEY_ID': '{{ var.value.aws_access_key_id }}',
        'AWS_SECRET_ACCESS_KEY': '{{ var.value.aws_secret_access_key }}',
        'S3_BUCKET_NAME': '{{ var.value.s3_bucket_name }}'
    },
    volumes=['/path/to/data:/data']
)
``` 