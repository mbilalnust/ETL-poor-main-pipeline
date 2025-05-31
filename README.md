# ETL Pipeline for Weather Data

This project implements a cost-effective "poor man's ETL pipeline" for weather data, following a medallion architecture with bronze, silver, and gold layers. Data is processed daily and moves through a modern data lakehouse architecture.

## Architecture

- **Bronze Layer**: Raw data extraction from OpenWeather API and ingestion into S3 data lake with Apache Iceberg format
- **Silver Layer**: Data transformation using DuckDB and storage back to S3 as Iceberg tables
- **Gold Layer**: Analytics-ready data loaded into PostgreSQL for serving applications

## Output
![Data Pipeline Diagram](https://github.com/mbilalnust/ETL-poor-main-pipeline/blob/main/1744553596425.jpeg?raw=true)
## Key Features

- **Daily weather data ingestion** from OpenWeather API
- **S3 data lake** configured with AWS Glue Catalog and Apache Iceberg table format
- **Delete and insert pattern** for daily data refreshes
- **CI/CD with GitHub Actions** for automatic Docker image building and publishing
- **Airflow integration** for scheduled execution
- **Cost-effective design** using open-source components
- **Extensible architecture** that can be upgraded to Athena or Spark in the future

## Data Flow

1. Daily weather data is extracted from the OpenWeather API
2. Raw data is ingested into the S3 data lake (bronze layer) in Apache Iceberg format
3. Existing data for that day is deleted before new data is inserted (delete-insert pattern)
4. Data is transformed using DuckDB and stored back in S3 as Iceberg tables (silver layer)
5. Final data products are loaded into PostgreSQL (gold layer) for serving applications

## Setup

1. Clone this repository
2. Create a `.env` file with the following environment variables:

```
# API Keys
OPENWEATHER_API_KEY=your_api_key_here

# AWS Credentials
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=your_aws_region
S3_BUCKET_NAME=your_bucket_name

# Database Connection
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=weatheruser
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_DB=weather_db
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
docker run --env-file .env weather-etl python -m src.bronze.weather_data_pipeline
```

Run the silver layer (transformation):

```bash
docker run --env-file .env weather-etl python -m src.silver.weather_transform_pipeline
```

Run the gold layer (joining data):

```bash
docker run --env-file .env weather-etl python -m src.gold.city_weather_join_pipeline
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

## Docker and CI/CD Setup

### Manual Docker Build and Push

You can manually build and push the Docker image to Docker Hub with these commands:

```bash
# Build the image
docker build -t your-dockerhub-username/weather-etl:latest .

# Push to Docker Hub
docker login
docker push your-dockerhub-username/weather-etl:latest
```

### GitHub Actions CI/CD Pipeline

This project includes a GitHub Actions workflow that automatically builds and pushes the Docker image to Docker Hub:

1. **Set up secrets in GitHub repository:**
   - Go to your repository on GitHub
   - Navigate to Settings > Secrets and variables > Actions
   - Add these repository secrets:
     - `DOCKERHUB_USERNAME`: Your Docker Hub username
     - `DOCKERHUB_TOKEN`: Your Docker Hub access token (create one at https://hub.docker.com/settings/security)

2. **Automated builds:**
   - Every push to `main` or `master` branch triggers a build and push to Docker Hub
   - When you create a tag (e.g., v1.0.0), it creates a tagged Docker image
   - Pull requests build the image but don't push it

3. **Using in Airflow:**
   - In your Airflow DAGs, reference the image as: `your-dockerhub-username/weather-etl:latest`
   - For specific versions, use the tag: `your-dockerhub-username/weather-etl:v1.0.0`

## Integration with Airflow

The containerized pipeline is designed to be run as tasks in an Airflow DAG. Each bronze and silver table has its own script and task in the pipeline.

Example Airflow task for a bronze layer table:

```python
from airflow.providers.docker.operators.docker import DockerOperator
from utils.config import get_environment_config

# Using a common image for all ETL tasks
WEATHER_ETL_IMAGE = "your-dockerhub-username/weather-etl:latest"

# Task for processing city weather data in bronze layer
city_weather_bronze = DockerOperator(
    task_id="city_weather_bronze",
    image=WEATHER_ETL_IMAGE,
    command='python -m src.bronze.city_weather_pipeline',
    api_version='auto',
    auto_remove=True,
    environment=get_environment_config(),  # Configuration from utility module
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge'
)

# This task would be part of a larger DAG with multiple bronze and silver tasks
# Each table or data entity would have its own specialized ETL script
```

The actual Airflow DAG would contain many similar tasks for different data entities, each with specific scripts for the bronze, silver, and gold layers.

## Future Extensibility

This project is designed as a "poor man's ETL pipeline" that can easily scale on local computer or one ec2 instance:

- **Replace DuckDB**: The transformation layer can be extended to use AWS Athena or Apache Spark for larger data volumes
- **Scale Storage**: S3 + Iceberg provides virtually unlimited scalable storage
- **Add Data Sources**: The modular design allows for easy addition of new data sources
- **Real-time Processing**: Can be extended to include streaming data with minimal changes 

## Overall flow
+-----------------------+
|      Analyst / BI     |
|  (SQL tools, Notebooks)|
+-----------+-----------+
            |
            v
+-----------------------+
|    Query Engine       |  <-- Spark, Presto, Trino, Athena (with Iceberg support)
+-----------------------+
            |
            v
+-----------------------+
|    Metadata Catalog   |  <-- AWS Glue Catalog or Hive Metastore
| - Table schemas       |
| - Table locations     |
| - Table ownership     |
+-----------+-----------+
            |
            v
+-----------------------+
|    Iceberg Table      |
| - Metadata files      |  <-- Tracks data files, partitions, snapshots, versions
| - Data files (Parquet, ORC, etc.)  | <-- Stored in S3, HDFS, or other object storage
+-----------------------+
            |
            v
+-----------------------+
|      Cloud Storage    |  <-- Amazon S3, Azure Blob, GCS
+-----------------------+

