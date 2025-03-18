FROM python:3.9-slim

WORKDIR /app

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directories
RUN mkdir -p /data/raw /data/processed /data/gold

# Set environment variable for data directory
ENV DATA_DIR="/data"

# Default command - can be overridden at runtime
CMD ["python", "-m", "src.bronze.weather_data_pipeline"] 