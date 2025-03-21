FROM python:3.9-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Create a virtual environment and install dependencies
RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Final stage
FROM python:3.9-slim

WORKDIR /app

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder stage
COPY --from=builder /venv /venv
ENV PATH="/venv/bin:$PATH"

# Create data directories
RUN mkdir -p /data/raw /data/processed /data/gold

# Set environment variable for data directory
ENV DATA_DIR="/data"
ENV PYTHONUNBUFFERED=1

# Copy application code
COPY . .

# Add health check that doesn't rely on a web server
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys, pandas, duckdb, boto3; sys.exit(0)" || exit 1

# Default command - can be overridden at runtime
CMD ["python", "-m", "src.bronze.weather_data_pipeline"] 