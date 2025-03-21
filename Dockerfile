FROM python:3.9-slim

WORKDIR /app

# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set up virtual environment
ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip

# Install workspace requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code (excluding .env and other sensitive files)
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="${PYTHONPATH}:/app"