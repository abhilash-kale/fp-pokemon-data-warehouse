# Use python 3.11 slim as the base image
FROM python:3.11-slim

# Python config
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/app/dbt_pokemon

# Set the working directory in the container
WORKDIR /app

# Install system dependencies (required for building certain Python packages)
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Ensure the data directory exists
RUN mkdir -p /app/data/raw /app/data/reports
