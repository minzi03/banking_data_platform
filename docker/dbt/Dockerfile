# =============================================================================
# Dockerfile — dbt for Banking Data Platform
# =============================================================================
# Architecture: Lakehouse 2.0
# Connection: Trino → Iceberg
# =============================================================================

FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Install dbt and dependencies
RUN pip install --no-cache-dir \
    dbt-core==1.12.0 \
    dbt-trino==1.8.0

# Set working directory
WORKDIR /opt/dbt

# Copy dbt project
COPY dbt/ .

# Create target directory
RUN mkdir -p target

# Default command
CMD ["dbt", "run"]
