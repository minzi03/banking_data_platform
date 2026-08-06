# =============================================================================
# dbt Dockerfile — Banking Data Platform Semantic Layer
# =============================================================================
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libffi-dev git && \
    rm -rf /var/lib/apt/lists/*

# Install dbt with Trino adapter
RUN pip install --no-cache-dir \
    dbt-core==1.12.0 \
    dbt-trino==1.9.0

WORKDIR /usr/src/dbt

# Copy project files
COPY profiles.yml /root/.dbt/profiles.yml
COPY dbt_project.yml .
COPY packages.yml .

# Install dbt packages
RUN dbt deps

# Default command
CMD ["dbt", "debug", "--target", "docker"]
