# =============================================================================
# Apache Superset — Custom Image for Banking Data Platform
# =============================================================================
FROM apache/superset:3.1.3

USER root

# Install Trino SQLAlchemy driver
RUN pip install --no-cache-dir \
    trino \
    sqlparse \
    flask-caching \
    flask-session

# Copy custom configuration
COPY superset_config.py /app/pythonpath/superset_config.py
COPY init.sh /app/docker/init.sh
COPY add_trino_connection.py /app/docker/superset/add_trino_connection.py

# Make init script executable
RUN chmod +x /app/docker/init.sh

# Copy dashboard definitions
COPY dashboards/ /app/dashboards/

USER superset
