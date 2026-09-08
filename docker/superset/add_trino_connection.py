#!/usr/bin/env python3
"""
Add Trino database connection to Apache Superset.
Called by init.sh during first startup.
"""
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def add_trino_connection():
    """Add Trino as a data source in Superset."""
    try:
        from flask import current_app
        from superset.app import create_app

        app = create_app("superset")
        with app.app_context():
            from superset.models.core import Database
            from superset.utils.core import get_or_create_db

            # Check if Trino connection already exists
            existing = Database.query.filter_by(database_name="Trino (Lakehouse)").first()
            if existing:
                logger.info("Trino connection already exists, skipping.")
                return

            # Create Trino connection
            trino_uri = "trino://admin@trino:8080/lakehouse"

            db = get_or_create_db(
                database_name="Trino (Lakehouse)",
                uri=trino_uri,
            )
            logger.info(f"Trino connection added: {db}")
    except ImportError:
        logger.warning("Could not import Superset modules. Connection will need to be configured via UI.")
    except Exception as e:
        logger.error(f"Failed to add Trino connection: {e}")
        logger.info("You can manually add Trino connection via Superset UI:")
        logger.info("  1. Go to Settings > Database Connections")
        logger.info("  2. Click + Database")
        logger.info("  3. Select Trino")
        logger.info("  4. Host: trino, Port: 8080, Database: lakehouse")


if __name__ == "__main__":
    time.sleep(5)
    add_trino_connection()
