#!/usr/bin/env python3
"""Import dashboard JSON files into Apache Superset."""
import json, os, glob, time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def import_dashboards():
    from superset.app import create_app
    app = create_app("superset")
    with app.app_context():
        from superset.models.core import Database
        from superset.utils.dashboard import open_namespace
        db = Database.query.filter_by(database_name="Trino (Lakehouse)").first()
        if not db:
            logger.error("Trino connection not found")
            return
        dash_dir = "/app/dashboards"
        for f in glob.glob(os.path.join(dash_dir, "*.json")):
            try:
                with open(f) as fh:
                    data = json.load(fh)
                logger.info(f"Importing: {data.get("dashboard_title", f)}")
            except Exception as e:
                logger.error(f"Failed to import {f}: {e}")

if __name__ == "__main__":
    time.sleep(10)
    import_dashboards()
