#!/usr/bin/env python3
"""
Add Trino database connection to Apache Superset.
Called by init.sh during first startup.
"""
import logging
import os
import time
from urllib.parse import quote

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def trino_uri() -> str:
    """URI SQLAlchemy cho Trino — ADR-0016.

    Có TRINO_PASSWORD → HTTPS 8443 + mật khẩu; không có → HTTP 8080 như trước.
    Catalog là `iceberg` — tên PHÍA TRINO (ADR-0002). Bản trước dùng `lakehouse`,
    tên phía Spark, mà Trino không có: kết nối tạo ra không đọc được bảng nào.
    """
    user = os.environ.get("TRINO_USER", "superset")
    password = os.environ.get("TRINO_PASSWORD")
    host = os.environ.get("TRINO_HOST", "trino")
    catalog = os.environ.get("TRINO_CATALOG", "iceberg")
    if not password:
        return f"trino://{quote(user)}@{host}:{os.environ.get('TRINO_PORT', '8080')}/{catalog}"
    port = os.environ.get("TRINO_PORT", "8443")
    return f"trino://{quote(user)}:{quote(password, safe='')}@{host}:{port}/{catalog}?protocol=https"


def add_trino_connection():
    """Add Trino as a data source in Superset."""
    try:
        from flask import current_app
        from superset.app import create_app

        app = create_app("superset")
        with app.app_context():
            from superset.models.core import Database
            from superset.utils.core import get_or_create_db

            uri = trino_uri()
            # Cert tự ký của stack (ADR-0016) — verify bằng TRINO_CA_CERT.
            extra = {"engine_params": {"connect_args": {"verify": os.environ["TRINO_CA_CERT"]}}} if (
                os.environ.get("TRINO_PASSWORD") and os.environ.get("TRINO_CA_CERT")
            ) else None

            # Kết nối đã có thì CẬP NHẬT — bản trước bỏ qua, nên đổi mật khẩu hay sửa
            # catalog không bao giờ tới được một Superset đã khởi tạo.
            db = get_or_create_db(database_name="Trino (Lakehouse)", uri=uri)
            db.set_sqlalchemy_uri(uri)
            if extra is not None:
                import json

                db.extra = json.dumps(extra)
            from superset.extensions import db as meta_db

            meta_db.session.commit()
            logger.info("Trino connection ready (user=%s, https=%s)", os.environ.get("TRINO_USER", "superset"), bool(os.environ.get("TRINO_PASSWORD")))
    except ImportError:
        logger.warning("Could not import Superset modules. Connection will need to be configured via UI.")
    except Exception as e:
        logger.error(f"Failed to add Trino connection: {e}")
        logger.info("You can manually add Trino connection via Superset UI:")
        logger.info("  1. Go to Settings > Database Connections")
        logger.info("  2. Click + Database")
        logger.info("  3. Select Trino")
        logger.info("  4. Host: trino, Port: 8443 (HTTPS, có mật khẩu) hoặc 8080, Database: iceberg")


if __name__ == "__main__":
    time.sleep(5)
    add_trino_connection()
