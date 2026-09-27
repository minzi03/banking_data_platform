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
    # HTTPS không nằm trong URI: driver trino của image Superset bỏ qua `?protocol=https`
    # (đo 2026-09-27: "TLS/SSL is required for authentication"). Nó được đặt qua
    # connect_args.http_scheme trong `extra` — xem add_trino_connection().
    return f"trino://{quote(user)}:{quote(password, safe='')}@{host}:{port}/{catalog}"


def add_trino_connection():
    """Add Trino as a data source in Superset (create or update)."""
    try:
        import json

        from superset.app import create_app

        # create_app() KHÔNG tham số. Bản trước gọi create_app("superset"): tham số đó
        # là tên MODULE CẤU HÌNH, nên Superset nạp nhầm module và chết KeyError
        # 'DATA_DIR' — script chưa từng chạy được, và init.sh nuốt lỗi bằng `|| echo`.
        app = create_app()
        with app.app_context():
            from superset.extensions import db as meta_db
            # Superset 3.1.3 đặt hàm này ở superset.utils.database, không ở utils.core.
            from superset.utils.database import get_or_create_db

            uri = trino_uri()
            # get_or_create_db tạo mới, hoặc CẬP NHẬT URI nếu kết nối đã có — đổi mật
            # khẩu hay sửa catalog tới được một Superset đã khởi tạo.
            database = get_or_create_db("Trino (Lakehouse)", uri)
            if os.environ.get("TRINO_PASSWORD") and os.environ.get("TRINO_CA_CERT"):
                # Cert tự ký của stack (ADR-0016) — verify bằng TRINO_CA_CERT.
                connect_args = {"http_scheme": "https", "verify": os.environ["TRINO_CA_CERT"]}
                database.extra = json.dumps({"engine_params": {"connect_args": connect_args}})
                meta_db.session.commit()
            logger.info(
                "Trino connection ready (user=%s, https=%s)",
                os.environ.get("TRINO_USER", "superset"),
                bool(os.environ.get("TRINO_PASSWORD")),
            )
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
