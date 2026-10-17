"""
Shared JDBC connection utilities cho Bronze/Silver/Gold DAGs.

Quy ước JDBC connection trong Airflow UI:
  - conn_type : jdbc / postgres / oracle
  - Host      : host (hoặc full JDBC URL khi conn_type = jdbc)
  - Port      : 5432 (Postgres) · 1521 (Oracle)
  - Schema    : banking_db (Postgres) · tên SERVICE_NAME (Oracle)
  - Login     : banking_admin
  - Password  : $POSTGRES_PASSWORD / $ORACLE_PASSWORD (docker/.env)

Vì sao cần conn_type (Khoá new2 — C2):
  Core Banking truyền thống chạy trên **Oracle** (VietinBank, BIDV, ABBANK…),
  Digital Banking chạy trên **PostgreSQL**. Platform phải nạp được cả hai trong
  cùng một Bronze layer. Trước đây URL sinh ra luôn cứng `jdbc:postgresql://`,
  nên một connection Oracle sẽ bị gửi cho driver Postgres và chết lúc chạy —
  hỏng im lặng: DAG parse xong, task mới lộ ra. Hàm dưới đây sinh scheme theo
  conn_type để lỗi lộ ngay ở tầng cấu hình.
"""

from __future__ import annotations

from airflow.hooks.base import BaseHook

# Map conn_type → (scheme, default port, driver class).
# `driver` dùng để Spark chọn đúng JDBC driver trong reader (ingestion_jdbc.py
# cũng suy ra từ prefix URL — hai nơi phải khớp nhau).
SOURCE_SCHEMES = {
    "postgres": ("jdbc:postgresql", 5432, "org.postgresql.Driver"),
    "oracle": ("jdbc:oracle:thin", 1521, "oracle.jdbc.OracleDriver"),
}


def _build_jdbc_url(conn) -> str:
    """Xây JDBC URL từ Airflow connection object."""
    conn_type = (conn.conn_type or "").lower()

    # `jdbc` = host chứa URL đầy đủ (dùng khi URL có tham số đặc thù mà các
    # trường host/port/schema không biểu diễn được).
    if conn_type == "jdbc":
        return conn.host

    if conn_type not in SOURCE_SCHEMES:
        raise ValueError(
            f"conn_id '{conn.conn_id}' có conn_type='{conn.conn_type}' không được hỗ trợ. "
            f"Supported: ['jdbc', {', '.join(repr(k) for k in SOURCE_SCHEMES)}]"
        )

    scheme, default_port, _driver = SOURCE_SCHEMES[conn_type]
    port = conn.port or default_port

    if conn_type == "oracle":
        # Oracle thin URL dùng SERVICE_NAME (không phải SID):
        #   jdbc:oracle:thin:@//host:1521/SERVICE_NAME
        # Schema trong Airflow UI mang tên service; nếu trống thì bỏ dấu '/'.
        service = conn.schema or ""
        suffix = f"/{service}" if service else ""
        return f"{scheme}:@//{conn.host}:{port}{suffix}"

    return f"{scheme}://{conn.host}:{port}/{conn.schema}"


def jdbc_jinja_args(conn_id: str) -> dict[str, str]:
    """
    Jinja template cho URL / user / password của một connection JDBC.

    Không gọi DB lúc parse DAG — Airflow resolve template lúc task chạy, và giá trị
    lấy qua `conn` được secrets masker che trong log / Rendered Template.
    Password nên đi qua `env` của BashOperator (docker exec -e DB_PASSWORD), không
    qua argv: argv hiện ở `ps` và Spark UI.

    Scheme của URL sinh TẠI THỜI ĐIỂM RENDER, theo `conn.conn_type` của connection
    thật — nhờ vậy cùng một DAG template dùng được cho cả PostgreSQL và Oracle.
    """
    c = f"conn['{conn_id}']"
    conn_type = f"({c}.conn_type or '')|lower"

    # p = postgres, o = oracle, j = jdbc (host đã là URL đầy đủ)
    url = (
        "{#- scheme theo conn_type — Postgres mặc định, Oracle dùng thin + service name -#}"
        "{{ " + c + ".host if " + conn_type + " == 'jdbc'"
        " else ('jdbc:oracle:thin:@//' ~ " + c + ".host ~ ':' ~ (" + c + ".port or 1521)"
        " ~ ('/' ~ " + c + ".schema if " + c + ".schema else ''))"
        " if " + conn_type + " == 'oracle'"
        " else 'jdbc:postgresql://' ~ " + c + ".host ~ ':' ~ (" + c + ".port or 5432)"
        " ~ '/' ~ (" + c + ".schema or '') }}"
    )
    return {
        "jdbc_url": url,
        "db_user": f"{{{{ conn['{conn_id}'].login }}}}",
        "db_password": f"{{{{ conn['{conn_id}'].password }}}}",
    }


def resolve_jdbc_conn(conn_id: str) -> dict[str, str]:
    """
    Trả về JDBC connection params thực (gọi DB ngay lập tức).
    Chỉ dùng trong runtime context, KHÔNG gọi ở module level.
    """
    conn = BaseHook.get_connection(conn_id)
    _, _port, driver = SOURCE_SCHEMES.get(
        (conn.conn_type or "").lower(), ("", 0, "")
    )
    return {
        "jdbc_url": _build_jdbc_url(conn),
        "db_user": conn.login,
        "db_password": conn.password,
        "driver": driver,
    }
