"""
Shared JDBC connection utilities cho Bronze/Silver/Gold DAGs.

Quy ước JDBC connection trong Airflow UI:
  - conn_type : jdbc hoặc postgres
  - Host      : host hoặc full JDBC URL
  - Port      : 5432
  - Schema    : banking_db
  - Login     : banking_admin
  - Password  : BankingAdmin123
"""

from __future__ import annotations

from airflow.hooks.base import BaseHook


def _build_jdbc_url(conn) -> str:
    """Xây JDBC URL từ Airflow connection object."""
    conn_type = (conn.conn_type or "").lower()

    builders = {
        "jdbc":     lambda c: c.host,
        "postgres": lambda c: f"jdbc:postgresql://{c.host}:{c.port or 5432}/{c.schema}",
    }

    if conn_type not in builders:
        raise ValueError(
            f"conn_id '{conn.conn_id}' có conn_type='{conn.conn_type}' không được hỗ trợ. "
            f"Supported: {list(builders)}"
        )

    return builders[conn_type](conn)


def jdbc_jinja_args(conn_id: str) -> dict[str, str]:
    """
    Trả về dict chứa Jinja template strings cho SparkSubmitOperator.application_args.
    Không gọi DB, Airflow resolve template lúc task execute.
    """
    return {
        "jdbc_url":    f"{{{{ conn['{conn_id}'].host }}}}",
        "db_user":     f"{{{{ conn['{conn_id}'].login }}}}",
        "db_password": f"{{{{ conn['{conn_id}'].password }}}}",
    }


def resolve_jdbc_conn(conn_id: str) -> dict[str, str]:
    """
    Trả về JDBC connection params thực (gọi DB ngay lập tức).
    Chỉ dùng trong runtime context, KHÔNG gọi ở module level.
    """
    conn = BaseHook.get_connection(conn_id)
    return {
        "jdbc_url":    _build_jdbc_url(conn),
        "db_user":     conn.login,
        "db_password": conn.password,
    }
