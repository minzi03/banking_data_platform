"""
PostgreSQL Batch Writer — Banking Data Platform
Writes generated data to PostgreSQL using COPY for fast bulk inserts.
"""

import io
import logging
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


class PostgresWriter:
    """Fast batch writer to PostgreSQL using COPY protocol."""

    def __init__(self, host: str, port: int, dbname: str, user: str, password: str):
        self.conn_params = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
        }
        self.conn = None

    def connect(self):
        """Establish connection to PostgreSQL."""
        self.conn = psycopg2.connect(**self.conn_params)
        self.conn.autocommit = False
        logger.info("Connected to PostgreSQL: %s/%s", self.conn_params["host"], self.conn_params["dbname"])

    def close(self):
        """Close connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("PostgreSQL connection closed")

    def write_rows(self, schema: str, table: str, columns: list[str], rows: list[tuple],
                   batch_size: int = 5000):
        """
        Write rows to a PostgreSQL table using COPY for performance.

        Args:
            schema: Target schema (e.g. 'core_banking')
            table: Target table name
            columns: List of column names
            rows: List of tuples, one per row
            batch_size: Rows per COPY batch
        """
        if not rows:
            logger.warning("No rows to write for %s.%s", schema, table)
            return

        table_name = f"{schema}.{table}"
        total = len(rows)
        written = 0

        try:
            cursor = self.conn.cursor()

            for i in range(0, total, batch_size):
                batch = rows[i:i + batch_size]

                # Build COPY command
                columns_str = ", ".join(columns)
                copy_sql = f"COPY {table_name} ({columns_str}) FROM STDIN WITH CSV"

                # Write batch to StringIO buffer
                buffer = io.StringIO()
                for row in batch:
                    line = self._row_to_csv(row)
                    buffer.write(line + "\n")
                buffer.seek(0)

                cursor.copy_expert(copy_sql, buffer)
                written += len(batch)

            self.conn.commit()
            logger.info("  ✓ %s.%s: %d/%d rows written", schema, table, written, total)

        except Exception as e:
            self.conn.rollback()
            logger.error("  ✗ %s.%s: FAILED — %s", schema, table, str(e))
            raise

    def write_rows_direct(self, schema: str, table: str, columns: list[str], rows: list[tuple]):
        """
        Fallback: write using INSERT statements (slower but more compatible).
        Used when COPY fails (e.g. special characters).
        """
        if not rows:
            return

        table_name = f"{schema}.{table}"
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        try:
            cursor = self.conn.cursor()
            psycopg2.extras.execute_batch(cursor, insert_sql, rows, page_size=1000)
            self.conn.commit()
            logger.info("  ✓ %s.%s: %d rows written (INSERT)", schema, table, len(rows))

        except Exception as e:
            self.conn.rollback()
            logger.error("  ✗ %s.%s: FAILED — %s", schema, table, str(e))
            raise

    def disable_triggers(self, schema: str):
        """Disable all triggers on a schema (for faster bulk load)."""
        cursor = self.conn.cursor()
        cursor.execute(f"SET session_replication_role = 'replica';")
        self.conn.commit()
        logger.info("  ⚡ Triggers disabled for bulk load")

    def enable_triggers(self):
        """Re-enable all triggers."""
        cursor = self.conn.cursor()
        cursor.execute(f"SET session_replication_role = 'origin';")
        self.conn.commit()
        logger.info("  ⚡ Triggers re-enabled")

    def truncate_all(self):
        """Truncate all source tables (in correct order for FK constraints)."""
        cursor = self.conn.cursor()
        # Truncate in reverse dependency order
        tables = [
            # digital_banking (no FK dependencies between these)
            ("digital_banking", "online_transaction"),
            ("digital_banking", "support_ticket"),
            ("digital_banking", "device"),
            ("digital_banking", "location"),
            ("digital_banking", "mcc_code"),
            # card_crm
            ("card_crm", "crm_interaction"),
            ("card_crm", "card_txn"),
            ("card_crm", "card"),
            # core_banking (reverse FK order)
            ("core_banking", "txn_account"),
            ("core_banking", "loan"),
            ("core_banking", "deposit"),
            ("core_banking", "account"),
            ("core_banking", "employee"),
            ("core_banking", "customer"),
            ("core_banking", "product"),
            ("core_banking", "branch"),
            # ops
            ("opslakehouse", "source_table_registry"),
        ]
        for schema, table in tables:
            try:
                cursor.execute(f"TRUNCATE TABLE {schema}.{table} CASCADE;")
                logger.info("  🗑  %s.%s truncated", schema, table)
            except Exception as e:
                logger.warning("  ⚠  %s.%s truncate failed: %s", schema, table, str(e))
                self.conn.rollback()
        self.conn.commit()
        logger.info("  All tables truncated")

    def get_row_count(self, schema: str, table: str) -> int:
        """Get row count for a table."""
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        return cursor.fetchone()[0]

    def table_exists(self, schema: str, table: str) -> bool:
        """Check if a table exists."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s)",
            (schema, table)
        )
        return cursor.fetchone()[0]

    @staticmethod
    def _row_to_csv(row: tuple) -> str:
        """Convert a tuple to CSV line, handling None and special characters."""
        import csv
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
        # Convert all values to strings, None stays as empty string for COPY
        str_row = ["" if v is None else str(v) for v in row]
        writer.writerow(str_row)
        return output.getvalue().strip()
