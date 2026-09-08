from __future__ import annotations
"""
CSV Export Writer — Banking Data Platform
Writes generated data to CSV files for non-PostgreSQL consumers.
"""

import csv
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class CsvWriter:
    """
    Export generated data to CSV files organized by schema/table.

    Directory structure:
        output_dir/
            core_banking/
                branch.csv
                customer.csv
                ...
            card_crm/
                card.csv
                ...
            digital_banking/
                device.csv
                ...
            opslakehouse/
                source_table_registry.csv
    """

    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("CSV output directory: %s", self.output_dir)

    def write_rows(self, schema: str, table: str, columns: list[str], rows: list[tuple]):
        """
        Write rows to CSV file.

        Args:
            schema: Target schema (e.g., 'core_banking')
            table: Target table name
            columns: List of column names
            rows: List of tuples, one per row
        """
        if not rows:
            logger.warning("No rows to write for %s.%s", schema, table)
            return

        # Create schema directory
        schema_dir = self.output_dir / schema
        schema_dir.mkdir(parents=True, exist_ok=True)

        csv_file = schema_dir / f"{table}.csv"
        total = len(rows)

        try:
            with open(csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)

                # Write header
                writer.writerow(columns)

                # Write data rows
                for row in rows:
                    # Convert tuples to lists, handle None values
                    csv_row = ["" if v is None else str(v) for v in row]
                    writer.writerow(csv_row)

            logger.info("  ✓ %s.%s: %d rows → %s", schema, table, total, csv_file)

        except Exception as e:
            logger.error("  ✗ %s.%s: CSV export FAILED — %s", schema, table, str(e))
            raise
