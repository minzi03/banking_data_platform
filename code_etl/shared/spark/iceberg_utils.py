"""
Iceberg-specific utilities for data writes.
Enforces overwritePartitions convention for partition-safe writes.
"""

from pyspark.sql import DataFrame


def get_iceberg_table_name(catalog: str, schema: str, table: str) -> str:
    """Ghép tên bảng Iceberg đầy đủ: catalog.schema.table"""
    return f"{catalog}.{schema}.{table}"


def table_exists(spark, table_name: str) -> bool:
    """Check if an Iceberg table exists in the catalog."""
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception:
        return False


def create_iceberg_table_if_not_exists(df: DataFrame, table_name: str, logger) -> None:
    """
    Create Iceberg table if it doesn't exist.
    Uses the DataFrame schema to create the table with proper partitioning.
    """
    spark = df.sparkSession

    if table_exists(spark, table_name):
        logger.info(f"Table {table_name} already exists")
        return

    logger.info(f"Table {table_name} does not exist, creating...")

    # Build CREATE TABLE statement from DataFrame schema
    fields = []
    for field in df.schema.fields:
        spark_type = field.dataType.simpleString()
        fields.append(f"  {field.name} {spark_type}")

    # Partition by cob_dt only if it exists in the DataFrame
    partition_cols = []
    if "cob_dt" in [f.name for f in df.schema.fields]:
        partition_cols.append("cob_dt")

    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    create_sql += ",\n".join(fields)
    create_sql += "\n) USING iceberg"

    if partition_cols:
        create_sql += f"\nPARTITIONED BY ({', '.join(partition_cols)})"

    create_sql += "\nTBLPROPERTIES ('format-version' = '2')"

    logger.info(f"Creating table with SQL: {create_sql[:200]}...")
    spark.sql(create_sql)
    logger.info(f"Table {table_name} created successfully")


def write_to_iceberg(df: DataFrame, table_name: str, logger) -> None:
    """
    Write DataFrame to Iceberg table.

    Luôn dùng overwritePartitions — an toàn cho cả partitioned và unpartitioned:
    - Partitioned tables: ghi đè đúng partition của dữ liệu đầu vào
    - Unpartitioned tables: ghi đè toàn bộ dữ liệu

    Không gọi df.count() trước write — sẽ gây executor OOM.
    """
    spark = df.sparkSession

    # Create table if not exists
    create_iceberg_table_if_not_exists(df, table_name, logger)

    logger.info(f"Writing to {table_name} using overwritePartitions")
    df.writeTo(table_name).overwritePartitions()
