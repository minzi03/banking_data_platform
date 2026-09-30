"""
Iceberg-specific utilities for data writes.
Enforces overwritePartitions convention for partition-safe writes
(bảng snapshot phải partition theo cob_dt — xem assert_partitioned_by_cob_dt).
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


def partition_fields(spark, table_name: str) -> list[str]:
    """
    Tên các partition field của một bảng Iceberg, đọc từ metadata table `.partitions`.

    Bảng không partition thì metadata table này không có cột `partition` (tài liệu
    Iceberg), nên đây là cách phân biệt không phụ thuộc định dạng output của DESCRIBE.
    """
    schema = spark.table(f"{table_name}.partitions").schema
    if "partition" not in schema.fieldNames():
        return []
    return list(schema["partition"].dataType.fieldNames())


def assert_partitioned_by_cob_dt(df: DataFrame, table_name: str) -> None:
    """
    Chặn overwritePartitions() lên một bảng snapshot KHÔNG partition theo cob_dt.

    Trên bảng không partition, overwritePartitions() thay TOÀN BỘ bảng: mỗi lần nạp
    xoá mọi snapshot cũ, và chạy lại một ngày cũ ghi đè luôn ngày mới. Bronze từng
    như vậy với 18/22 bảng (DDL không có PARTITIONED BY) nên lịch sử product/branch
    mất hẳn. Bảng tạo từ DDL mới đã partition; bảng cũ phải migrate một lần.
    """
    if "cob_dt" not in df.columns:
        return
    fields = partition_fields(df.sparkSession, table_name)
    if "cob_dt" not in fields:
        raise RuntimeError(
            f"{table_name} không partition theo cob_dt (partition hiện tại: {fields or 'không có'}). "
            "overwritePartitions() sẽ ghi đè TOÀN BỘ bảng thay vì một snapshot. "
            "Chạy migration một lần: make bronze-partition-migrate "
            "(code_etl/bronze/bootstrap/partition_bronze_by_cob_dt.py)."
        )


def write_to_iceberg(df: DataFrame, table_name: str, logger) -> None:
    """
    Write DataFrame to Iceberg table bằng overwritePartitions (dynamic overwrite).

    Chỉ an toàn khi bảng partition theo cob_dt: ghi đè đúng snapshot cob_dt của dữ
    liệu đầu vào, các ngày khác giữ nguyên, chạy lại cùng ngày cho cùng kết quả.
    DataFrame có cột cob_dt mà bảng không partition theo nó → FAIL trước khi ghi
    (assert_partitioned_by_cob_dt), không âm thầm xoá lịch sử.

    Không gọi df.count() trước write — sẽ gây executor OOM.
    """
    # Create table if not exists (partition theo cob_dt nếu có cột này)
    create_iceberg_table_if_not_exists(df, table_name, logger)
    assert_partitioned_by_cob_dt(df, table_name)

    logger.info(f"Writing to {table_name} using overwritePartitions")
    df.writeTo(table_name).overwritePartitions()
