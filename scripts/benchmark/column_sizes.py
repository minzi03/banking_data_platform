"""Dung lượng từng cột (byte nén trên đĩa) của một bảng Iceberg, cho một cob_dt.

spark-submit scripts/benchmark/column_sizes.py lakehouse.silver.fact_txn_account 2026-09-29
Đọc `files.column_sizes` (map field_id → byte) rồi đổi field_id sang tên cột.
"""

import sys

from pyspark.sql import SparkSession

table, cob = sys.argv[1], sys.argv[2]
spark = SparkSession.builder.appName("column-sizes").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

jtable = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
names = {f.fieldId(): f.name() for f in jtable.schema().columns()}
files = spark.table(f"{table}.files")
if "partition" in files.columns and "cob_dt" in files.schema["partition"].dataType.fieldNames():
    files = files.filter(f"partition.cob_dt = DATE '{cob}'")
rows = files.selectExpr("sum(record_count) n").first().n
sizes = files.selectExpr("explode(column_sizes) AS (field_id, bytes)").groupBy("field_id").sum("bytes").collect()
total = sum(r[1] for r in sizes)
print(f"RESULT {table} cob_dt={cob} rows={rows:,} total={total / 1e6:.1f} MB ({total / rows:.1f} B/row)")
for r in sorted(sizes, key=lambda r: -r[1]):
    print(
        f"RESULT   {names.get(r[0], r[0]):22s} {r[1] / 1e6:8.1f} MB  {r[1] / rows:6.1f} B/row  {100 * r[1] / total:5.1f}%"
    )
