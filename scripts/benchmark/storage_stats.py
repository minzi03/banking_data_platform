"""Số dòng / số file / dung lượng dữ liệu của mỗi bảng lakehouse cho một cob_dt.

spark-submit bench_storage.py 2026-09-29 → JSON {schema.table: {rows, files, bytes}}
Bảng partition theo cob_dt: chỉ partition đó. Bảng không có cob_dt (dim SCD2, current): cả bảng.
"""

import json
import sys

from pyspark.sql import SparkSession

cob = sys.argv[1]
spark = SparkSession.builder.appName("bench-storage").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
out = {}
for schema in ("bronze", "silver", "gold"):
    for row in spark.sql(f"SHOW TABLES IN lakehouse.{schema}").collect():
        t = f"lakehouse.{schema}.{row.tableName}"
        if row.tableName.endswith("_cdc") or row.tableName == "cdc_dead_letter":
            continue
        cols = spark.table(t).columns
        files = spark.table(f"{t}.files")
        if (
            "cob_dt" in cols
            and "partition" in files.columns
            and "cob_dt" in files.schema["partition"].dataType.fieldNames()
        ):
            f = files.filter(f"partition.cob_dt = DATE '{cob}'")
            rows = spark.sql(f"SELECT count(*) c FROM {t} WHERE cob_dt = DATE '{cob}'").first().c
        else:
            f = files
            rows = spark.table(t).count()
        agg = f.selectExpr("count(*) n", "coalesce(sum(file_size_in_bytes), 0) b").first()
        out[f"{schema}.{row.tableName}"] = {"rows": rows, "files": agg.n, "bytes": agg.b}
print("JSON_BEGIN")
print(json.dumps(out, indent=1, sort_keys=True))
print("JSON_END")
