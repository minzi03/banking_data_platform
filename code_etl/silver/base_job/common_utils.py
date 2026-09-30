"""Các hàm dùng chung cho tất cả job tầng Silver."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared"))

from utils.sql_renderer import render_sql


def parse_arguments(description: str = "Silver Layer Job") -> argparse.Namespace:
    """
    Đọc tham số dòng lệnh chung cho tất cả job Silver.

    Tham số:
        --config : Đường dẫn file YAML cấu hình job
        --cob_dt : Ngày xử lý (YYYY-MM-DD). Bắt buộc với fact job, không bắt buộc với SCD.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--config", required=True, help="Đường dẫn đến file cấu hình YAML")
    parser.add_argument(
        "--cob_dt", required=False, default=None, help="Ngày xử lý dữ liệu (YYYY-MM-DD), bắt buộc cho fact jobs"
    )
    parser.add_argument(
        "--backfill_from",
        default=None,
        help="Chỉ fact có job.incremental: ghi mọi partition ngày nghiệp vụ từ ngày này tới cob_dt "
        "(lần nạp đầu, ADR-0018). Bỏ trống = chỉ cob_dt.",
    )
    return parser.parse_args()


def get_target_table(config: dict) -> str:
    """
    Ghép tên bảng Iceberg đầy đủ từ config theo định dạng: catalog.schema.table.

    Ví dụ: lakehouse.silver.dim_customer
    """
    t = config["target"]
    return f"{t['catalog']}.{t['schema']}.{t['table']}"


def load_source_df(spark, config: dict, cob_dt: str, backfill_from: str | None = None):
    """
    Render câu SQL từ YAML rồi chạy trên Spark.

    Biến: {{ cob_dt }} và {{ from_dt }} (đầu khoảng partition cho fact incremental,
    ADR-0018; = cob_dt trừ khi nạp lịch sử bằng backfill_from).
    """
    incremental = bool(config.get("job", {}).get("incremental"))
    if backfill_from and not incremental:
        raise ValueError(f"--backfill_from chỉ dùng cho fact có job.incremental, không cho {get_target_table(config)}")
    if backfill_from and backfill_from > cob_dt:
        raise ValueError(f"backfill_from={backfill_from} phải <= cob_dt={cob_dt}")
    sql = render_sql(config["sql"], {"cob_dt": cob_dt, "from_dt": backfill_from or cob_dt})
    return spark.sql(sql)
