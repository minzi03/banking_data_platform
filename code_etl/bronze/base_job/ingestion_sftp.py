"""
Bronze Ingestion — SFTP file source (khoá new2: POS files / settlement files).

Nguồn file: quy trình đối soát cuối ngày thả file POS/settlement vào một SFTP
landing zone. Job này tải file qua paramiko về local rồi Spark đọc CSV/Parquet,
gắn cột cob_dt và ghi vào Bronze Iceberg — cùng đích với ingestion_jdbc.py.

Khác với JDBC (đọc trực tiếp database), ở đây phải:
  1. Kết nối SFTP, liệt kê file khớp pattern.
  2. Tải về thư mục tạm local (Spark đọc file local).
  3. Đọc theo schema/header khai trong config.
  4. Ghi Bronze (overwrite partition cob_dt — idempotent theo cùng cách mọi
     Bronze job khác).

Config mẫu: code_etl/bronze/file_sources/pos_settlement.yml

Usage:
    spark-submit --master spark://spark-master:7077 \
        code_etl/bronze/base_job/ingestion_sftp.py \
        --config code_etl/bronze/file_sources/pos_settlement.yml --cob_dt 2025-12-31
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "shared"))

from spark.iceberg_utils import get_iceberg_table_name, write_to_iceberg  # noqa: E402
from spark.spark_session import get_spark_session  # noqa: E402
from utils.logger import get_logger  # noqa: E402
from utils.yaml_loader import load_config  # noqa: E402


def _download_sftp_files(logger, sftp_cfg: dict) -> list[str]:
    """Tải các file khớp pattern từ SFTP về thư mục tạm. Trả về danh sách path local."""
    try:
        import paramiko
    except ImportError as exc:  # pragma: no cover - phụ thuộc môi trường
        raise ImportError("Cần paramiko để đọc SFTP. Thêm vào requirements-spark.txt.") from exc

    host = os.environ.get(sftp_cfg.get("host_env", "SFTP_HOST"), sftp_cfg.get("host", "sftp"))
    port = int(os.environ.get("SFTP_PORT", sftp_cfg.get("port", 22)))
    user = os.environ.get(sftp_cfg.get("user_env", "SFTP_USER"), sftp_cfg.get("user", "sftp_user"))
    password = os.environ.get(sftp_cfg.get("password_env", "SFTP_PASSWORD"), "")
    remote_dir = sftp_cfg["remote_dir"]
    pattern = sftp_cfg.get("file_pattern", "*.csv")

    logger.info(f"Kết nối SFTP {user}@{host}:{port}, dir={remote_dir}, pattern={pattern}")
    transport = paramiko.Transport((host, port))
    try:
        transport.connect(username=user, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            names = [n for n in sftp.listdir(remote_dir) if Path(n).match(pattern)]
            if not names:
                raise FileNotFoundError(f"Không có file khớp {pattern} trong {remote_dir}")
            local_dir = tempfile.mkdtemp(prefix="sftp_ingest_")
            paths = []
            for name in sorted(names):
                local = os.path.join(local_dir, name)
                sftp.get(f"{remote_dir}/{name}", local)
                logger.info(f"  tải {name} → {local}")
                paths.append(local)
            return paths
        finally:
            sftp.close()
    finally:
        transport.close()


def extract_from_sftp(spark, config: dict, cob_dt: str, logger) -> DataFrame:  # noqa: F821
    """Tải file từ SFTP và đọc thành DataFrame, gắn cột cob_dt."""
    local_paths = _download_sftp_files(logger, config["source"])
    reader = spark.read.option("header", str(config["source"].get("header", True)).lower())
    if config["source"].get("delimiter"):
        reader = reader.option("sep", config["source"]["delimiter"])
    if config["source"].get("infer_schema") is False and config["source"].get("schema"):
        reader = reader.schema(config["source"]["schema"])
    else:
        reader = reader.option("inferSchema", "true")

    df = reader.csv(local_paths)
    # cob_dt = ngày nghiệp vụ của file (settlement date), không phải now().
    col = config["load"].get("business_date_column")
    if col and col in df.columns:
        return df.withColumn("cob_dt", F.to_date(F.col(col)))
    return df.withColumn("cob_dt", F.lit(cob_dt).cast("date"))


def run_ingestion(spark, config: dict, cob_dt: str, logger) -> None:
    logger.info(f"Bronze SFTP ingestion → {config['target']}")
    df = extract_from_sftp(spark, config, cob_dt, logger)
    target = config["target"]
    table_name = get_iceberg_table_name(catalog=target["catalog"], schema=target["schema"], table=target["table"])
    logger.info(f"Ghi vào Iceberg: {table_name}")
    write_to_iceberg(df, table_name, logger)
    logger.info(f"Hoàn tất nạp file cho {table_name}")


def main():
    parser = argparse.ArgumentParser(description="Bronze SFTP file ingestion")
    parser.add_argument("--config", required=True)
    parser.add_argument("--cob_dt", required=True)
    args = parser.parse_args()

    logger = get_logger("ingestion_sftp")
    config = load_config(args.config)
    spark = get_spark_session("bronze-sftp-ingest")
    try:
        run_ingestion(spark, config, args.cob_dt, logger)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
