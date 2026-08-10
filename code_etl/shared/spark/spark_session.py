"""
SparkSession factory — Iceberg REST catalog + MinIO S3 backend.
Đọc cấu hình từ biến môi trường để tránh hardcode credential.

Khi chạy trên Spark cluster (spark-submit), spark-defaults.conf đã set sẵn
Iceberg config → SparkSession chỉ cần set appName, phần còn lại kế thừa từ config.
"""

import os

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "banking-lakehouse-job") -> SparkSession:
    """
    Tạo SparkSession với Iceberg REST catalog và MinIO S3A.

    Khi chạy trong Docker cluster:
      - spark-defaults.conf đã cấu hình Iceberg REST catalog + S3FileIO
      - spark-submit tự động load config từ spark-defaults.conf
      - Chỉ cần set appName, các config khác đã có sẵn

    Biến môi trường (dùng cho test local hoặc override):
      ICEBERG_CATALOG_URI  — REST catalog URI (default: http://iceberg-rest:8181)
      ICEBERG_WAREHOUSE    — s3a://lakehouse/lakehouse
      MINIO_ENDPOINT       — http://minio:9000
      MINIO_ACCESS_KEY     — minio access key
      MINIO_SECRET_KEY     — minio secret key
    """
    builder = SparkSession.builder.appName(app_name)

    # Chỉ override config khi có env var (cho phép chạy local test)
    catalog_uri = os.environ.get("ICEBERG_CATALOG_URI")
    if catalog_uri:
        warehouse = os.environ.get("ICEBERG_WAREHOUSE", "s3a://lakehouse/lakehouse")
        minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
        minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
        minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "Minioadmin123")

        builder = (
            builder
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lakehouse",
                    "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "rest")
            .config("spark.sql.catalog.lakehouse.uri", catalog_uri)
            .config("spark.sql.catalog.lakehouse.warehouse", warehouse)
            .config("spark.sql.defaultCatalog", "lakehouse")
            .config("spark.sql.catalog.lakehouse.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.lakehouse.s3.endpoint", minio_endpoint)
            .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
            .config("spark.sql.catalog.lakehouse.s3.access-key-id", minio_access_key)
            .config("spark.sql.catalog.lakehouse.s3.secret-access-key", minio_secret_key)
            .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
            .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
