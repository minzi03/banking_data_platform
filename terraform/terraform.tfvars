# =============================================================================
# Banking Data Platform — Terraform Values
# =============================================================================

environment  = "dev"
project_name = "banking"

# ── Resource Limits ────────────────────────────────────────────────────────
postgres_memory      = "400m"
spark_worker_memory  = "8192m"
spark_worker_cores   = 8
trino_memory         = "1200m"
openmetadata_memory  = "1500m"
superset_memory      = "1024m"

# ── Database ───────────────────────────────────────────────────────────────
postgres_user     = "banking_admin"
postgres_password = "BankingAdmin123"
postgres_db       = "banking_db"

# ── Object Storage ─────────────────────────────────────────────────────────
minio_access_key = "minioadmin"
minio_secret_key = "Minioadmin123"
minio_bucket     = "lakehouse"

# ── Ports ──────────────────────────────────────────────────────────────────
ports = {
  postgres         = 5432
  minio_api        = 9000
  minio_console    = 9001
  spark_master_ui  = 9090
  spark_worker_ui  = 9091
  airflow          = 8080
  trino            = 8085
  openmetadata     = 8585
  superset         = 8088
  streamlit        = 8501
  prometheus       = 9095
  grafana          = 3000
}
