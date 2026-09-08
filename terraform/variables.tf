# =============================================================================
# Banking Data Platform — Variables
# =============================================================================

# ── Environment ─────────────────────────────────────────────────────────────
variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "project_name" {
  description = "Project name prefix"
  type        = string
  default     = "banking"
}

# ── Resource Limits ────────────────────────────────────────────────────────
variable "postgres_memory" {
  description = "PostgreSQL memory limit"
  type        = string
  default     = "400m"
}

variable "spark_worker_memory" {
  description = "Spark worker memory"
  type        = string
  default     = "8192m"
}

variable "spark_worker_cores" {
  description = "Spark worker CPU cores"
  type        = number
  default     = 8
}

variable "trino_memory" {
  description = "Trino memory limit"
  type        = string
  default     = "1200m"
}

variable "openmetadata_memory" {
  description = "OpenMetadata memory limit"
  type        = string
  default     = "1500m"
}

variable "superset_memory" {
  description = "Superset memory limit"
  type        = string
  default     = "1024m"
}

# ── Database ───────────────────────────────────────────────────────────────
variable "postgres_user" {
  description = "PostgreSQL username"
  type        = string
  default     = "banking_admin"
}

variable "postgres_password" {
  description = "PostgreSQL password"
  type        = string
  default     = "BankingAdmin123"
  sensitive   = true
}

variable "postgres_db" {
  description = "PostgreSQL database name"
  type        = string
  default     = "banking_db"
}

# ── Object Storage ─────────────────────────────────────────────────────────
variable "minio_access_key" {
  description = "MinIO access key"
  type        = string
  default     = "minioadmin"
}

variable "minio_secret_key" {
  description = "MinIO secret key"
  type        = string
  default     = "Minioadmin123"
  sensitive   = true
}

variable "minio_bucket" {
  description = "MinIO bucket name"
  type        = string
  default     = "lakehouse"
}

# ── Ports ──────────────────────────────────────────────────────────────────
variable "ports" {
  description = "Service ports"
  type = object({
    postgres         = number
    minio_api        = number
    minio_console    = number
    spark_master_ui  = number
    spark_worker_ui  = number
    airflow          = number
    trino            = number
    openmetadata     = number
    superset         = number
    streamlit        = number
    prometheus       = number
    grafana          = number
  })
  default = {
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
}
