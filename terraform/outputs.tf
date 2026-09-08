# =============================================================================
# Banking Data Platform — Outputs
# =============================================================================

output "postgres_host" {
  description = "PostgreSQL connection host"
  value       = "localhost"
}

output "postgres_port" {
  description = "PostgreSQL connection port"
  value       = var.ports.postgres
}

output "minio_endpoint" {
  description = "MinIO S3 endpoint"
  value       = "http://localhost:${var.ports.minio_api}"
}

output "minio_console" {
  description = "MinIO console URL"
  value       = "http://localhost:${var.ports.minio_console}"
}

output "spark_master_ui" {
  description = "Spark Master UI URL"
  value       = "http://localhost:${var.ports.spark_master_ui}"
}

output "airflow_url" {
  description = "Airflow webserver URL"
  value       = "http://localhost:${var.ports.airflow}"
}

output "trino_url" {
  description = "Trino query URL"
  value       = "http://localhost:${var.ports.trino}"
}

output "openmetadata_url" {
  description = "OpenMetadata URL"
  value       = "http://localhost:${var.ports.openmetadata}"
}

output "superset_url" {
  description = "Superset BI URL"
  value       = "http://localhost:${var.ports.superset}"
}

output "streamlit_url" {
  description = "Streamlit app URL"
  value       = "http://localhost:${var.ports.streamlit}"
}

output "grafana_url" {
  description = "Grafana monitoring URL"
  value       = "http://localhost:${var.ports.grafana}"
}

output "prometheus_url" {
  description = "Prometheus metrics URL"
  value       = "http://localhost:${var.ports.prometheus}"
}

output "service_summary" {
  description = "Summary of all service endpoints"
  value = <<-EOT
    ╔══════════════════════════════════════════════════════════╗
    ║     Banking Data Platform — Service Endpoints           ║
    ╠══════════════════════════════════════════════════════════╣
    ║ PostgreSQL:    localhost:${format("%-5d", var.ports.postgres)}  │ banki  ║
    ║ MinIO:         localhost:${format("%-5d", var.ports.minio_console)}  │ Mini  ║
    ║ Spark UI:      localhost:${format("%-5d", var.ports.spark_master_ui)}       ║
    ║ Airflow:       localhost:${format("%-5d", var.ports.airflow)}       ║
    ║ Trino:         localhost:${format("%-5d", var.ports.trino)}         ║
    ║ OpenMetadata:  localhost:${format("%-5d", var.ports.openmetadata)}  ║
    ║ Superset:      localhost:${format("%-5d", var.ports.superset)}      ║
    ║ Streamlit:     localhost:${format("%-5d", var.ports.streamlit)}     ║
    ║ Prometheus:    localhost:${format("%-5d", var.ports.prometheus)}     ║
    ║ Grafana:       localhost:${format("%-5d", var.ports.grafana)}       ║
    ╚══════════════════════════════════════════════════════════╝
  EOT
}
