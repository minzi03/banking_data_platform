# =============================================================================
# Banking Data Platform — Terraform Configuration
# Infrastructure as Code (IaC) for Docker-based deployment
# =============================================================================
#
# This configuration manages the Docker containers as Terraform resources.
# It provides a declarative way to deploy and manage the banking data platform.
#
# Usage:
#   terraform init
#   terraform plan
#   terraform apply
#   terraform destroy
# =============================================================================

terraform {
  required_version = ">= 1.0"
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {
  host = var.docker_host
}

# ── Variables ───────────────────────────────────────────────────────────────
variable "docker_host" {
  description = "Docker daemon socket"
  type        = string
  default     = "unix:///var/run/docker.sock"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name prefix for containers"
  type        = string
  default     = "banking"
}

# ── Network ─────────────────────────────────────────────────────────────────
resource "docker_network" "lakehouse_net" {
  name = "${var.project_name}_lakehouse_net"
  driver = "bridge"
}

# ── Volumes ─────────────────────────────────────────────────────────────────
resource "docker_volume" "postgres_data" {
  name = "${var.project_name}_postgres_data"
}

resource "docker_volume" "minio_data" {
  name = "${var.project_name}_minio_data"
}

resource "docker_volume" "spark_data" {
  name = "${var.project_name}_spark_data"
}

resource "docker_volume" "airflow_logs" {
  name = "${var.project_name}_airflow_logs"
}

resource "docker_volume" "om_mysql_data" {
  name = "${var.project_name}_om_mysql_data"
}

resource "docker_volume" "om_es_data" {
  name = "${var.project_name}_om_es_data"
}

resource "docker_volume" "prometheus_data" {
  name = "${var.project_name}_prometheus_data"
}

resource "docker_volume" "grafana_data" {
  name = "${var.project_name}_grafana_data"
}

resource "docker_volume" "superset_home" {
  name = "${var.project_name}_superset_home"
}

# =============================================================================
# OUTPUTS
# =============================================================================
output "network_name" {
  description = "Docker network name"
  value       = docker_network.lakehouse_net.name
}

output "postgres_port" {
  description = "PostgreSQL port"
  value       = "5432"
}

output "minio_console_port" {
  description = "MinIO console port"
  value       = "9001"
}

output "spark_ui_port" {
  description = "Spark Master UI port"
  value       = "9090"
}

output "airflow_port" {
  description = "Airflow webserver port"
  value       = "8080"
}

output "trino_port" {
  description = "Trino port"
  value       = "8085"
}

output "openmetadata_port" {
  description = "OpenMetadata port"
  value       = "8585"
}

output "superset_port" {
  description = "Superset port"
  value       = "8088"
}

output "streamlit_port" {
  description = "Streamlit port"
  value       = "8501"
}

output "grafana_port" {
  description = "Grafana port"
  value       = "3000"
}
