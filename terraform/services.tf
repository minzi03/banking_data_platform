# =============================================================================
# Banking Data Platform — Docker Services
# =============================================================================

# ── PostgreSQL ──────────────────────────────────────────────────────────────
resource "docker_container" "postgres" {
  name  = "${var.project_name}-postgres"
  image = "postgres:15-alpine"
  hostname = "postgres"

  env = [
    "POSTGRES_USER=${var.postgres_user}",
    "POSTGRES_PASSWORD=${var.postgres_password}",
    "POSTGRES_DB=${var.postgres_db}",
    "PGTZ=Asia/Ho_Chi_Minh",
  ]

  ports {
    internal = 5432
    external = var.ports.postgres
  }

  volumes {
    container_path = "/var/lib/postgresql/data"
    volume_name    = docker_volume.postgres_data.name
  }

  volumes {
    host_path      = abspath("${path.module}/../docker/init_postgres")
    container_path = "/docker-entrypoint-initdb.d"
  }

  memory = var.postgres_memory

  healthcheck {
    test     = ["CMD-SHELL", "pg_isready -U ${var.postgres_user} -d ${var.postgres_db}"]
    interval = "10s"
    timeout  = "5s"
    retries  = 5
  }

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── MinIO ───────────────────────────────────────────────────────────────────
resource "docker_container" "minio" {
  name  = "${var.project_name}-minio"
  image = "minio/minio:latest"
  hostname = "minio"

  env = [
    "MINIO_ROOT_USER=${var.minio_access_key}",
    "MINIO_ROOT_PASSWORD=${var.minio_secret_key}",
  ]

  ports {
    internal = 9000
    external = var.ports.minio_api
  }

  ports {
    internal = 9001
    external = var.ports.minio_console
  }

  volumes {
    container_path = "/data"
    volume_name    = docker_volume.minio_data.name
  }

  command = ["server", "/data", "--console-address", ":9001"]

  memory = 384

  healthcheck {
    test     = ["CMD-SHELL", "curl -sf http://localhost:9000/minio/health/live || exit 1"]
    interval = "10s"
    timeout  = "5s"
    retries  = 5
  }

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── MinIO Client (bucket creation) ─────────────────────────────────────────
resource "docker_container" "mc" {
  name  = "${var.project_name}-mc"
  image = "minio/mc:latest"

  depends_on = [docker_container.minio]

  entrypoint = ["/bin/sh", "-c", <<-EOT
    mc alias set local http://minio:9000 ${var.minio_access_key} ${var.minio_secret_key};
    mc mb --ignore-existing local/${var.minio_bucket};
    mc mb --ignore-existing local/${var.minio_bucket}/airflow-logs;
    mc mb --ignore-existing local/${var.minio_bucket}/spark-logs;
    echo 'MinIO buckets created successfully';
  EOT
  ]

  env = [
    "MINIO_ROOT_USER=${var.minio_access_key}",
    "MINIO_ROOT_PASSWORD=${var.minio_secret_key}",
  ]

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── Iceberg REST Catalog ───────────────────────────────────────────────────
resource "docker_container" "iceberg_rest" {
  name  = "${var.project_name}-iceberg-rest"
  image = "tabulario/iceberg-rest:1.6.0"
  hostname = "iceberg-rest"

  env = [
    "CATALOG_WAREHOUSE=s3a://${var.minio_bucket}/lakehouse",
    "CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO",
    "CATALOG_S3_ENDPOINT=http://minio:9000",
    "CATALOG_S3_PATH__STYLE__ACCESS=true",
    "AWS_ACCESS_KEY_ID=${var.minio_access_key}",
    "AWS_SECRET_ACCESS_KEY=${var.minio_secret_key}",
    "AWS_REGION=us-east-1",
    "CATALOG_CATALOG__IMPL=org.apache.iceberg.jdbc.JdbcCatalog",
    "CATALOG_URI=jdbc:postgresql://postgres:5432/iceberg_catalog",
    "CATALOG_JDBC_USER=${var.postgres_user}",
    "CATALOG_JDBC_PASSWORD=${var.postgres_password}",
    "CATALOG_JDBC_INITIALIZE__TABLES=true",
  ]

  ports {
    internal = 8181
    external = 8181
  }

  memory = 384

  healthcheck {
    test     = ["CMD-SHELL", "curl -sf http://localhost:8181/ || exit 1"]
    interval = "10s"
    timeout  = "5s"
    retries  = 5
  }

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── Spark Master ────────────────────────────────────────────────────────────
resource "docker_container" "spark_master" {
  name  = "${var.project_name}-spark-master"
  image = "${var.project_name}-spark:3.5.1"
  hostname = "spark-master"

  build {
    context    = abspath("${path.module}/../docker")
    dockerfile = "Dockerfile.spark"
  }

  env = [
    "SPARK_MASTER_HOST=spark-master",
    "SPARK_MASTER_PORT=7077",
    "SPARK_MASTER_WEBUI_PORT=9090",
  ]

  ports {
    internal = 7077
    external = 7077
  }

  ports {
    internal = 9090
    external = var.ports.spark_master_ui
  }

  command = [
    "/opt/spark/bin/spark-class",
    "org.apache.spark.deploy.master.Master",
    "--host", "spark-master",
    "--port", "7077",
    "--webui-port", "9090",
  ]

  memory = 400

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── Spark Worker ────────────────────────────────────────────────────────────
resource "docker_container" "spark_worker" {
  name  = "${var.project_name}-spark-worker-1"
  image = "${var.project_name}-spark:3.5.1"
  hostname = "spark-worker-1"

  depends_on = [docker_container.spark_master]

  env = [
    "SPARK_WORKER_MEMORY=${var.spark_worker_memory}",
    "SPARK_WORKER_CORES=${var.spark_worker_cores}",
    "SPARK_MASTER=spark://spark-master:7077",
    "SPARK_WORKER_WEBUI_PORT=9091",
  ]

  ports {
    internal = 9091
    external = var.ports.spark_worker_ui
  }

  command = [
    "/opt/spark/bin/spark-class",
    "org.apache.spark.deploy.worker.Worker",
    "--webui-port", "9091",
    "spark://spark-master:7077",
  ]

  memory = 10240

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── Zookeeper ───────────────────────────────────────────────────────────────
resource "docker_container" "zookeeper" {
  name  = "${var.project_name}-zookeeper"
  image = "confluentinc/cp-zookeeper:7.4.3"
  hostname = "zookeeper"

  env = [
    "ZOOKEEPER_CLIENT_PORT=2181",
    "ZOOKEEPER_TICK_TIME=2000",
  ]

  ports {
    internal = 2181
    external = 2181
  }

  memory = 256

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── Kafka ───────────────────────────────────────────────────────────────────
resource "docker_container" "kafka" {
  name  = "${var.project_name}-kafka"
  image = "confluentinc/cp-kafka:7.4.3"
  hostname = "kafka"

  depends_on = [docker_container.zookeeper]

  env = [
    "KAFKA_BROKER_ID=1",
    "KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
    "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092",
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
  ]

  ports {
    internal = 9092
    external = 9092
  }

  memory = 1024

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── Debezium ────────────────────────────────────────────────────────────────
resource "docker_container" "debezium" {
  name  = "${var.project_name}-debezium"
  image = "debezium/connect:2.5"
  hostname = "debezium"

  depends_on = [
    docker_container.kafka,
    docker_container.postgres,
  ]

  env = [
    "BOOTSTRAP_SERVERS=kafka:9092",
    "GROUP_ID=1",
    "CONFIG_STORAGE_TOPIC=my_connect_configs",
    "OFFSET_STORAGE_TOPIC=my_connect_offsets",
    "STATUS_STORAGE_TOPIC=my_connect_statuses",
    "CONFIG_STORAGE_REPLICATION_FACTOR=1",
    "OFFSET_STORAGE_REPLICATION_FACTOR=1",
    "STATUS_STORAGE_REPLICATION_FACTOR=1",
  ]

  ports {
    internal = 8083
    external = 8083
  }

  memory = 2048

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── Trino ───────────────────────────────────────────────────────────────────
resource "docker_container" "trino" {
  name  = "${var.project_name}-trino"
  image = "trinodb/trino:435"
  hostname = "trino"

  depends_on = [
    docker_container.iceberg_rest,
    docker_container.postgres,
  ]

  ports {
    internal = 8080
    external = var.ports.trino
  }

  volumes {
    host_path      = abspath("${path.module}/../docker/init_trino")
    container_path = "/etc/trino/catalog"
  }

  memory = var.trino_memory

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── Airflow Webserver ───────────────────────────────────────────────────────
resource "docker_container" "airflow_webserver" {
  name  = "${var.project_name}-airflow-webserver"
  image = "${var.project_name}-airflow:2.10.0"
  hostname = "airflow-webserver"

  depends_on = [
    docker_container.postgres,
    docker_container.spark_master,
  ]

  env = [
    "AIRFLOW__CORE__EXECUTOR=LocalExecutor",
    "AIRFLOW__CORE__LOAD_EXAMPLES=false",
    "AIRFLOW__WEBSERVER__SECRET_KEY=8f7c9ab3f2f9f2d1fbe6f8f3b4e7d8c1a9e7c2d4b5f6a1e3",
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${var.postgres_user}:${var.postgres_password}@postgres:5432/airflow",
    "AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Ho_Chi_Minh",
  ]

  ports {
    internal = 8080
    external = var.ports.airflow
  }

  command = ["airflow", "webserver"]

  memory = 800

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── Airflow Scheduler ──────────────────────────────────────────────────────
resource "docker_container" "airflow_scheduler" {
  name  = "${var.project_name}-airflow-scheduler"
  image = "${var.project_name}-airflow:2.10.0"
  hostname = "airflow-scheduler"

  depends_on = [
    docker_container.postgres,
    docker_container.spark_master,
  ]

  env = [
    "AIRFLOW__CORE__EXECUTOR=LocalExecutor",
    "AIRFLOW__CORE__LOAD_EXAMPLES=false",
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${var.postgres_user}:${var.postgres_password}@postgres:5432/airflow",
    "AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Ho_Chi_Minh",
  ]

  command = ["airflow", "scheduler"]

  memory = 1300

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── OpenMetadata MySQL ─────────────────────────────────────────────────────
resource "docker_container" "om_mysql" {
  name  = "${var.project_name}-om-mysql"
  image = "mysql:8.0"
  hostname = "om-mysql"

  env = [
    "MYSQL_ROOT_PASSWORD=RootPassword123",
    "MYSQL_USER=openmetadata",
    "MYSQL_PASSWORD=OpenMetadata123",
    "MYSQL_DATABASE=openmetadata_db",
  ]

  ports {
    internal = 3306
    external = 3306
  }

  memory = 500

  healthcheck {
    test     = ["CMD-SHELL", "mysqladmin ping -h localhost -u root -pRootPassword123"]
    interval = "10s"
    timeout  = "5s"
    retries  = 5
  }

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── OpenMetadata Elasticsearch ──────────────────────────────────────────────
resource "docker_container" "om_elasticsearch" {
  name  = "${var.project_name}-om-elasticsearch"
  image = "docker.elastic.co/elasticsearch/elasticsearch:8.12.2"
  hostname = "om-elasticsearch"

  env = [
    "discovery.type=single-node",
    "ES_JAVA_OPTS=-Xms512m -Xmx512m",
    "xpack.security.enabled=false",
  ]

  ports {
    internal = 9200
    external = 9200
  }

  memory = 1024

  healthcheck {
    test     = ["CMD-SHELL", "curl -sf http://localhost:9200/_cluster/health || exit 1"]
    interval = "30s"
    timeout  = "10s"
    retries  = 10
  }

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── OpenMetadata ────────────────────────────────────────────────────────────
resource "docker_container" "openmetadata" {
  name  = "${var.project_name}-openmetadata"
  image = "openmetadata/server:1.5.6"
  hostname = "openmetadata"

  depends_on = [
    docker_container.om_mysql,
    docker_container.om_elasticsearch,
  ]

  env = [
    "SERVER_PORT=8585",
    "DB_HOST=om-mysql",
    "DB_PORT=3306",
    "DB_USER=openmetadata",
    "DB_USER_PASSWORD=OpenMetadata123",
    "OM_DATABASE=openmetadata_db",
    "ELASTICSEARCH_HOST=om-elasticsearch",
    "ELASTICSEARCH_PORT=9200",
  ]

  ports {
    internal = 8585
    external = var.ports.openmetadata
  }

  memory = var.openmetadata_memory

  healthcheck {
    test     = ["CMD-SHELL", "curl -sf http://localhost:8585/ || exit 1"]
    interval = "30s"
    timeout  = "10s"
    retries  = 10
  }

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── Superset ────────────────────────────────────────────────────────────────
resource "docker_container" "superset" {
  name  = "${var.project_name}-superset"
  image = "${var.project_name}-superset:3.1.3"
  hostname = "superset"

  depends_on = [
    docker_container.postgres,
  ]

  env = [
    "SUPERSET_SECRET_KEY=banking_platform_secret_key_2025",
    "DATABASE_USER=${var.postgres_user}",
    "DATABASE_PASSWORD=${var.postgres_password}",
    "DATABASE_HOST=postgres",
    "DATABASE_PORT=5432",
    "DATABASE_DB=superset",
    "FLASK_APP=superset",
    "SUPERSET_ENV=production",
    "TZ=Asia/Ho_Chi_Minh",
  ]

  ports {
    internal = 8088
    external = var.ports.superset
  }

  volumes {
    container_path = "/app/superset_home"
    volume_name    = docker_volume.superset_home.name
  }

  memory = var.superset_memory

  healthcheck {
    test     = ["CMD-SHELL", "curl -sf http://localhost:8088/health || exit 1"]
    interval = "30s"
    timeout  = "10s"
    retries  = 5
  }

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── Streamlit ───────────────────────────────────────────────────────────────
resource "docker_container" "streamlit" {
  name  = "${var.project_name}-streamlit"
  image = "${var.project_name}-streamlit:latest"
  hostname = "streamlit"

  ports {
    internal = 8501
    external = var.ports.streamlit
  }

  memory = 512

  healthcheck {
    test     = ["CMD-SHELL", "curl -sf http://localhost:8501/_stcore/health || exit 1"]
    interval = "30s"
    timeout  = "10s"
    retries  = 5
  }

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── Prometheus ──────────────────────────────────────────────────────────────
resource "docker_container" "prometheus" {
  name  = "${var.project_name}-prometheus"
  image = "prom/prometheus:v2.51.0"
  hostname = "prometheus"

  ports {
    internal = 9090
    external = var.ports.prometheus
  }

  command = [
    "--config.file=/etc/prometheus/prometheus.yml",
    "--storage.tsdb.retention.time=7d",
    "--web.enable-lifecycle",
  ]

  volumes {
    host_path      = abspath("${path.module}/../docker/monitoring/prometheus.yml")
    container_path = "/etc/prometheus/prometheus.yml"
    read_only      = true
  }

  volumes {
    container_path = "/prometheus"
    volume_name    = docker_volume.prometheus_data.name
  }

  memory = 256

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}

# ── Grafana ─────────────────────────────────────────────────────────────────
resource "docker_container" "grafana" {
  name  = "${var.project_name}-grafana"
  image = "grafana/grafana:10.4.0"
  hostname = "grafana"

  depends_on = [
    docker_container.prometheus,
  ]

  env = [
    "GF_SECURITY_ADMIN_USER=admin",
    "GF_SECURITY_ADMIN_PASSWORD=admin",
  ]

  ports {
    internal = 3000
    external = var.ports.grafana
  }

  volumes {
    host_path      = abspath("${path.module}/../docker/monitoring/grafana/provisioning")
    container_path = "/etc/grafana/provisioning"
    read_only      = true
  }

  volumes {
    host_path      = abspath("${path.module}/../docker/monitoring/grafana/dashboards")
    container_path = "/var/lib/grafana/dashboards"
    read_only      = true
  }

  volumes {
    container_path = "/var/lib/grafana"
    volume_name    = docker_volume.grafana_data.name
  }

  memory = 256

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }
}
