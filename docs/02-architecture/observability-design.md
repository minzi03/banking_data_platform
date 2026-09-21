# P2 — Observability Design (Before Code)

## Goal

> Pipeline health + CDC data freshness visible in Grafana with real metrics.
> No ELK, no OpenTelemetry, no complex alerting.

---

## Metric Source Mapping

### What's Available Now

| Service | Endpoint | Format | Metrics Available |
|---------|----------|--------|-------------------|
| Spark Master | `http://spark-master:9090/json/` | JSON REST | Workers, cores, memory, active apps |
| Spark Worker | `http://spark-worker-1:9091/` | HTML | Executor status |
| Airflow | `http://airflow-webserver:8080/health` | JSON | Scheduler status, metadatabase status |
| Debezium | `http://debezium:8083/connectors/{name}/status` | JSON REST | Connector state, task state |
| Kafka | `localhost:9092` (CLI) | CLI | Offsets, consumer lag |
| PostgreSQL | `localhost:5432` | SQL | pg_stat_activity, custom freshness queries |
| Trino | `http://trino:8080/v1/info` | JSON | Server version, environment |

### What Needs an Exporter

| Service | Exporter | Why |
|---------|----------|-----|
| Kafka | `kafka_exporter` (danielqsj/kafka_exporter) | Expose consumer lag, broker metrics in Prometheus format |
| Spark | Enable `PrometheusServlet` in spark-defaults.conf | Expose JVM + Spark metrics at `/metrics/prometheus` |
| PostgreSQL | `postgres_exporter` (prometheus-community) | Expose pg_stat, custom freshness queries |
| Airflow | `statsd_exporter` + Airflow statsd config | Convert Airflow statsd metrics to Prometheus |
| CDC Freshness | `textfile collector` + shell script | Query Iceberg watermark, expose as Prometheus gauge |

### What Doesn't Need Changes

| Service | Reason |
|---------|--------|
| Debezium | REST API polled by custom script → textfile collector |
| MinIO | Health endpoint sufficient for "up" check |
| Iceberg REST | Health endpoint sufficient for "up" check |
| Trino | `/v1/info` polled by custom script → textfile collector |

---

## Architecture Design

```
┌─────────────────────────────────────────────────────────┐
│                    METRIC SOURCES                        │
│                                                          │
│  Kafka ──kafka_exporter──┐                              │
│  Spark ──PrometheusServlet┤                              │
│  PostgreSQL ──postgres_exporter┤                         │
│  Airflow ──statsd_exporter────┤                         │
│  Debezium ──textfile script───┤                         │
│  CDC Freshness ──textfile script┘                       │
│                                                          │
└──────────────────────────┬──────────────────────────────┘
                           │ scrape
                           ▼
                    ┌──────────────┐
                    │  Prometheus  │
                    │  (port 9095) │
                    └──────┬───────┘
                           │ query
                           ▼
                    ┌──────────────┐
                    │   Grafana    │
                    │  (port 3000) │
                    └──────────────┘
```

---

## New Containers (Minimal)

### 1. Prometheus
- Image: `prom/prometheus:latest`
- Port: 9095 (NOT 9090 — conflicts with Spark Master)
- Config: `docker/monitoring/prometheus.yml`
- Volume: `prometheus-data:/prometheus`

### 2. Grafana
- Image: `grafana/grafana:latest`
- Port: 3000
- Datasource: Prometheus (auto-provisioned)
- Dashboard: 1 banking data-platform dashboard (provisioned)

### 3. kafka_exporter
- Image: `danielqsj/kafka_exporter:latest`
- Port: 9308
- Args: `--kafka.server=kafka:9092`
- No persistent volume needed

### 4. postgres_exporter
- Image: `prometheuscommunity/postgres-exporter:latest`
- Port: 9187
- Env: `DATA_SOURCE_NAME=postgresql://banking_admin:BankingAdmin123@postgres:5432/banking_db?sslmode=disable`
- Custom metrics SQL for freshness

### 5. statsd_exporter
- Image: `prom/statsd-exporter:latest`
- Port: 9125 (statsd), 9102 (metrics)
- Converts Airflow statsd → Prometheus format

### Total resource addition: ~512MB RAM

---

## Prometheus Scrape Config

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  # --- Platform health ---
  - job_name: 'kafka-exporter'
    static_configs:
      - targets: ['kafka-exporter:9308']

  - job_name: 'spark-master'
    metrics_path: '/metrics/prometheus'
    static_configs:
      - targets: ['spark-master:9090']

  - job_name: 'postgres-exporter'
    static_configs:
      - targets: ['postgres-exporter:9187']

  - job_name: 'airflow-statsd'
    static_configs:
      - targets: ['statsd-exporter:9102']

  # --- CDC freshness (custom textfile) ---
  - job_name: 'cdc-freshness'
    static_configs:
      - targets: ['postgres-exporter:9187']
    # postgres_exporter will expose custom freshness metrics

  # --- Infrastructure up checks ---
  - job_name: 'blackbox-debezium'
    metrics_path: /probe
    params:
      module: [http_2xx]
    static_configs:
      - targets:
        - http://debezium:8083/connectors
        - http://trino:8080/v1/info
        - http://airflow-webserver:8080/health
```

---

## Dashboard Panels (6-8)

### Row 1: Platform Health
| Panel | Metric Source | Query |
|-------|--------------|-------|
| Service Up/Down | Prometheus blackbox / up metrics | `up{job=~"kafka-exporter\|spark-master\|postgres-exporter"}` |
| Kafka Broker Status | kafka_exporter | `kafka_brokers` |

### Row 2: CDC Pipeline
| Panel | Metric Source | Query |
|-------|--------------|-------|
| CDC Consumer Lag | kafka_exporter | `kafka_consumergroup_lag` for CDC topics |
| Debezium Connector Status | custom textfile/script | `debezium_connector_state` |

### Row 3: Spark
| Panel | Metric Source | Query |
|-------|--------------|-------|
| Running Applications | Spark PrometheusServlet | `spark_applications_active` |
| Executor Memory | Spark PrometheusServlet | `spark_executor_memoryUsed` |

### Row 4: Data Freshness (KEY PANEL)
| Panel | Metric Source | Query |
|-------|--------------|-------|
| Customer Current Freshness | postgres_exporter (custom SQL) | `now() - MAX(__consolidated_at)` |
| Account Current Freshness | postgres_exporter (custom SQL) | `now() - MAX(__consolidated_at)` |

---

## CDC Freshness Metric Design

The freshness metric is the most important for portfolio value.

### Approach: postgres_exporter with custom queries

postgres_exporter supports custom metrics via `PG_EXPORTER_EXTEND_QUERY_PATH`:

```yaml
# docker/monitoring/custom_queries.yml
pg_cdc_freshness:
  query: |
    SELECT
      table_name,
      EXTRACT(EPOCH FROM (NOW() - last_processed_at))::bigint AS freshness_seconds
    FROM lakehouse.meta.cdc_watermark
  metrics:
    - table_name:
        usage: "LABEL"
        description: "Table name"
    - freshness_seconds:
        usage: "GAUGE"
        description: "Seconds since last consolidation"
```

Wait — `lakehouse.meta.cdc_watermark` is in Iceberg, not PostgreSQL. postgres_exporter queries PostgreSQL.

**Correction**: Need to either:
1. Create a PostgreSQL view/function that reads from Iceberg via Trino JDBC → too complex
2. Use a separate textfile collector script that queries Trino and writes to a .prom file
3. Mirror the watermark to PostgreSQL via the consolidation job

**Best approach for lean P2**: Option 2 — textfile collector.

### Textfile Collector Approach

A shell script runs every 60 seconds (via cron or Airflow):
1. Queries Trino for `MAX(__consolidated_at)` from dim_customer_current and dim_account_current
2. Calculates freshness = NOW() - MAX(__consolidated_at)
3. Writes to `/tmp/cdc_freshness.prom`
4. Prometheus scrapes this file via `node_exporter --collector.textfile` or a simple HTTP server

**Simpler alternative**: Add the freshness query to postgres_exporter by creating a PostgreSQL view that the consolidation job updates:

```sql
-- In PostgreSQL, create a freshness tracking table
CREATE TABLE IF NOT EXISTS opslakehouse.cdc_freshness (
    table_name VARCHAR(100) PRIMARY KEY,
    last_consolidated_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW()
);
```

The consolidation job updates this table after each successful MERGE. postgres_exporter then exposes it.

**This is the cleanest approach** — no new containers for freshness, just a small table in PostgreSQL that the existing consolidation job writes to.

---

## Revised Container List

| Container | RAM | Purpose |
|-----------|-----|---------|
| prometheus | 256m | Metrics collection |
| grafana | 256m | Dashboard visualization |
| kafka-exporter | 128m | Kafka consumer lag metrics |
| postgres-exporter | 128m | PostgreSQL + CDC freshness metrics |
| **TOTAL** | **768m** | |

Removed: statsd_exporter (Airflow statsd not critical for lean P2).
Airflow health is checked via its `/health` endpoint directly.

---

## Pass Criteria

```
P2.1 Prometheus deployed + scraping       ⏳
P2.2 Grafana deployed + datasource        ⏳
P2.3 Real metrics collected               ⏳
P2.4 One useful dashboard (6-8 panels)    ⏳
P2.5 Failure/recovery verified            ⏳
P2.6 Evidence/docs                        ⏳
```

## Files to Create/Modify

```
NEW:  docker/docker-compose.monitoring.yml    (prometheus, grafana, exporters)
NEW:  docker/monitoring/prometheus.yml        (scrape config)
NEW:  docker/monitoring/grafana/provisioning/datasources/prometheus.yml
NEW:  docker/monitoring/grafana/provisioning/dashboards/dashboard.yml
NEW:  docker/monitoring/grafana/dashboards/banking-platform.json
MODIFY: code_etl/cdc/consolidation/cdc_consolidation.py  (write freshness to PostgreSQL)
NEW:  docker/init_postgres/05_monitoring.sql  (freshness tracking table)
```
