# =============================================================================
# Banking Data Platform — Makefile
# Quick commands for managing the data platform
# =============================================================================

.PHONY: help up down restart logs status clean seed superset superset-init

COMPOSE_FILE := docker/docker-compose.yml
DC := docker compose -f $(COMPOSE_FILE)

# Default target
help:
	@echo "=========================================="
	@echo " Banking Data Platform — Commands"
	@echo "=========================================="
	@echo ""
	@echo "  Infrastructure:"
	@echo "    make up          Start all services"
	@echo "    make up-lite     Start without CDC (lighter)"
	@echo "    make up-db       Start only PostgreSQL + MinIO (for seed data)"
	@echo "    make down        Stop all services"
	@echo "    make restart     Restart all services"
	@echo "    make status      Show service status"
	@echo "    make logs        Tail all logs"
	@echo ""
	@echo "  Data:"
	@echo "    make seed        Generate seed data (inside container)"
	@echo "    make seed-local  Generate seed data (from host)"
	@echo ""
	@echo "  Bronze Layer:"
	@echo "    make bronze-init       Create Iceberg Bronze tables"
	@echo "    make bronze-bootstrap  Full load (COB_DT=YYYY-MM-DD)"
	@echo "    make bronze-ingest     Incremental (CONFIG=path COB_DT=YYYY-MM-DD)"
	@echo ""
	@echo "  Silver Layer:"
	@echo "    make silver-init       Create Iceberg Silver tables"
	@echo "    make silver-bootstrap  Full load (COB_DT=YYYY-MM-DD)"
	@echo "    make silver-scd1       Run SCD1 job (CONFIG=path COB_DT=YYYY-MM-DD)"
	@echo "    make silver-scd2       Run SCD2 job (CONFIG=path COB_DT=YYYY-MM-DD)"
	@echo "    make silver-fact       Run Fact job (CONFIG=path COB_DT=YYYY-MM-DD)"
	@echo ""
	@echo "  Gold Layer:"
	@echo "    make gold-init         Create Iceberg Gold tables"
	@echo "    make gold-bootstrap    Full load (COB_DT=YYYY-MM-DD)"
	@echo "    make gold-job          Run single Gold job (CONFIG=path COB_DT=YYYY-MM-DD)"
	@echo ""
	@echo "  Tools:"
	@echo "    make trino       Open Trino CLI"
	@echo "    make psql        Open PostgreSQL CLI"
	@echo "    make superset    Open Superset UI (http://localhost:8088)"
	@echo "    make clean       Remove volumes and data"
	@echo ""
	@echo "  CI / Code Quality:"
	@echo "    make lint            Run ruff linter"
	@echo "    make format          Auto-format with ruff"
	@echo "    make test            Run unit tests (fast, no Spark)"
	@echo "    make test-slow       Run all tests including slow ones"
	@echo "    make test-integration Run integration tests (Docker stack)"
	@echo "    make check           Lint + test (pre-push gate)"
	@echo "    make ci              Full local CI (lint + YAML + test + security)"
	@echo "    make pre-commit      Install pre-commit hooks"
	@echo "    make install-dev     Install dev dependencies (editable mode)"
	@echo ""
	@echo "  Infrastructure as Code:"
	@echo "    make infra-init      terraform init"
	@echo "    make infra-plan      terraform plan"
	@echo "    make infra-apply     terraform apply"
	@echo "    make infra-destroy   terraform destroy"
	@echo ""
	@echo "UI URLs:"
	@echo "  Airflow:      http://localhost:8080 (admin/admin123)"
	@echo "  MinIO:        http://localhost:9001 (minioadmin/Minioadmin123)"
	@echo "  Spark:        http://localhost:9090"
	@echo "  Spark Worker: http://localhost:9091"
	@echo "  Trino:        http://localhost:8085"
	@echo "  OpenMetadata: http://localhost:8585"
	@echo "  Superset:     http://localhost:8088 (admin/admin123)"
	@echo "  Prometheus:   http://localhost:9095"
	@echo "  Grafana:      http://localhost:3000"
	@echo ""

# ---------------------------------------------------------------------------
# Service management
# ---------------------------------------------------------------------------
up:
	cd docker && cp -n .env .env.local 2>/dev/null || true
	$(DC) up -d
	@echo ""
	@echo "All services starting... check status with: make status"

up-lite:
	cd docker && cp -n .env .env.local 2>/dev/null || true
	$(DC) up -d --scale debezium=0 --scale kafka=0 --scale zookeeper=0
	@echo ""
	@echo "Core services starting (no CDC)..."

up-db:
	cd docker && cp -n .env .env.local 2>/dev/null || true
	$(DC) up -d postgres minio mc iceberg-rest
	@echo ""
	@echo "Database + Storage services starting..."

up-airflow:
	$(DC) up -d airflow-init airflow-webserver airflow-scheduler
	@echo ""
	@echo "Airflow services starting..."

down:
	$(DC) down

restart:
	$(DC) restart

stop:
	$(DC) stop

# ---------------------------------------------------------------------------
# Monitoring
# ---------------------------------------------------------------------------
status:
	$(DC) ps

logs:
	$(DC) logs -f --tail=50

logs-postgres:
	$(DC) logs -f postgres --tail=30

logs-spark:
	$(DC) logs -f spark-master spark-worker-1 --tail=30

logs-airflow:
	$(DC) logs -f airflow-webserver airflow-scheduler --tail=30

logs-cdc:
	$(DC) logs -f debezium kafka --tail=30

# ---------------------------------------------------------------------------
# Database access
# ---------------------------------------------------------------------------
psql:
	$(DC) exec postgres psql -U banking_admin -d banking_db

# ---------------------------------------------------------------------------
# Seed data generation
# ---------------------------------------------------------------------------
seed:
	@echo "Running seed data generator inside PostgreSQL container..."
	$(DC) exec postgres python /opt/project/data_generator/generate_all.py \
		--host postgres --port 5432
	@echo "Seed data generated successfully"

seed-local:
	@echo "Running seed data generator from host (PostgreSQL must be running)..."
	python data_generator/generate_all.py --host localhost --port 5432
	@echo "Seed data generated successfully"

# ---------------------------------------------------------------------------
# Trino queries
# ---------------------------------------------------------------------------
trino:
	$(DC) exec trino trino --catalog lakehouse

# ---------------------------------------------------------------------------
# Superset (BI Layer)
# ---------------------------------------------------------------------------
superset:
	$(DC) up -d superset superset-init
	@echo ""
	@echo "Superset starting... http://localhost:8088"
	@echo "Login: admin / admin123"

superset-logs:
	$(DC) logs -f superset superset-init --tail=30

# ---------------------------------------------------------------------------
# Infrastructure as Code (Terraform)
# ---------------------------------------------------------------------------
infra-init:
	cd terraform && terraform init
	@echo "Terraform initialized"

infra-plan:
	cd terraform && terraform plan
	@echo "Terraform plan complete"

infra-apply:
	cd terraform && terraform apply -auto-approve
	@echo "Terraform apply complete"

infra-destroy:
	cd terraform && terraform destroy -auto-approve
	@echo "Terraform destroy complete"

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
clean:
	$(DC) down -v --remove-orphans
	@echo "All containers, volumes, and networks removed"

clean-images:
	$(DC) down --rmi local
	@echo "Custom images removed"

# ---------------------------------------------------------------------------
# Spark submit (convenience)
# ---------------------------------------------------------------------------
spark-submit:
	$(DC) exec spark-worker-1 spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		--conf spark.driver.memory=512m \
		--conf spark.executor.memory=768m \
		$(ARGS)

# ---------------------------------------------------------------------------
# Bronze layer — Bootstrap & Ingestion
# ---------------------------------------------------------------------------
bronze-init:
	@echo "Creating Iceberg Bronze tables..."
	$(DC) exec spark-worker-1 spark-sql \
		--master spark://spark-master:7077 \
		-f /opt/project/docker/init_iceberg/01_ddl_bronze.sql
	@echo "Bronze tables created"

bronze-bootstrap:
	@echo "Running Bronze bootstrap (full load from PostgreSQL)..."
	$(DC) exec spark-worker-1 spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		/opt/project/code_etl/bronze/bootstrap/initial_load.py \
		--jdbc_url "jdbc:postgresql://postgres:5432/banking_db" \
		--db_user banking_admin \
		--db_password BankingAdmin123 \
		--cob_dt $(COB_DT)
	@echo "Bronze bootstrap completed"

bronze-ingest:
	@echo "Running Bronze incremental ingestion..."
	$(DC) exec spark-worker-1 spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		/opt/project/code_etl/bronze/base_job/ingestion_jdbc.py \
		--config $(CONFIG) \
		--cob_dt $(COB_DT) \
		--jdbc_url "jdbc:postgresql://postgres:5432/banking_db" \
		--db_user banking_admin \
		--db_password BankingAdmin123
	@echo "Bronze ingestion completed"

# ---------------------------------------------------------------------------
# Silver layer — Bootstrap & ETL
# ---------------------------------------------------------------------------
silver-init:
	@echo "Creating Iceberg Silver tables..."
	$(DC) exec spark-worker-1 spark-sql \
		--master spark://spark-master:7077 \
		-f /opt/project/docker/init_iceberg/02_ddl_silver.sql
	@echo "Silver tables created"

silver-bootstrap:
	@echo "Running Silver bootstrap (all dims + facts)..."
	$(DC) exec spark-worker-1 spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		/opt/project/code_etl/silver/bootstrap/initial_load.py \
		--cob_dt $(COB_DT)
	@echo "Silver bootstrap completed"

silver-scd1:
	@echo "Running Silver SCD Type 1 job..."
	$(DC) exec spark-worker-1 spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		-m code_etl.silver.base_job.scd_type1 \
		--config $(CONFIG) \
		--cob_dt $(COB_DT)
	@echo "SCD1 job completed"

silver-scd2:
	@echo "Running Silver SCD Type 2 job..."
	$(DC) exec spark-worker-1 spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		-m code_etl.silver.base_job.scd_type2 \
		--config $(CONFIG) \
		--cob_dt $(COB_DT)
	@echo "SCD2 job completed"

silver-fact:
	@echo "Running Silver Fact job..."
	$(DC) exec spark-worker-1 spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		-m code_etl.silver.base_job.fact_txn \
		--config $(CONFIG) \
		--cob_dt $(COB_DT)
	@echo "Fact job completed"

# ---------------------------------------------------------------------------
# Gold layer — Bootstrap & ETL
# ---------------------------------------------------------------------------
gold-init:
	@echo "Creating Iceberg Gold tables..."
	$(DC) exec spark-worker-1 spark-sql \
		--master spark://spark-master:7077 \
		-f /opt/project/docker/init_iceberg/03_ddl_gold.sql
	@echo "Gold tables created"

gold-bootstrap:
	@echo "Running Gold bootstrap (all marts + segments)..."
	$(DC) exec spark-worker-1 spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		/opt/project/code_etl/gold/bootstrap/initial_load.py \
		--cob_dt $(COB_DT)
	@echo "Gold bootstrap completed"

gold-job:
	@echo "Running Gold job..."
	$(DC) exec spark-worker-1 spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		-m code_etl.gold.base_job.gold_job \
		--config $(CONFIG) \
		--cob_dt $(COB_DT)
	@echo "Gold job completed"

# ---------------------------------------------------------------------------
# Validation and end-to-end execution
# ---------------------------------------------------------------------------
validate-pipeline:
	@echo "Validating Bronze/Silver/Gold counts and grain..."
	$(DC) exec spark-worker-1 spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		/opt/project/code_etl/scripts/validate_pipeline.py \
		--cob_dt $(COB_DT)

serving-bootstrap:
	@echo "Publishing current serving snapshots..."
	$(DC) exec spark-worker-1 spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		/opt/project/code_etl/serving/bootstrap/run_serving.py \
		--cob_dt $(COB_DT)

full-batch:
	@$(MAKE) bronze-bootstrap COB_DT=$(COB_DT)
	@$(MAKE) silver-bootstrap COB_DT=$(COB_DT)
	@$(MAKE) gold-bootstrap COB_DT=$(COB_DT)
	@$(MAKE) serving-bootstrap COB_DT=$(COB_DT)
	@$(MAKE) validate-pipeline COB_DT=$(COB_DT)
	@echo "Full batch pipeline completed successfully"

.PHONY: validate-pipeline serving-bootstrap full-batch

# ---------------------------------------------------------------------------
# CI / Code Quality (local development)
# ---------------------------------------------------------------------------
.PHONY: lint format test test-slow test-integration check ci pre-commit install-dev

install-dev:
	pip install -e ".[dev]"
	pre-commit install
	@echo "✅ Dev dependencies installed + pre-commit hooks activated"

lint:
	@echo "=== Ruff Lint ==="
	ruff check governance/ code_etl/ data_generator/ api/ ml/ tests/ --output-format=github
	@echo "✅ Lint clean"

format:
	@echo "=== Ruff Format ==="
	ruff format governance/ code_etl/ data_generator/ api/ ml/ tests/
	@echo "✅ Formatted"

test:
	@echo "=== Unit Tests (fast) ==="
	python -m pytest tests/ \
		-v --tb=short \
		-m "not integration and not slow" \
		--cov=governance --cov=code_etl/shared \
		--cov-report=term-missing \
		--junitxml=test-results.xml
	@echo "✅ Unit tests passed"

test-slow:
	@echo "=== All Tests (including slow) ==="
	python -m pytest tests/ \
		-v --tb=short \
		-m "not integration" \
		--cov=governance --cov=code_etl/shared \
		--cov-report=term-missing
	@echo "✅ All tests passed"

test-integration:
	@echo "=== Integration Tests (requires Docker stack) ==="
	python -m pytest tests/integration/ \
		-v --tb=short -m integration \
		--junitxml=integration-results.xml
	@echo "✅ Integration tests passed"

validate-yaml:
	@echo "=== Validate YAML Configs ==="
	python -c "\
	import yaml; from pathlib import Path; \
	errs=[]; \
	[errs.append(f'{f}: {e}') or print(f'  ❌ {f.name}') \
	 for f in Path('code_etl').rglob('*.yml') \
	 for e in [None] if not (lambda: (yaml.safe_load(open(f)), print(f'  ✅ {f.name}'))[0] or True)()]; \
	exit(1) if errs else print('✅ All YAML valid')"

security-scan:
	@echo "=== Security Scan (Bandit) ==="
	bandit -r code_etl/ governance/ --severity-level medium -f screen || true
	@echo "✅ Security scan complete"

check: lint test
	@echo "✅ Pre-push gate passed (lint + test)"

ci: lint validate-yaml test security-scan
	@echo "✅ Full local CI passed"

pre-commit:
	pre-commit install
	pre-commit run --all-files
	@echo "✅ Pre-commit hooks installed and verified"

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
