# ─────────────────────────────────────────
# AML Pipeline — Makefile
# IBM Consulting Portfolio
# ─────────────────────────────────────────

.PHONY: help up down build restart logs test lint format security clean dag-run status

# Default
help:
	@echo ""
	@echo "AML Pipeline — Available Commands"
	@echo "────────────────────────────────────────"
	@echo "  make up          Start all services"
	@echo "  make down        Stop all services"
	@echo "  make build       Build Docker images"
	@echo "  make restart     Restart all services"
	@echo "  make status      Show service status + RAM"
	@echo "  make logs        Tail all logs"
	@echo ""
	@echo "  make test        Run all tests"
	@echo "  make test-unit   Run unit tests only"
	@echo "  make test-cov    Run tests with coverage"
	@echo "  make lint        Run linters"
	@echo "  make format      Auto-format code"
	@echo "  make security    Run security scan"
	@echo ""
	@echo "  make dag-run     Trigger all DAGs in order"
	@echo "  make dag-status  Show DAG run status"
	@echo "  make health      Check all service health"
	@echo ""
	@echo "  make clean       Remove containers + volumes"
	@echo "  make clean-logs  Remove Airflow logs"
	@echo ""

# ─────────────────────────────────────────
# Docker Commands
# ─────────────────────────────────────────
CORE_SERVICES = postgres minio airflow-webserver airflow-scheduler mlflow fastapi nginx streamlit

up:
	docker-compose up -d $(CORE_SERVICES)
	@echo "✅ Core services started"
	@echo "   Airflow:   http://localhost:8080"
	@echo "   FastAPI:   http://localhost:8010"
	@echo "   Streamlit: http://localhost:8501"
	@echo "   MLflow:    http://localhost:5010"
	@echo "   MinIO:     http://localhost:9001"

up-all:
	docker-compose up -d
	@echo "✅ All services started (including monitoring)"

down:
	docker-compose down
	@echo "✅ All services stopped"

build:
	docker-compose build --no-cache
	@echo "✅ Images built"

rebuild-api:
	docker-compose up -d --build fastapi
	@echo "✅ FastAPI rebuilt"

rebuild-streamlit:
	docker-compose up -d --build streamlit
	@echo "✅ Streamlit rebuilt"

restart:
	docker-compose restart $(CORE_SERVICES)
	@echo "✅ Services restarted"

logs:
	docker-compose logs -f --tail=100 $(CORE_SERVICES)

status:
	@echo "\n📊 Service Status"
	@echo "──────────────────────────────────────────────────"
	@docker-compose ps
	@echo "\n💾 RAM Usage"
	@echo "──────────────────────────────────────────────────"
	@docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}"

clean:
	docker-compose down -v
	docker system prune -f
	@echo "✅ Cleaned containers and volumes"

clean-logs:
	docker exec aml-pipeline-airflow-scheduler-1 \
		find /opt/airflow/logs -name "*.log" -mtime +7 -delete 2>/dev/null || true
	@echo "✅ Logs older than 7 days removed"

# ─────────────────────────────────────────
# Testing Commands
# ─────────────────────────────────────────
PYTEST_OPTS = -v --tb=short

test:
	pytest tests/ $(PYTEST_OPTS)

test-unit:
	pytest tests/ $(PYTEST_OPTS) -m "not integration"

test-api:
	pytest tests/test_api.py $(PYTEST_OPTS)

test-data:
	pytest tests/test_data_quality.py $(PYTEST_OPTS)

test-dags:
	pytest tests/test_dags.py $(PYTEST_OPTS)

test-ml:
	pytest tests/test_ml.py $(PYTEST_OPTS)

test-cov:
	pytest tests/ \
		--cov=src \
		--cov=dags \
		--cov-report=html:coverage_html \
		--cov-report=term-missing \
		$(PYTEST_OPTS)
	@echo "✅ Coverage report: coverage_html/index.html"

# ─────────────────────────────────────────
# Code Quality Commands
# ─────────────────────────────────────────
lint:
	@echo "🔍 Running flake8..."
	flake8 dags/ src/ tests/ --max-line-length=120 --extend-ignore=E203,W503
	@echo "✅ Lint passed"

format:
	@echo "✏️  Formatting with black..."
	black dags/ src/ tests/
	@echo "✏️  Sorting imports with isort..."
	isort dags/ src/ tests/
	@echo "✅ Code formatted"

format-check:
	black --check dags/ src/ tests/
	isort --check-only dags/ src/ tests/

security:
	@echo "🔒 Running bandit..."
	bandit -r dags/ src/ -ll --exclude src/dashboard
	@echo "🔒 Checking dependencies..."
	safety check -r requirements.txt
	@echo "✅ Security scan passed"

# ─────────────────────────────────────────
# Airflow / DAG Commands
# ─────────────────────────────────────────
SCHEDULER = aml-pipeline-airflow-scheduler-1

dag-run:
	@echo "🚀 Triggering Bronze DAG..."
	docker exec $(SCHEDULER) airflow dags trigger aml_bronze_pipeline
	@echo "⏳ Waiting for Bronze..."
	@sleep 60
	@echo "🚀 Triggering Silver DAG..."
	docker exec $(SCHEDULER) airflow dags trigger aml_silver_pipeline
	@echo "⏳ Waiting for Silver..."
	@sleep 120
	@echo "🚀 Triggering Gold DAG..."
	docker exec $(SCHEDULER) airflow dags trigger aml_gold_pipeline
	@echo "⏳ Waiting for Gold..."
	@sleep 120
	@echo "🚀 Triggering ML DAG..."
	docker exec $(SCHEDULER) airflow dags trigger aml_ml_pipeline
	@echo "✅ All DAGs triggered"

dag-status:
	@echo "\n📋 DAG Run Status"
	@docker exec $(SCHEDULER) airflow dags list-runs aml_bronze_pipeline --limit 1
	@docker exec $(SCHEDULER) airflow dags list-runs aml_silver_pipeline --limit 1
	@docker exec $(SCHEDULER) airflow dags list-runs aml_gold_pipeline --limit 1
	@docker exec $(SCHEDULER) airflow dags list-runs aml_ml_pipeline --limit 1

dag-validate:
	@echo "🔍 Validating DAG imports..."
	docker exec $(SCHEDULER) python3 -c "\
		import importlib, os, sys; \
		sys.path.insert(0, '/opt/airflow/dags'); \
		dags = ['aml_bronze_dag', 'aml_silver_dag', 'aml_gold_dag', 'aml_ml_dag']; \
		[print(f'✅ {d}') for d in dags if importlib.import_module(d)]"

# ─────────────────────────────────────────
# Health Check Commands
# ─────────────────────────────────────────
health:
	@echo "\n🏥 Service Health Checks"
	@echo "──────────────────────────────────────────────────"
	@curl -sf http://localhost:8010/health | python3 -m json.tool || echo "❌ FastAPI unhealthy"
	@curl -sf http://localhost:8080/health | python3 -m json.tool || echo "❌ Airflow unhealthy"
	@curl -sf http://localhost:8501/_stcore/health || echo "❌ Streamlit unhealthy"
	@curl -sf http://localhost:5010/health || echo "❌ MLflow unhealthy"

health-api:
	curl -s http://localhost:8010/health | python3 -m json.tool

smoke-test:
	@echo "💨 Running smoke tests..."
	@python3 -c "\
import requests, sys; \
tests = [ \
    ('GET', 'http://localhost:8010/health', 200), \
    ('GET', 'http://localhost:8010/', 200), \
    ('GET', 'http://localhost:8010/analytics/summary', 200), \
    ('GET', 'http://localhost:8010/analytics/risk-distribution', 200), \
    ('GET', 'http://localhost:8010/alerts?limit=1', 200), \
]; \
failed = []; \
[failed.append(url) or print(f'❌ {url}') \
    if requests.get(url, timeout=5).status_code != exp \
    else print(f'✅ {url}') \
    for method, url, exp in tests]; \
sys.exit(len(failed))"

# ─────────────────────────────────────────
# MinIO Commands
# ─────────────────────────────────────────
minio-setup:
	docker-compose up -d minio-setup
	@echo "✅ MinIO buckets created"

minio-list:
	@echo "\n📦 MinIO Buckets"
	docker exec aml-pipeline-minio-1 mc ls local/ 2>/dev/null || \
		docker run --rm --network aml-pipeline_default minio/mc \
		ls http://minio:9000/

# ─────────────────────────────────────────
# Dev Setup
# ─────────────────────────────────────────
install-dev:
	pip install -r requirements.txt
	pip install pytest pytest-cov flake8 black isort bandit safety httpx pytest-asyncio
	@echo "✅ Dev dependencies installed"

setup: install-dev up minio-setup
	@echo "✅ Development environment ready"