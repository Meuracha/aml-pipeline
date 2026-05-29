# AML Transaction Monitoring Pipeline
### IBM Consulting Portfolio — End-to-End MLOps

[![CI](https://github.com/meuracha/aml-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/meuracha/aml-pipeline/actions/workflows/ci.yml)
[![CD](https://github.com/meuracha/aml-pipeline/actions/workflows/cd.yml/badge.svg)](https://github.com/meuracha/aml-pipeline/actions/workflows/cd.yml)
[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://python.org)
[![Airflow](https://img.shields.io/badge/Airflow-2.8.0-green)](https://airflow.apache.org)
[![XGBoost](https://img.shields.io/badge/XGBoost-AUC--ROC%200.9362-orange)](https://xgboost.readthedocs.io)

---

## Overview

End-to-end **Anti-Money Laundering (AML)** transaction monitoring system built on the IBM AML Dataset (6.9M transactions). Demonstrates production-grade MLOps: medallion data architecture, XGBoost risk scoring, FastAPI serving, and Streamlit compliance dashboard.

| Metric | Value |
|--------|-------|
| Dataset | 6.9M transactions (IBM AML) |
| Alerts generated | 103,899 |
| Model AUC-ROC | 0.9362 |
| CRITICAL risk transactions | 173,330 (2.5%) |
| API endpoints | 12 |

---

## Data Flow

![Data Flow Diagram](docs/diagrams/aml_data_flow.svg)

> **Storage design:** MinIO stores large Parquet files (scores.parquet 297 MB) for full-file O(1) lookup. PostgreSQL stores structured queryable data (transactions, alerts) for filter/join/aggregate operations.

---

## Screenshots

### Streamlit Executive Summary Dashboard
![Streamlit Executive Summary](docs/diagrams/executive_summary.png)
![Streamlit Executive Summary](docs/diagrams/executive_summary_2.png)

### Streamlit Alert Management Dashboard
![Alert Management](docs/diagrams/Alert_management.png)

### Streamlit Risk Analytics Dashboard
![Risk Analytics](docs/diagrams/risk_analytics.png)
![Risk Analytics](docs/diagrams/risk_analytics_1.png)
![Risk Analytics](docs/diagrams/risk_analytics_2.png)

### Streamlit Model Performance Dashboard
![Model Performance](docs/diagrams/model_performance.png)
![Model Performance](docs/diagrams/model_performance_1.png)
![Model Performance](docs/diagrams/model_performance_2.png)

### Streamlit Transaction Search
![Transaction Search](docs/diagrams/transaction_search.png)

### Airflow DAG View
![Airflow Dags](docs/diagrams/airflow_dags.png)

### MLflow Experiment Tracking
![MLflow Run](docs/diagrams/mlflow_run.png)

### MLflow Model Registry
![MLflow Registry](docs/diagrams/mlflow_registry.png)

### FastAPI Swagger UI
![Fast Api](docs/diagrams/fast_api.png)

### Marquez Data Lineage
![Marquez](docs/diagrams/merquez.png)
![Marquez](docs/diagrams/merquez_1.png)

### MinIO Storage
![Minio](docs/diagrams/minio.png)

### Prometheus Monitoring
![Prometheus Target](docs/diagrams/prometheus_target.png)
![Prometheus Rule Health](docs/diagrams/prometheus_rulehealth.png)

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2.8.0 |
| Data Lake | MinIO (S3-compatible) |
| Database | PostgreSQL 15 |
| Feature Engineering | DuckDB + psycopg2 |
| ML Model | XGBoost (binary:logistic) |
| ML Tracking | MLflow 2.10.0 |
| Model Explainability | SHAP |
| API Serving | FastAPI 0.109 + Uvicorn |
| Dashboard | Streamlit + Plotly |
| Reverse Proxy | Nginx |
| Monitoring | Prometheus + Grafana + Loki |
| Data Lineage | Marquez (OpenLineage) |
| CI/CD | GitHub Actions |
| Containerization | Docker + Docker Compose |

---

## ML Model Performance

| Metric | Value | Notes |
|--------|-------|-------|
| Val AUC-ROC | **0.9362** | vs Random baseline 0.50 |
| Val AUC-PR | 0.0066 | Expected low (0.05% positive rate) |
| Val Recall | 27.83% | At threshold=0.90 |
| Val Precision | 0.83% | Extreme class imbalance 1:2,155 |
| Val F1 | 0.0162 | |
| Best Threshold | 0.90 | Optimized for F1 |
| Training Rows | 5,091,431 | Sep 01-07, 2022 |
| Val Rows | 1,549,510 | Sep 08-09, 2022 |
| scale_pos_weight | 2,155.47 | neg/pos ratio |

**Key Design Decisions:**
- Time-based train/val/test split → no data leakage
- `scale_pos_weight=2155` to handle extreme imbalance
- Threshold tuned on val set (not test) → unbiased evaluation
- SHAP for model explainability (compliance requirement)

---

## Pipeline Results

| Layer | Metric | Value |
|-------|--------|-------|
| Bronze | Files | 14 parquet files |
| Bronze | Dead Letter | 8 invalid rows |
| Silver | Rows | 6,924,041 |
| Gold | Alerts | 103,899 |
| Gold | cross_currency_high_risk | 96,715 (93.1%) |
| Gold | structuring_rapid | 5,276 (5.1%) |
| Gold | structuring | 1,908 (1.8%) |
| ML | CRITICAL risk | 173,330 (2.5%) |
| ML | HIGH risk | 541,656 (8.4%) |
| ML | scores.parquet | 297 MB |

---

## Quick Start

### Prerequisites

- Docker Desktop (8GB RAM recommended)
- macOS / Linux

### 1. Clone & Configure

```bash
git clone https://github.com/meuracha/aml-pipeline.git
cd aml-pipeline
cp .env.example .env   # แก้ credentials ถ้าต้องการ
```

### 2. Start Services

```bash
make up
# หรือ
docker-compose up -d postgres minio airflow-webserver airflow-scheduler mlflow fastapi nginx streamlit
```

### 3. Setup MinIO Buckets

```bash
make minio-setup
```

### 4. Download Dataset

```
1. ไปที่ https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml
2. Download LI-Small_Trans.csv
3. วางไว้ที่ data/raw/LI-Small_Trans.csv
```

### 5. Run Pipeline

```bash
make dag-run
# Bronze → Silver → Gold → ML
```

หรือ trigger ทีละ DAG ผ่าน Airflow UI: http://localhost:8080

### 6. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | see `.env` |
| FastAPI | http://localhost:8010 | — |
| FastAPI Docs | http://localhost:8010/docs | — |
| Streamlit | http://localhost:8501 | — |
| MLflow | http://localhost:5010 | — |
| MinIO | http://localhost:9001 | see `.env` |
| Grafana | http://localhost:3000 | see `.env` |
| Marquez | http://localhost:3001 | — |

---

## Project Structure

```
aml-pipeline/
├── .github/
│   └── workflows/
│       ├── ci.yml              # lint + test + security + docker build
│       └── cd.yml              # build + push + deploy + smoke test
├── dags/
│   ├── aml_bronze_dag.py       # CSV → MinIO parquet
│   ├── aml_silver_dag.py       # clean + standardize → PostgreSQL
│   ├── aml_gold_dag.py         # feature engineering + alerts
│   ├── aml_ml_dag.py           # XGBoost train + evaluate + score
│   └── config.py               # shared connections
├── src/
│   ├── serving/
│   │   └── main.py             # FastAPI endpoints
│   └── dashboard/
│       └── app.py              # Streamlit dashboard
├── tests/
│   ├── conftest.py             # shared fixtures
│   ├── test_data_quality.py    # Bronze/Silver/Gold quality
│   ├── test_api.py             # FastAPI endpoints
│   ├── test_dags.py            # DAG imports + structure
│   └── test_ml.py              # model validation gate
├── docker/
│   ├── Dockerfile.airflow
│   ├── Dockerfile.api
│   ├── Dockerfile.mlflow
│   ├── Dockerfile.streamlit
│   ├── init.sql                # PostgreSQL schema
│   └── nginx.conf
├── k8s/
│   ├── fastapi-deployment.yaml
│   ├── fastapi-service.yaml
│   ├── streamlit-deployment.yaml
│   ├── streamlit-service.yaml
│   ├── ingress.yaml
│   ├── secrets.example.yaml
│   └── README-k8s.md
├── docs/
│   └── diagrams/
│       ├── aml_data_flow.svg           # data flow diagram (all layers)
│       └── *.png                       # screenshots
├── data/
│   └── raw/                    # LI-Small_Trans.csv (not committed)
├── docker-compose.yml
├── data_contract.yaml
├── Makefile
└── requirements.txt
```

---

## Development

### Run Tests

```bash
make test                  # all tests
make test-unit             # unit tests only
make test-cov              # with coverage report
make test-data             # data quality tests
make test-api              # API tests
```

### Code Quality

```bash
make lint                  # flake8
make format                # black + isort
make security              # bandit + safety
```

### DAG Management

```bash
make dag-status            # show run status
make dag-validate          # validate imports in Docker
make health                # health check all services
```

---

## API Endpoints

```
GET  /health                          → service health check
GET  /transactions/{id}               → transaction + ML risk score
POST /transactions/batch              → bulk score lookup
GET  /alerts                          → alert list (filter by status/typology)
GET  /alerts/{id}                     → alert detail
PATCH /alerts/{id}                    → update alert status
GET  /alerts/by-transaction/{tx_id}   → alert for transaction
GET  /analytics/summary               → KPIs + top senders
GET  /analytics/risk-distribution     → score histogram
GET  /analytics/daily-stats           → daily transaction + alert trend
GET  /analytics/payment-type-stats    → risk by payment type
POST /predict                         → real-time ML scoring
```

---

## CI/CD Pipeline

### CI (every PR + push)
```
1. Lint (flake8, black, isort)
2. Security scan (bandit, safety)
3. Unit tests (pytest + coverage)
4. DAG validation (import + structure)
5. Docker build test (all images)
6. Model validation gate (AUC-ROC ≥ 0.85) — trigger with [retrain] in commit message
```

### CD (merge to main)
```
1. Build & push Docker images → GitHub Container Registry
2. Deploy via SSH (docker-compose up)
3. Smoke test (curl all health endpoints)
4. Auto-create GitHub Release with changelog
```

---

## Kubernetes

Serving layer (FastAPI + Streamlit) is designed for Kubernetes deployment.
See [`k8s/README-k8s.md`](k8s/README-k8s.md) for manifests and deployment guide.

---

## Architecture Decisions

**Why MinIO for scores instead of PostgreSQL UPDATE?**
Updating 6.9M rows in PostgreSQL takes 3-6 hours. Saving scores as parquet to MinIO takes ~15 minutes. FastAPI loads the 297MB file into memory at startup for O(1) lookup.

**Why time-based split instead of random?**
AML fraud detection in production always predicts the future from the past. Random split would leak future patterns into training, causing optimistic metrics.

**Why threshold=0.90?**
Compliance officers have limited capacity to review alerts. High threshold reduces false positives (currently 25k FP at threshold=0.90). Threshold can be tuned via the Streamlit simulator.

**Why XGBoost instead of deep learning?**
XGBoost is interpretable via SHAP, trains fast on tabular data, and handles class imbalance well with `scale_pos_weight`. Deep learning would require more data and longer training time with marginal benefit for this dataset.

---

*Built with ❤️ for IBM Consulting Data & AI Practice*