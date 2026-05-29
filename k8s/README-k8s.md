# Kubernetes Deployment

This directory contains Kubernetes manifests for deploying the AML pipeline's serving layer (FastAPI + Streamlit) to a production cluster.

> **Note:** Airflow and the data pipeline itself run via Docker Compose on-premise or on a VM. Only the serving layer is K8s-deployed, since it requires horizontal scaling and health-check-based rollouts.

---

## Files

| File | Purpose |
|------|---------|
| `fastapi-deployment.yaml` | FastAPI serving layer (2 replicas) |
| `fastapi-service.yaml` | ClusterIP service for FastAPI |
| `streamlit-deployment.yaml` | Streamlit dashboard (1 replica) |
| `streamlit-service.yaml` | ClusterIP service for Streamlit |
| `ingress.yaml` | Nginx ingress routing /api → FastAPI, / → Streamlit |
| `secrets.example.yaml` | Secret template (do not commit real values) |

---

## Deploy

```bash
# 1. Create namespace
kubectl create namespace aml

# 2. Create secrets (fill in real values first)
cp k8s/secrets.example.yaml k8s/secrets.yaml
# edit secrets.yaml with real credentials
kubectl apply -f k8s/secrets.yaml -n aml

# 3. Deploy serving layer
kubectl apply -f k8s/fastapi-deployment.yaml -n aml
kubectl apply -f k8s/fastapi-service.yaml -n aml
kubectl apply -f k8s/streamlit-deployment.yaml -n aml
kubectl apply -f k8s/streamlit-service.yaml -n aml
kubectl apply -f k8s/ingress.yaml -n aml

# 4. Verify
kubectl get pods -n aml
kubectl get svc -n aml
```

---

## Design Decisions

**Why 2 replicas for FastAPI?**
FastAPI loads scores.parquet (297 MB) into RAM at startup for O(1) lookup. Two replicas ensure availability during rolling updates — one pod stays ready while the other restarts.

**Why 1 replica for Streamlit?**
Streamlit is stateless and read-only. A single replica is sufficient for compliance dashboard use; scale up if concurrent users increase.

**Why ClusterIP instead of LoadBalancer?**
Both services are internal — Ingress handles external traffic routing. ClusterIP avoids provisioning separate cloud load balancers per service, reducing cost.

**Why only the serving layer in K8s?**
Airflow with its DAG schedules, volume mounts, and worker pools is operationally simpler on Docker Compose for a single-node setup. K8s adds value where horizontal scaling and self-healing matter — which is the API serving layer.

---

## Resource Sizing

| Pod | Memory request | Memory limit | Reason |
|-----|---------------|-------------|--------|
| fastapi | 512 Mi | 1 Gi | scores.parquet 297 MB in RAM |
| streamlit | 256 Mi | 512 Mi | read-only dashboard |