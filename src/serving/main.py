"""
AML Detection API v2.0
FastAPI serving layer สำหรับ AML Pipeline

Architecture:
- startup: load scores.parquet จาก MinIO → dict (RAM efficient)
- startup: load XGBoost model จาก MLflow
- /transactions/{id} → lookup dict + query PostgreSQL
- /alerts → query aml_alerts
- /analytics/* → aggregate stats
- /predict → real-time scoring
"""

import os
import io
import logging
from contextlib import asynccontextmanager
from typing import Optional, List
from datetime import datetime

import boto3
import pandas as pd
import psycopg2
import psycopg2.extras
import xgboost as xgb
import mlflow
import mlflow.xgboost
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from prometheus_fastapi_instrumentator import Instrumentator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Global state
# ─────────────────────────────────────────
app_state = {
    "scores": {},  # {transaction_id: (ml_probability, final_risk_score)}
    "model": None,
    "features": None,
    "threshold": 0.90,
}

FEATURES = [
    "amount",
    "amount_log",
    "tx_hour",
    "tx_day_of_week",
    "is_weekend",
    "is_cross_currency",
    "sender_tx_count_1h",
    "sender_amount_sum_1h",
    "sender_avg_amount",
    "amount_vs_sender_avg",
    "payment_type_risk",
    "is_high_risk_type",
    "is_structuring",
    "is_round_amount",
    "rule_score",
]


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    )


def get_pg():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "aml_db"),
        user=os.getenv("POSTGRES_USER", ""),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )


def get_risk_level(score: float) -> str:
    if score >= 0.90:
        return "CRITICAL"
    elif score >= 0.70:
        return "HIGH"
    elif score >= 0.40:
        return "MEDIUM"
    return "LOW"


# ─────────────────────────────────────────
# Startup / Shutdown
# ─────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Loading scores from MinIO...")
    try:
        s3 = get_s3()
        obj = s3.get_object(Bucket="gold", Key="ml/scores.parquet")
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        app_state["scores"] = {
            row.transaction_id: (float(row.ml_probability), float(row.final_risk_score))
            for row in df.itertuples()
        }
        logger.info(f"Loaded {len(app_state['scores']):,} scores")
        del df
    except Exception as e:
        logger.warning(f"Could not load scores: {e}")

    logger.info("Loading XGBoost model from MLflow...")
    try:
        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
        client = mlflow.MlflowClient()
        versions = client.get_latest_versions("aml_risk_model")
        if versions:
            latest = versions[-1]
            model_uri = f"models:/aml_risk_model/{latest.version}"
            app_state["model"] = mlflow.xgboost.load_model(model_uri)
            app_state["threshold"] = float(latest.tags.get("threshold", 0.90))
            logger.info(
                f"Loaded model version {latest.version}, threshold={app_state['threshold']}"
            )
    except Exception as e:
        logger.warning(f"Could not load model: {e}")

    app_state["features"] = FEATURES
    logger.info("Startup complete")
    yield

    app_state["scores"].clear()
    logger.info("Shutdown complete")


# ─────────────────────────────────────────
# App
# ─────────────────────────────────────────
app = FastAPI(
    title="AML Detection API",
    version="2.0.0",
    description="AML Transaction Monitoring — IBM Consulting Portfolio",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

Instrumentator().instrument(app).expose(app)


# ─────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────
class TransactionResponse(BaseModel):
    transaction_id: str
    timestamp: Optional[str]
    amount: Optional[float]
    payment_type: Optional[str]
    payment_currency: Optional[str]
    receiving_currency: Optional[str]
    sender_account_masked: Optional[str]
    receiver_account_masked: Optional[str]
    sender_bank: Optional[str]
    receiver_bank: Optional[str]
    is_cross_currency: Optional[int]
    is_laundering: Optional[int]
    rule_score: Optional[float]
    ml_probability: Optional[float]
    final_risk_score: Optional[float]
    risk_level: Optional[str]


class AlertResponse(BaseModel):
    alert_id: str
    transaction_id: str
    risk_score: float
    typology: str
    status: str
    created_at: Optional[str]


class AlertUpdateRequest(BaseModel):
    status: str


class PredictRequest(BaseModel):
    amount: float
    amount_log: float
    tx_hour: int
    tx_day_of_week: int
    is_weekend: int
    is_cross_currency: int
    sender_tx_count_1h: int
    sender_amount_sum_1h: float
    sender_avg_amount: float
    amount_vs_sender_avg: float
    payment_type_risk: float
    is_high_risk_type: int
    is_structuring: int
    is_round_amount: int
    rule_score: float


class PredictResponse(BaseModel):
    ml_probability: float
    final_risk_score: float
    risk_level: str
    is_suspicious: bool


# ─────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────


@app.get("/health")
def health():
    pg_ok = False
    minio_ok = False
    scores_loaded = len(app_state["scores"]) > 0
    model_loaded = app_state["model"] is not None

    try:
        conn = get_pg()
        conn.close()
        pg_ok = True
    except Exception:
        pass

    try:
        s3 = get_s3()
        s3.head_bucket(Bucket="gold")
        minio_ok = True
    except Exception:
        pass

    status = "ok" if (pg_ok and minio_ok and scores_loaded) else "degraded"
    return {
        "status": status,
        "postgres": pg_ok,
        "minio": minio_ok,
        "scores_loaded": scores_loaded,
        "scores_count": len(app_state["scores"]),
        "model_loaded": model_loaded,
    }


@app.get("/")
def root():
    return {
        "message": "AML Detection API v2.0",
        "scores_loaded": len(app_state["scores"]),
        "model_loaded": app_state["model"] is not None,
    }


@app.get("/transactions/{transaction_id}", response_model=TransactionResponse)
def get_transaction(transaction_id: str):
    conn = get_pg()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT
                transaction_id, timestamp, amount, payment_type,
                payment_currency, receiving_currency,
                sender_account_masked, receiver_account_masked,
                sender_bank, receiver_bank,
                is_cross_currency, is_laundering, rule_score
            FROM transactions_featured
            WHERE transaction_id = %s
        """,
            (transaction_id,),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Transaction not found")

    result = dict(row)
    result["timestamp"] = str(result["timestamp"]) if result.get("timestamp") else None

    score_data = app_state["scores"].get(transaction_id)
    if score_data:
        result["ml_probability"] = score_data[0]
        result["final_risk_score"] = score_data[1]
        result["risk_level"] = get_risk_level(score_data[1])
    else:
        result["ml_probability"] = None
        result["final_risk_score"] = None
        result["risk_level"] = "UNKNOWN"

    return result


@app.post("/transactions/batch")
def get_transactions_batch(transaction_ids: List[str]):
    if len(transaction_ids) > 1000:
        raise HTTPException(
            status_code=400, detail="Max 1000 transaction_ids per request"
        )

    results = []
    for tx_id in transaction_ids:
        score_data = app_state["scores"].get(tx_id)
        if score_data:
            results.append(
                {
                    "transaction_id": tx_id,
                    "ml_probability": score_data[0],
                    "final_risk_score": score_data[1],
                    "risk_level": get_risk_level(score_data[1]),
                }
            )
        else:
            results.append(
                {
                    "transaction_id": tx_id,
                    "ml_probability": None,
                    "final_risk_score": None,
                    "risk_level": "UNKNOWN",
                }
            )
    return {"results": results, "count": len(results)}


@app.get("/alerts", response_model=List[AlertResponse])
def get_alerts(
    status: Optional[str] = None,
    typology: Optional[str] = None,
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
):
    conn = get_pg()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        conditions = []
        params = []
        if status:
            conditions.append("status = %s")
            params.append(status.upper())
        if typology:
            conditions.append("typology = %s")
            params.append(typology)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.extend([limit, offset])

        cur.execute(
            f"""
            SELECT alert_id, transaction_id, risk_score, typology, status, created_at
            FROM aml_alerts
            {where}
            ORDER BY risk_score DESC
            LIMIT %s OFFSET %s
        """,
            params,
        )

        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    return [
        {**dict(r), "created_at": str(r["created_at"]) if r.get("created_at") else None}
        for r in rows
    ]


@app.get("/alerts/by-transaction/{transaction_id}")
def get_alert_by_transaction(transaction_id: str):
    conn = get_pg()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT alert_id, transaction_id, risk_score, typology, status, created_at
            FROM aml_alerts
            WHERE transaction_id = %s
            LIMIT 1
        """,
            (transaction_id,),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="No alert for this transaction")

    result = dict(row)
    result["created_at"] = (
        str(result["created_at"]) if result.get("created_at") else None
    )
    return result


@app.get("/alerts/{alert_id}")
def get_alert(alert_id: str):
    conn = get_pg()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT a.alert_id, a.transaction_id, a.risk_score, a.typology, a.status, a.created_at,
                   t.amount, t.payment_type, t.sender_account_masked,
                   t.receiver_account_masked, t.timestamp, t.is_laundering, t.rule_score
            FROM aml_alerts a
            LEFT JOIN transactions_featured t ON a.transaction_id = t.transaction_id
            WHERE a.alert_id = %s
        """,
            (alert_id,),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Alert not found")

    result = dict(row)
    result["created_at"] = (
        str(result["created_at"]) if result.get("created_at") else None
    )
    result["timestamp"] = str(result["timestamp"]) if result.get("timestamp") else None

    score_data = app_state["scores"].get(result["transaction_id"])
    if score_data:
        result["ml_probability"] = score_data[0]
        result["risk_level"] = get_risk_level(score_data[1])

    return result


@app.patch("/alerts/{alert_id}")
def update_alert(alert_id: str, body: AlertUpdateRequest):
    valid_statuses = {"OPEN", "INVESTIGATING", "CLOSED"}
    if body.status.upper() not in valid_statuses:
        raise HTTPException(
            status_code=400, detail=f"Status must be one of {valid_statuses}"
        )

    conn = get_pg()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE aml_alerts
            SET status = %s, updated_at = NOW()
            WHERE alert_id = %s
        """,
            (body.status.upper(), alert_id),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Alert not found")
        conn.commit()
        cur.close()
    finally:
        conn.close()

    return {"alert_id": alert_id, "status": body.status.upper(), "updated": True}


@app.get("/analytics/summary")
def get_summary():
    conn = get_pg()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("SELECT COUNT(*) as total FROM transactions_featured")
        total_tx = cur.fetchone()["total"]

        cur.execute(
            "SELECT COUNT(*) as total FROM transactions_featured WHERE is_laundering = 1"
        )
        total_laundering = cur.fetchone()["total"]

        cur.execute("SELECT COUNT(*) as total FROM aml_alerts")
        total_alerts = cur.fetchone()["total"]

        cur.execute("SELECT COUNT(*) as total FROM aml_alerts WHERE status = 'OPEN'")
        open_alerts = cur.fetchone()["total"]

        cur.execute("""
            SELECT typology, COUNT(*) as count,
                   ROUND(AVG(risk_score)::numeric, 4) as avg_score
            FROM aml_alerts
            GROUP BY typology
            ORDER BY count DESC
        """)
        alerts_by_typology = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT sender_account_masked,
                   COUNT(*) as tx_count,
                   ROUND(AVG(rule_score)::numeric, 4) as avg_rule_score
            FROM transactions_featured
            WHERE rule_score >= 0.5
            GROUP BY sender_account_masked
            ORDER BY avg_rule_score DESC
            LIMIT 10
        """)
        top_risk_senders = [dict(r) for r in cur.fetchall()]

        cur.close()
    finally:
        conn.close()

    laundering_rate = round(total_laundering / total_tx * 100, 4) if total_tx > 0 else 0

    return {
        "total_transactions": total_tx,
        "total_laundering": total_laundering,
        "laundering_rate_pct": laundering_rate,
        "total_alerts": total_alerts,
        "open_alerts": open_alerts,
        "alerts_by_typology": alerts_by_typology,
        "top_risk_senders": top_risk_senders,
        "scores_loaded": len(app_state["scores"]),
        "model_loaded": app_state["model"] is not None,
    }


@app.get("/analytics/risk-distribution")
def get_risk_distribution():
    if not app_state["scores"]:
        raise HTTPException(status_code=503, detail="Scores not loaded")

    scores = [v[0] for v in app_state["scores"].values()]

    bins = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    labels = [
        "0-0.1",
        "0.1-0.2",
        "0.2-0.3",
        "0.3-0.4",
        "0.4-0.5",
        "0.5-0.6",
        "0.6-0.7",
        "0.7-0.8",
        "0.8-0.9",
        "0.9-1.0",
    ]

    counts = [0] * 10
    for s in scores:
        idx = min(int(s * 10), 9)
        counts[idx] += 1

    risk_counts = {
        "LOW": sum(1 for s in scores if s < 0.40),
        "MEDIUM": sum(1 for s in scores if 0.40 <= s < 0.70),
        "HIGH": sum(1 for s in scores if 0.70 <= s < 0.90),
        "CRITICAL": sum(1 for s in scores if s >= 0.90),
    }

    return {
        "histogram": [{"bin": labels[i], "count": counts[i]} for i in range(10)],
        "risk_levels": risk_counts,
        "total": len(scores),
        "avg_score": round(sum(scores) / len(scores), 4) if scores else 0,
        "threshold": app_state["threshold"],
    }


@app.get("/analytics/daily-stats")
def get_daily_stats():
    conn = get_pg()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("""
            SELECT
                DATE(timestamp) as date,
                COUNT(*) as transaction_count,
                SUM(is_laundering) as laundering_count,
                ROUND(AVG(rule_score)::numeric, 4) as avg_rule_score
            FROM transactions_featured
            GROUP BY DATE(timestamp)
            ORDER BY date
        """)
        tx_daily = {str(r["date"]): dict(r) for r in cur.fetchall()}

        # ใช้ transaction timestamp แทน alert created_at
        # เพราะ alerts ถูกสร้างตอนรัน pipeline ไม่ใช่วันที่ transaction จริง
        cur.execute("""
            SELECT
                DATE(t.timestamp) as date,
                COUNT(*) as alert_count
            FROM aml_alerts a
            JOIN transactions_featured t ON a.transaction_id = t.transaction_id
            GROUP BY DATE(t.timestamp)
            ORDER BY date
        """)
        alert_daily = {str(r["date"]): r["alert_count"] for r in cur.fetchall()}

        cur.close()
    finally:
        conn.close()

    result = []
    for date_str, tx_data in tx_daily.items():
        result.append(
            {
                "date": date_str,
                "transaction_count": tx_data["transaction_count"],
                "laundering_count": int(tx_data["laundering_count"] or 0),
                "avg_rule_score": float(tx_data["avg_rule_score"] or 0),
                "alert_count": alert_daily.get(date_str, 0),
            }
        )

    return result


@app.get("/analytics/payment-type-stats")
def get_payment_type_stats():
    conn = get_pg()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                payment_type,
                COUNT(*) as tx_count,
                ROUND(AVG(rule_score)::numeric, 4) as avg_rule_score,
                ROUND(AVG(amount)::numeric, 2) as avg_amount,
                SUM(is_laundering) as laundering_count,
                ROUND(
                    (SUM(is_laundering)::float / COUNT(*) * 100)::numeric, 4
                ) as laundering_rate
            FROM transactions_featured
            GROUP BY payment_type
            ORDER BY avg_rule_score DESC
        """)
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    return [dict(r) for r in rows]


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    if app_state["model"] is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    features = [getattr(req, f) for f in FEATURES]
    dmatrix = xgb.DMatrix([features], feature_names=FEATURES)
    prob = float(app_state["model"].predict(dmatrix)[0])
    threshold = app_state["threshold"]

    return {
        "ml_probability": round(prob, 4),
        "final_risk_score": round(prob, 4),
        "risk_level": get_risk_level(prob),
        "is_suspicious": prob >= threshold,
    }
