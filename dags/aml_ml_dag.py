import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "aml_pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "sla": timedelta(hours=4),
}


def emit_lineage(
    input_dataset,
    output_dataset,
    run_id,
    job_name,
    input_namespace="postgres",
    output_namespace="mlflow",
    event_type="COMPLETE",
):
    import sys
    import uuid

    import requests

    sys.path.insert(0, "/opt/airflow/dags")
    from config import MARQUEZ_URL

    unique_run_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, run_id + job_name))
    event = {
        "eventType": event_type,
        "eventTime": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "run": {"runId": unique_run_id},
        "job": {"namespace": "aml_pipeline", "name": job_name},
        "inputs": [{"namespace": input_namespace, "name": input_dataset}],
        "outputs": [{"namespace": output_namespace, "name": output_dataset}],
        "producer": "aml_pipeline/airflow",
    }
    for attempt in range(3):
        try:
            resp = requests.post(
                f"{MARQUEZ_URL}/api/v1/lineage", json=event, timeout=10
            )
            if resp.status_code == 201:
                logger.info(f"Lineage [{event_type}]: {job_name}")
            else:
                logger.warning(f"Lineage failed: {resp.status_code}")
            break
        except Exception as e:
            if attempt < 2:
                logger.warning(f"Lineage retry {attempt+1}/3: {e}")
            else:
                logger.warning(f"Lineage emit error (non-critical): {e}")


def log_audit(
    pg_conn,
    dag_id,
    run_id,
    task_id,
    layer,
    status,
    rows_processed=0,
    duration_seconds=0,
    laundering_rate=0.0,
    error_message=None,
):
    cur = pg_conn.cursor()
    cur.execute(
        """
        INSERT INTO pipeline_audit_log
            (dag_id, run_id, task_id, layer, status, rows_processed,
             duration_seconds, laundering_rate, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
        (
            dag_id,
            run_id,
            task_id,
            layer,
            status,
            rows_processed,
            duration_seconds,
            laundering_rate,
            error_message,
        ),
    )
    pg_conn.commit()
    cur.close()


def prepare_dataset(**context):
    """
    t1: อ่าน Gold features จาก PostgreSQL
        time-based split Train/Val/Test
        save ลง MinIO ml/
    """
    import io
    import sys
    import time

    sys.path.insert(0, "/opt/airflow/dags")
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from config import get_pg_conn, get_s3_client

    start_time = time.time()
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    task_id = context["task"].task_id
    job_name = "ml.prepare_dataset"

    pg_conn = get_pg_conn()
    s3 = get_s3_client()

    emit_lineage(
        "transactions_featured", "ml/dataset", run_id, job_name, event_type="START"
    )

    feature_cols = [
        "amount",
        "amount_log",
        "tx_hour",
        "tx_day_of_week",
        "is_weekend",
        "is_cross_currency",
        "is_laundering",
        "sender_tx_count_1h",
        "sender_amount_sum_1h",
        "sender_avg_amount",
        "amount_vs_sender_avg",
        "payment_type_risk",
        "is_high_risk_type",
        "is_structuring",
        "is_round_amount",
        "rule_score",
        "timestamp",
    ]

    logger.info("Reading gold features from PostgreSQL...")
    BATCH_SIZE = 200_000
    batch_cur = pg_conn.cursor("ml_dataset_cursor")
    batch_cur.execute(f"""
        SELECT {', '.join(feature_cols)}
        FROM transactions_featured
        ORDER BY timestamp
    """)

    first_batch = batch_cur.fetchmany(BATCH_SIZE)
    if not first_batch:
        raise ValueError("No data in transactions_featured!")

    cols = [desc[0] for desc in batch_cur.description]
    chunks = [pd.DataFrame(first_batch, columns=cols)]

    while True:
        rows = batch_cur.fetchmany(BATCH_SIZE)
        if not rows:
            break
        chunks.append(pd.DataFrame(rows, columns=cols))
        logger.info(f"Read {sum(len(c) for c in chunks):,} rows...")

    batch_cur.close()
    df = pd.concat(chunks, ignore_index=True)
    del chunks
    logger.info(f"Total: {len(df):,} rows")

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    train_end = pd.Timestamp("2022-09-07 23:59:59")
    val_end = pd.Timestamp("2022-09-09 23:59:59")

    df_train = df[df["timestamp"] <= train_end].copy()
    df_val = df[(df["timestamp"] > train_end) & (df["timestamp"] <= val_end)].copy()
    df_test = df[df["timestamp"] > val_end].copy()

    logger.info(
        f"Train: {len(df_train):,} rows, laundering: {df_train['is_laundering'].sum():,}"
    )
    logger.info(
        f"Val:   {len(df_val):,} rows, laundering: {df_val['is_laundering'].sum():,}"
    )
    logger.info(
        f"Test:  {len(df_test):,} rows, laundering: {df_test['is_laundering'].sum():,}"
    )

    for split_df in [df_train, df_val, df_test]:
        split_df.drop(columns=["timestamp"], inplace=True)

    def save_parquet(df_split, key):
        buf = io.BytesIO()
        table = pa.Table.from_pandas(df_split, preserve_index=False)
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        s3.put_object(Bucket="gold", Key=key, Body=buf.getvalue())
        logger.info(f"Saved: gold/{key} ({len(df_split):,} rows)")

    save_parquet(df_train, "ml/train.parquet")
    save_parquet(df_val, "ml/val.parquet")
    save_parquet(df_test, "ml/test.parquet")

    neg = int((df_train["is_laundering"] == 0).sum())
    pos = int((df_train["is_laundering"] == 1).sum())
    scale_pos_weight = round(neg / pos, 2) if pos > 0 else 1000

    duration = time.time() - start_time
    log_audit(
        pg_conn,
        dag_id,
        run_id,
        task_id,
        "ml",
        "success",
        rows_processed=len(df),
        duration_seconds=round(duration, 2),
    )
    emit_lineage(
        "transactions_featured", "ml/dataset", run_id, job_name, event_type="COMPLETE"
    )

    pg_conn.commit()
    pg_conn.close()

    context["ti"].xcom_push(key="train_rows", value=len(df_train))
    context["ti"].xcom_push(key="val_rows", value=len(df_val))
    context["ti"].xcom_push(key="test_rows", value=len(df_test))
    context["ti"].xcom_push(key="scale_pos_weight", value=scale_pos_weight)
    context["ti"].xcom_push(key="pos_count", value=pos)
    context["ti"].xcom_push(key="neg_count", value=neg)

    logger.info(
        f"Dataset ready. scale_pos_weight={scale_pos_weight} in {duration:.1f}s"
    )
    return len(df)


def train_model(**context):
    """
    t2: train XGBoost บน train set
        early stopping บน val set
        threshold tuning บน val set
        log metrics ใน MLflow
    """
    import io
    import os
    import sys
    import time

    sys.path.insert(0, "/opt/airflow/dags")
    import mlflow
    import mlflow.xgboost
    import numpy as np
    import pandas as pd
    import xgboost as xgb
    from config import get_s3_client
    from sklearn.metrics import (
        average_precision_score,
        confusion_matrix,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )

    start_time = time.time()
    run_id = context["run_id"]
    job_name = "ml.train_model"

    s3 = get_s3_client()
    scale_pos_weight = context["ti"].xcom_pull(
        task_ids="prepare_dataset", key="scale_pos_weight"
    )

    emit_lineage(
        "ml/dataset",
        "mlflow/aml_risk_model",
        run_id,
        job_name,
        input_namespace="minio",
        output_namespace="mlflow",
        event_type="START",
    )

    def load_parquet(key):
        obj = s3.get_object(Bucket="gold", Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))

    logger.info("Loading train/val data...")
    df_train = load_parquet("ml/train.parquet")
    df_val = load_parquet("ml/val.parquet")

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
    TARGET = "is_laundering"

    X_train = df_train[FEATURES].astype(float)
    y_train = df_train[TARGET].astype(int)
    X_val = df_val[FEATURES].astype(float)
    y_val = df_val[TARGET].astype(int)

    logger.info(f"Train: {len(X_train):,} rows")
    logger.info(f"Val:   {len(X_val):,} rows")
    logger.info(f"scale_pos_weight: {scale_pos_weight}")

    dtrain = xgb.DMatrix(X_train, label=y_train, feature_names=FEATURES)
    dval = xgb.DMatrix(X_val, label=y_val, feature_names=FEATURES)

    params = {
        "objective": "binary:logistic",
        "eval_metric": ["aucpr", "auc"],
        "scale_pos_weight": scale_pos_weight,
        "max_depth": 6,
        "learning_rate": 0.1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 5,
        "tree_method": "hist",
        "seed": 42,
    }

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    mlflow.set_experiment("aml_risk_model")

    with mlflow.start_run(
        run_name=f'xgboost_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}'
    ) as mlrun:

        logger.info("Training XGBoost...")
        model = xgb.train(
            params,
            dtrain,
            num_boost_round=500,
            evals=[(dtrain, "train"), (dval, "val")],
            early_stopping_rounds=50,
            verbose_eval=50,
        )

        y_val_prob = model.predict(dval)

        logger.info("Tuning threshold on val set...")
        best_f1 = 0
        best_threshold = 0.5

        for threshold in np.arange(0.05, 0.95, 0.05):
            y_pred = (y_val_prob >= threshold).astype(int)
            if y_pred.sum() == 0:
                continue
            f1 = f1_score(y_val, y_pred, zero_division=0)
            if f1 > best_f1:
                best_f1 = f1
                best_threshold = threshold

        logger.info(f"Best threshold: {best_threshold:.2f} (F1={best_f1:.4f})")

        y_val_pred = (y_val_prob >= best_threshold).astype(int)
        val_auc_roc = roc_auc_score(y_val, y_val_prob)
        val_auc_pr = average_precision_score(y_val, y_val_prob)
        val_recall = recall_score(y_val, y_val_pred, zero_division=0)
        val_precision = precision_score(y_val, y_val_pred, zero_division=0)
        val_f1 = f1_score(y_val, y_val_pred, zero_division=0)
        cm = confusion_matrix(y_val, y_val_pred)

        logger.info(f"Val AUC-ROC  : {val_auc_roc:.4f}")
        logger.info(f"Val AUC-PR   : {val_auc_pr:.4f}")
        logger.info(f"Val Recall   : {val_recall:.4f}")
        logger.info(f"Val Precision: {val_precision:.4f}")
        logger.info(f"Val F1       : {val_f1:.4f}")
        logger.info(f"Confusion Matrix:\n{cm}")

        mlflow.log_params(params)
        mlflow.log_param("best_threshold", best_threshold)
        mlflow.log_param("scale_pos_weight", scale_pos_weight)
        mlflow.log_param("features", FEATURES)
        mlflow.log_metric("val_auc_roc", val_auc_roc)
        mlflow.log_metric("val_auc_pr", val_auc_pr)
        mlflow.log_metric("val_recall", val_recall)
        mlflow.log_metric("val_precision", val_precision)
        mlflow.log_metric("val_f1", val_f1)
        mlflow.log_metric("best_rounds", model.best_iteration)
        mlflow.xgboost.log_model(model, artifact_path="xgboost_model")

        mlrun_id = mlrun.info.run_id
        logger.info(f"MLflow run_id: {mlrun_id}")

    duration = time.time() - start_time
    emit_lineage(
        "ml/dataset",
        "mlflow/aml_risk_model",
        run_id,
        job_name,
        input_namespace="minio",
        output_namespace="mlflow",
        event_type="COMPLETE",
    )

    context["ti"].xcom_push(key="mlrun_id", value=mlrun_id)
    context["ti"].xcom_push(key="best_threshold", value=float(best_threshold))
    context["ti"].xcom_push(key="val_auc_roc", value=float(val_auc_roc))
    context["ti"].xcom_push(key="val_auc_pr", value=float(val_auc_pr))
    context["ti"].xcom_push(key="val_recall", value=float(val_recall))
    context["ti"].xcom_push(key="val_f1", value=float(val_f1))
    context["ti"].xcom_push(key="features", value=FEATURES)

    logger.info(f"Training done in {duration:.1f}s")
    return mlrun_id


def evaluate_model(**context):
    """
    t3: evaluate บน test set
        SHAP feature importance (sample 5000)
        log ใน MLflow
    """
    import io
    import os
    import sys
    import time

    sys.path.insert(0, "/opt/airflow/dags")
    import matplotlib
    import mlflow
    import mlflow.xgboost
    import numpy as np
    import pandas as pd
    import shap
    import xgboost as xgb
    from config import get_s3_client

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from sklearn.metrics import (
        average_precision_score,
        classification_report,
        confusion_matrix,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )

    start_time = time.time()
    run_id = context["run_id"]
    job_name = "ml.evaluate_model"

    mlrun_id = context["ti"].xcom_pull(task_ids="train_model", key="mlrun_id")
    threshold = context["ti"].xcom_pull(task_ids="train_model", key="best_threshold")
    FEATURES = context["ti"].xcom_pull(task_ids="train_model", key="features")

    s3 = get_s3_client()
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))

    emit_lineage(
        "mlflow/aml_risk_model",
        "mlflow/aml_risk_model",
        run_id,
        job_name,
        input_namespace="mlflow",
        output_namespace="mlflow",
        event_type="START",
    )

    logger.info("Loading test data...")
    obj = s3.get_object(Bucket="gold", Key="ml/test.parquet")
    df_test = pd.read_parquet(io.BytesIO(obj["Body"].read()))

    X_test = df_test[FEATURES].astype(float)
    y_test = df_test["is_laundering"].astype(int)

    logger.info(f"Loading model from MLflow run: {mlrun_id}")
    model_uri = f"runs:/{mlrun_id}/xgboost_model"
    model = mlflow.xgboost.load_model(model_uri)

    dtest = xgb.DMatrix(X_test, label=y_test, feature_names=FEATURES)
    y_test_prob = model.predict(dtest)
    y_test_pred = (y_test_prob >= threshold).astype(int)

    test_auc_roc = roc_auc_score(y_test, y_test_prob)
    test_auc_pr = average_precision_score(y_test, y_test_prob)
    test_recall = recall_score(y_test, y_test_pred, zero_division=0)
    test_precision = precision_score(y_test, y_test_pred, zero_division=0)
    test_f1 = f1_score(y_test, y_test_pred, zero_division=0)
    cm = confusion_matrix(y_test, y_test_pred)

    logger.info(f"Test AUC-ROC  : {test_auc_roc:.4f}")
    logger.info(f"Test AUC-PR   : {test_auc_pr:.4f}")
    logger.info(f"Test Recall   : {test_recall:.4f}")
    logger.info(f"Test Precision: {test_precision:.4f}")
    logger.info(f"Test F1       : {test_f1:.4f}")
    logger.info(f"Confusion Matrix:\n{cm}")
    logger.info(f"\n{classification_report(y_test, y_test_pred)}")

    logger.info("Computing SHAP values (sample 5000 rows)...")
    sample_size = min(5000, len(X_test))
    X_sample = X_test.sample(sample_size, random_state=42)

    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_sample)

    plt.figure(figsize=(10, 6))
    shap.summary_plot(
        shap_values, X_sample, feature_names=FEATURES, show=False, max_display=15
    )
    plt.tight_layout()
    shap_path = "/tmp/shap_summary.png"
    plt.savefig(shap_path, dpi=100, bbox_inches="tight")
    plt.close()

    shap_importance = pd.DataFrame(
        {"feature": FEATURES, "importance": np.abs(shap_values).mean(axis=0)}
    ).sort_values("importance", ascending=False)

    logger.info("SHAP Feature Importance:")
    for _, row in shap_importance.iterrows():
        logger.info(f"  {row['feature']}: {row['importance']:.4f}")

    with mlflow.start_run(run_id=mlrun_id):
        mlflow.log_metric("test_auc_roc", test_auc_roc)
        mlflow.log_metric("test_auc_pr", test_auc_pr)
        mlflow.log_metric("test_recall", test_recall)
        mlflow.log_metric("test_precision", test_precision)
        mlflow.log_metric("test_f1", test_f1)
        mlflow.log_artifact(shap_path, "shap")
        for _, row in shap_importance.iterrows():
            mlflow.log_metric(f"shap_{row['feature']}", row["importance"])

    duration = time.time() - start_time
    emit_lineage(
        "mlflow/aml_risk_model",
        "mlflow/aml_risk_model",
        run_id,
        job_name,
        input_namespace="mlflow",
        output_namespace="mlflow",
        event_type="COMPLETE",
    )

    context["ti"].xcom_push(key="test_auc_roc", value=float(test_auc_roc))
    context["ti"].xcom_push(key="test_auc_pr", value=float(test_auc_pr))
    context["ti"].xcom_push(key="test_recall", value=float(test_recall))
    context["ti"].xcom_push(key="test_f1", value=float(test_f1))
    context["ti"].xcom_push(
        key="shap_importance", value=shap_importance.to_dict("records")
    )

    logger.info(f"Evaluation done in {duration:.1f}s")
    return test_auc_roc


def register_model(**context):
    """
    t4: register model ใน MLflow Model Registry

    Architecture ใหม่ (เร็วกว่าเดิมมาก ~15 นาที):
    1. register model ใน MLflow
    2. score ทีละ batch → collect ทั้งหมด
    3. save scores.parquet ลง MinIO (gold/ml/scores.parquet)
       → transaction_id, ml_probability, final_risk_score
    4. FastAPI และ Streamlit อ่าน scores จาก MinIO แทน PostgreSQL

    ไม่ UPDATE PostgreSQL ทีละ row อีกต่อไป
    → ไม่ OOM, ไม่ timeout, เร็วกว่า 10-20x
    """
    import io
    import os
    import sys
    import time

    sys.path.insert(0, "/opt/airflow/dags")
    import mlflow
    import mlflow.xgboost
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import xgboost as xgb
    from config import get_pg_conn, get_s3_client
    from mlflow import MlflowClient

    start_time = time.time()
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    task_id = context["task"].task_id
    job_name = "ml.register_model"

    mlrun_id = context["ti"].xcom_pull(task_ids="train_model", key="mlrun_id")
    threshold = context["ti"].xcom_pull(task_ids="train_model", key="best_threshold")
    FEATURES = context["ti"].xcom_pull(task_ids="train_model", key="features")
    test_auc_roc = context["ti"].xcom_pull(
        task_ids="evaluate_model", key="test_auc_roc"
    )
    test_recall = context["ti"].xcom_pull(task_ids="evaluate_model", key="test_recall")
    test_f1 = context["ti"].xcom_pull(task_ids="evaluate_model", key="test_f1")

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))

    emit_lineage(
        "mlflow/aml_risk_model",
        "ml/scores",
        run_id,
        job_name,
        input_namespace="mlflow",
        output_namespace="minio",
        event_type="START",
    )

    # register model
    logger.info("Registering model in MLflow Model Registry...")
    model_uri = f"runs:/{mlrun_id}/xgboost_model"
    mv = mlflow.register_model(model_uri, "aml_risk_model")

    client = MlflowClient()
    client.set_registered_model_tag("aml_risk_model", "team", "aml_pipeline")
    client.set_model_version_tag(
        "aml_risk_model", mv.version, "threshold", str(threshold)
    )
    client.set_model_version_tag(
        "aml_risk_model", mv.version, "test_auc_roc", str(round(test_auc_roc, 4))
    )
    client.set_model_version_tag(
        "aml_risk_model", mv.version, "test_recall", str(round(test_recall, 4))
    )
    client.set_model_version_tag(
        "aml_risk_model", mv.version, "test_f1", str(round(test_f1, 4))
    )
    logger.info(f"Model registered: aml_risk_model version {mv.version}")

    # load model
    logger.info("Loading model for scoring...")
    model = mlflow.xgboost.load_model(model_uri)

    # score ทีละ batch แล้ว collect ผลลัพธ์ทั้งหมด
    logger.info("Scoring all transactions...")
    pg_conn = get_pg_conn()
    s3 = get_s3_client()

    BATCH_SIZE = 200_000
    offset = 0
    total_scored = 0
    all_tx_ids = []
    all_scores = []

    while True:
        cur = pg_conn.cursor()
        cur.execute(f"""
            SELECT transaction_id, {', '.join(FEATURES)}
            FROM transactions_featured
            ORDER BY timestamp
            LIMIT {BATCH_SIZE} OFFSET {offset}
        """)
        rows = cur.fetchall()
        cur.close()

        if not rows:
            break

        cols = ["transaction_id"] + FEATURES
        df_batch = pd.DataFrame(rows, columns=cols)

        X_batch = df_batch[FEATURES].astype(float)
        ml_prob = model.predict(xgb.DMatrix(X_batch, feature_names=FEATURES))

        all_tx_ids.extend(df_batch["transaction_id"].tolist())
        all_scores.extend(ml_prob.tolist())

        total_scored += len(df_batch)
        offset += BATCH_SIZE
        logger.info(f"Scored {total_scored:,} transactions...")
        del df_batch

    logger.info(f"All {total_scored:,} transactions scored")

    # save scores ลง MinIO เป็น parquet
    logger.info("Saving scores to MinIO gold/ml/scores.parquet...")
    df_scores = pd.DataFrame(
        {
            "transaction_id": all_tx_ids,
            "ml_probability": all_scores,
            "final_risk_score": all_scores,
        }
    )
    del all_tx_ids, all_scores

    buf = io.BytesIO()
    table = pa.Table.from_pandas(df_scores, preserve_index=False)
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    s3.put_object(Bucket="gold", Key="ml/scores.parquet", Body=buf.getvalue())
    logger.info(
        f"Saved: gold/ml/scores.parquet ({total_scored:,} rows, {buf.tell()/1e6:.1f} MB)"
    )
    del df_scores, buf

    duration = time.time() - start_time
    log_audit(
        pg_conn,
        dag_id,
        run_id,
        task_id,
        "ml",
        "success",
        rows_processed=total_scored,
        duration_seconds=round(duration, 2),
    )
    emit_lineage(
        "mlflow/aml_risk_model",
        "ml/scores",
        run_id,
        job_name,
        input_namespace="mlflow",
        output_namespace="minio",
        event_type="COMPLETE",
    )

    pg_conn.close()

    logger.info(
        f"Model registered & {total_scored:,} transactions scored in {duration:.1f}s"
    )
    context["ti"].xcom_push(key="model_version", value=mv.version)
    context["ti"].xcom_push(key="total_scored", value=total_scored)
    context["ti"].xcom_push(key="scores_path", value="gold/ml/scores.parquet")
    return mv.version


with DAG(
    dag_id="aml_ml_pipeline",
    default_args=default_args,
    description="AML ML — prepare → train → evaluate → register",
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["aml", "ml", "production"],
) as dag:

    t1 = PythonOperator(
        task_id="prepare_dataset",
        python_callable=prepare_dataset,
        execution_timeout=timedelta(minutes=30),
    )
    t2 = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
        execution_timeout=timedelta(hours=1),
    )
    t3 = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluate_model,
        execution_timeout=timedelta(minutes=30),
    )
    t4 = PythonOperator(
        task_id="register_model",
        python_callable=register_model,
        execution_timeout=timedelta(hours=1),
    )

    t1 >> t2 >> t3 >> t4
