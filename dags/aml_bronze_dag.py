import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "aml_pipeline",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "sla": timedelta(hours=1),
}


def emit_lineage(
    input_dataset,
    output_dataset,
    run_id,
    job_name,
    input_namespace="local",
    output_namespace="minio",
    event_type="COMPLETE",
):
    import uuid

    import requests
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

    try:
        resp = requests.post(MARQUEZ_URL + "/api/v1/lineage", json=event, timeout=5)
        if resp.status_code == 201:
            logger.info(f"Lineage [{event_type}]: {job_name} (run: {unique_run_id})")
        else:
            logger.warning(f"Lineage failed: {resp.status_code} — {resp.text}")
    except Exception as e:
        logger.warning(f"Lineage emit error (non-critical): {e}")


def log_audit(
    pg_conn,
    dag_id,
    run_id,
    task_id,
    layer,
    status,
    rows_processed=0,
    rows_dead_letter=0,
    duration_seconds=0,
    laundering_rate=0.0,
    error_message=None,
):
    cur = pg_conn.cursor()
    cur.execute(
        """
        INSERT INTO pipeline_audit_log
            (dag_id, run_id, task_id, layer, status, rows_processed,
             rows_dead_letter, duration_seconds, laundering_rate, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
        (
            dag_id,
            run_id,
            task_id,
            layer,
            status,
            rows_processed,
            rows_dead_letter,
            duration_seconds,
            laundering_rate,
            error_message,
        ),
    )
    pg_conn.commit()
    cur.close()


def upload_to_bronze(**context):
    import sys
    import time

    sys.path.insert(0, "/opt/airflow/dags")
    import io

    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from config import get_pg_conn, get_s3_client

    start_time = time.time()
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    task_id = context["task"].task_id
    job_name = "bronze.upload_to_bronze"

    s3 = get_s3_client()
    pg_conn = get_pg_conn()

    emit_lineage(
        "data/raw/LI-Small_Trans.csv",
        "bronze/ibm_aml",
        run_id,
        job_name,
        input_namespace="local",
        output_namespace="minio",
        event_type="START",
    )

    csv_path = "/opt/airflow/data/raw/LI-Small_Trans.csv"
    logger.info(f"Reading {csv_path}")

    chunksize = 500_000
    chunk_num = 0
    total_rows = 0
    partition_counts = {}

    try:
        for chunk in pd.read_csv(csv_path, chunksize=chunksize):
            chunk.columns = [
                "Timestamp",
                "From Bank",
                "Account_sender",
                "To Bank",
                "Account_receiver",
                "Amount Received",
                "Receiving Currency",
                "Amount Paid",
                "Payment Currency",
                "Payment Format",
                "Is Laundering",
            ]
            chunk["ingested_at"] = datetime.utcnow().isoformat()
            chunk["source"] = "ibm_aml"

            chunk["_ts"] = pd.to_datetime(chunk["Timestamp"])
            chunk["_year"] = chunk["_ts"].dt.year
            chunk["_month"] = chunk["_ts"].dt.month

            for (year, month), group in chunk.groupby(["_year", "_month"]):
                group = group.drop(columns=["_ts", "_year", "_month"])
                table = pa.Table.from_pandas(group, preserve_index=False)
                buf = io.BytesIO()
                pq.write_table(table, buf, compression="snappy")
                buf.seek(0)

                key = f"ibm_aml/year={year}/month={month:02d}/part-{chunk_num:04d}.parquet"
                s3.put_object(Bucket="bronze", Key=key, Body=buf.getvalue())

                partition_key = f"year={year}/month={month:02d}"
                partition_counts[partition_key] = partition_counts.get(
                    partition_key, 0
                ) + len(group)
                logger.info(f"Uploaded {key} ({len(group):,} rows)")

            total_rows += len(chunk)
            chunk_num += 1

        duration = time.time() - start_time
        log_audit(
            pg_conn,
            dag_id,
            run_id,
            task_id,
            "bronze",
            "success",
            rows_processed=total_rows,
            duration_seconds=round(duration, 2),
        )
        emit_lineage(
            "data/raw/LI-Small_Trans.csv",
            "bronze/ibm_aml",
            run_id,
            job_name,
            input_namespace="local",
            output_namespace="minio",
            event_type="COMPLETE",
        )

    except Exception as e:
        duration = time.time() - start_time
        log_audit(
            pg_conn,
            dag_id,
            run_id,
            task_id,
            "bronze",
            "failed",
            rows_processed=total_rows,
            duration_seconds=round(duration, 2),
            error_message=str(e),
        )
        emit_lineage(
            "data/raw/LI-Small_Trans.csv",
            "bronze/ibm_aml",
            run_id,
            job_name,
            input_namespace="local",
            output_namespace="minio",
            event_type="FAIL",
        )
        raise

    pg_conn.close()
    logger.info(f"Done. {total_rows:,} rows total in {duration:.1f}s")
    logger.info(f"Partitions: {partition_counts}")
    context["ti"].xcom_push(key="total_rows", value=total_rows)
    context["ti"].xcom_push(key="partitions", value=partition_counts)
    return total_rows


def validate_bronze(**context):
    import sys
    import time

    sys.path.insert(0, "/opt/airflow/dags")
    import io

    import pyarrow.parquet as pq
    from config import get_pg_conn, get_s3_client

    start_time = time.time()
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    task_id = context["task"].task_id
    job_name = "bronze.validate_bronze"

    s3 = get_s3_client()
    pg_conn = get_pg_conn()

    ti = context["ti"]
    total_rows = ti.xcom_pull(task_ids="upload_to_bronze", key="total_rows")
    partitions = ti.xcom_pull(task_ids="upload_to_bronze", key="partitions")

    emit_lineage(
        "bronze/ibm_aml",
        "bronze/ibm_aml",
        run_id,
        job_name,
        input_namespace="minio",
        output_namespace="minio",
        event_type="START",
    )

    objects = s3.list_objects_v2(Bucket="bronze", Prefix="ibm_aml/")
    files = [o["Key"] for o in objects.get("Contents", [])]
    logger.info(f"Found {len(files)} parquet files in bronze")

    try:
        if len(files) == 0:
            raise ValueError("No files found in bronze bucket!")

        obj = s3.get_object(Bucket="bronze", Key=files[0])
        table = pq.read_table(io.BytesIO(obj["Body"].read()))

        required_cols = [
            "Timestamp",
            "From Bank",
            "Account_sender",
            "To Bank",
            "Account_receiver",
            "Amount Paid",
            "Is Laundering",
            "source",
        ]
        missing = [c for c in required_cols if c not in table.schema.names]
        if missing:
            raise ValueError(f"Missing columns: {missing}")

        duration = time.time() - start_time
        log_audit(
            pg_conn,
            dag_id,
            run_id,
            task_id,
            "bronze",
            "success",
            rows_processed=total_rows,
            duration_seconds=round(duration, 2),
        )
        emit_lineage(
            "bronze/ibm_aml",
            "bronze/ibm_aml",
            run_id,
            job_name,
            input_namespace="minio",
            output_namespace="minio",
            event_type="COMPLETE",
        )

        logger.info(f"Schema valid: {table.schema.names}")
        logger.info(f"Total rows: {total_rows:,}")
        logger.info(f"Partitions: {partitions}")

    except Exception as e:
        duration = time.time() - start_time
        log_audit(
            pg_conn,
            dag_id,
            run_id,
            task_id,
            "bronze",
            "failed",
            rows_processed=total_rows,
            duration_seconds=round(duration, 2),
            error_message=str(e),
        )
        emit_lineage(
            "bronze/ibm_aml",
            "bronze/ibm_aml",
            run_id,
            job_name,
            input_namespace="minio",
            output_namespace="minio",
            event_type="FAIL",
        )
        raise

    pg_conn.close()
    context["ti"].xcom_push(key="bronze_files", value=len(files))
    return {"files": len(files), "total_rows": total_rows, "schema_valid": True}


def route_dead_letter(**context):
    import sys
    import time

    sys.path.insert(0, "/opt/airflow/dags")
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq
    from config import get_pg_conn, get_s3_client

    start_time = time.time()
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    task_id = context["task"].task_id
    job_name = "bronze.route_dead_letter"

    s3 = get_s3_client()
    pg_conn = get_pg_conn()

    emit_lineage(
        "bronze/ibm_aml",
        "dead-letter/ibm_aml",
        run_id,
        job_name,
        input_namespace="minio",
        output_namespace="minio",
        event_type="START",
    )

    objects = s3.list_objects_v2(Bucket="bronze", Prefix="ibm_aml/")
    files = [o["Key"] for o in objects.get("Contents", [])]

    dead_letter_rows = 0
    try:
        for key in files:
            obj = s3.get_object(Bucket="bronze", Key=key)
            table = pq.read_table(io.BytesIO(obj["Body"].read()))
            df = table.to_pandas()

            bad = df[
                df["Amount Paid"].isna()
                | (df["Amount Paid"] < 0)
                | df["Account_sender"].isna()
                | df["Account_receiver"].isna()
            ]

            if len(bad) > 0:
                dead_table = pa.Table.from_pandas(bad)
                buf = io.BytesIO()
                pq.write_table(dead_table, buf)
                buf.seek(0)
                dead_key = f"ibm_aml/{key.split('/')[-1]}"
                s3.put_object(Bucket="dead-letter", Key=dead_key, Body=buf.getvalue())
                dead_letter_rows += len(bad)
                logger.info(f"Routed {len(bad)} rows to dead-letter from {key}")

        duration = time.time() - start_time
        log_audit(
            pg_conn,
            dag_id,
            run_id,
            task_id,
            "bronze",
            "success",
            rows_dead_letter=dead_letter_rows,
            duration_seconds=round(duration, 2),
        )
        emit_lineage(
            "bronze/ibm_aml",
            "dead-letter/ibm_aml",
            run_id,
            job_name,
            input_namespace="minio",
            output_namespace="minio",
            event_type="COMPLETE",
        )

    except Exception as e:
        duration = time.time() - start_time
        log_audit(
            pg_conn,
            dag_id,
            run_id,
            task_id,
            "bronze",
            "failed",
            duration_seconds=round(duration, 2),
            error_message=str(e),
        )
        emit_lineage(
            "bronze/ibm_aml",
            "dead-letter/ibm_aml",
            run_id,
            job_name,
            input_namespace="minio",
            output_namespace="minio",
            event_type="FAIL",
        )
        raise

    pg_conn.close()
    logger.info(f"Total dead letter rows: {dead_letter_rows}")
    context["ti"].xcom_push(key="dead_letter_rows", value=dead_letter_rows)
    return dead_letter_rows


with DAG(
    dag_id="aml_bronze_ingestion",
    default_args=default_args,
    description="AML Bronze Layer — CSV to MinIO Parquet partitioned by transaction date",
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["aml", "bronze", "production"],
) as dag:

    t1 = PythonOperator(
        task_id="upload_to_bronze",
        python_callable=upload_to_bronze,
        execution_timeout=timedelta(minutes=10),
    )
    t2 = PythonOperator(
        task_id="validate_bronze",
        python_callable=validate_bronze,
        execution_timeout=timedelta(minutes=5),
    )
    t3 = PythonOperator(
        task_id="route_dead_letter",
        python_callable=route_dead_letter,
        execution_timeout=timedelta(minutes=15),
    )

    t1 >> t2 >> t3
