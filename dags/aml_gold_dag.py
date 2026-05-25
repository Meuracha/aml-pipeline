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
    "sla": timedelta(hours=2),
}


def emit_lineage(
    input_dataset,
    output_dataset,
    run_id,
    job_name,
    input_namespace="postgres",
    output_namespace="postgres",
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


def feature_engineering(**context):
    import io
    import os
    import sys
    import time

    sys.path.insert(0, "/opt/airflow/dags")
    import duckdb
    from config import get_pg_conn

    start_time = time.time()
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    task_id = context["task"].task_id
    job_name = "gold.feature_engineering"

    pg_host = os.getenv("POSTGRES_HOST", "postgres")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB", "aml_db")
    pg_user = os.getenv("POSTGRES_USER", "")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "")
    pg_dsn = (
        f"host={pg_host} port={pg_port} dbname={pg_db} "
        f"user={pg_user} password={pg_pass}"
    )

    pg_conn = get_pg_conn()
    emit_lineage(
        "transactions_silver",
        "transactions_gold_temp",
        run_id,
        job_name,
        event_type="START",
    )

    cur = pg_conn.cursor()
    cur.execute("DROP TABLE IF EXISTS transactions_gold_temp")
    pg_conn.commit()
    cur.close()
    logger.info("Dropped old temp table")

    total_written = 0
    duration = 0

    try:
        logger.info("Connecting DuckDB to PostgreSQL...")
        duck = duckdb.connect()
        duck.execute("INSTALL postgres; LOAD postgres;")
        duck.execute(f"ATTACH '{pg_dsn}' AS pg (TYPE postgres)")
        logger.info("DuckDB connected to PostgreSQL")

        logger.info("Computing features in DuckDB...")
        duck.execute("""
            CREATE TABLE gold_features AS
            SELECT
                t.transaction_id,
                t.timestamp,
                t.sender_account_masked,
                t.receiver_account_masked,
                t.sender_bank,
                t.receiver_bank,
                t.amount,
                t.amount_log,
                t.payment_currency,
                t.receiving_currency,
                t.payment_type,
                t.is_laundering,
                t.typology,
                t.source,
                t.tx_hour,
                t.tx_day_of_week,
                t.is_weekend,
                t.is_cross_currency,
                t.ingested_at,
                COUNT(*) OVER (
                    PARTITION BY t.sender_account_masked
                    ORDER BY t.timestamp
                    RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
                ) AS sender_tx_count_1h,
                SUM(t.amount) OVER (
                    PARTITION BY t.sender_account_masked
                    ORDER BY t.timestamp
                    RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
                ) AS sender_amount_sum_1h,
                AVG(t.amount) OVER (
                    PARTITION BY t.sender_account_masked
                ) AS sender_avg_amount,
                CASE t.payment_type
                    WHEN 'ACH'          THEN 1.000
                    WHEN 'Bitcoin'      THEN 0.109
                    WHEN 'Cash'         THEN 0.058
                    WHEN 'Cheque'       THEN 0.056
                    WHEN 'Credit Card'  THEN 0.045
                    WHEN 'Wire'         THEN 0.000
                    WHEN 'Reinvestment' THEN 0.000
                    ELSE 0.050
                END AS payment_type_risk,
                CASE WHEN t.payment_type IN ('ACH', 'Bitcoin')
                    THEN 1 ELSE 0
                END AS is_high_risk_type,
                CASE WHEN t.amount >= 9000 AND t.amount < 10000
                    THEN 1 ELSE 0
                END AS is_structuring,
                CASE WHEN CAST(t.amount AS BIGINT) % 1000 = 0 AND t.amount > 0
                    THEN 1 ELSE 0
                END AS is_round_amount
            FROM pg.transactions_silver t
        """)
        logger.info("Base features computed")

        duck.execute("ALTER TABLE gold_features ADD COLUMN amount_vs_sender_avg DOUBLE")
        duck.execute("ALTER TABLE gold_features ADD COLUMN rule_score DOUBLE")
        duck.execute("""
            UPDATE gold_features SET
                amount_vs_sender_avg = LEAST(
                    amount / GREATEST(sender_avg_amount, 0.01), 1000.0
                ),
                rule_score = LEAST(
                    payment_type_risk  * 0.30
                    + is_cross_currency * 0.20
                    + is_structuring    * 0.25
                    + is_round_amount   * 0.10
                    + is_high_risk_type * 0.15,
                    1.0
                )
        """)
        logger.info("Derived features computed")

        cur = pg_conn.cursor()
        cur.execute("""
            CREATE TABLE transactions_gold_temp (
                transaction_id        TEXT,
                timestamp             TIMESTAMP,
                sender_account_masked TEXT,
                receiver_account_masked TEXT,
                sender_bank           TEXT,
                receiver_bank         TEXT,
                amount                DOUBLE PRECISION,
                amount_log            DOUBLE PRECISION,
                payment_currency      TEXT,
                receiving_currency    TEXT,
                payment_type          TEXT,
                is_laundering         BIGINT,
                typology              TEXT,
                source                TEXT,
                tx_hour               BIGINT,
                tx_day_of_week        BIGINT,
                is_weekend            BIGINT,
                is_cross_currency     BIGINT,
                ingested_at           TEXT,
                sender_tx_count_1h    BIGINT,
                sender_amount_sum_1h  DOUBLE PRECISION,
                sender_avg_amount     DOUBLE PRECISION,
                payment_type_risk     DOUBLE PRECISION,
                is_high_risk_type     BIGINT,
                is_structuring        BIGINT,
                is_round_amount       BIGINT,
                amount_vs_sender_avg  DOUBLE PRECISION,
                rule_score            DOUBLE PRECISION
            )
        """)
        pg_conn.commit()
        cur.close()

        gold_cols = [
            "transaction_id",
            "timestamp",
            "sender_account_masked",
            "receiver_account_masked",
            "sender_bank",
            "receiver_bank",
            "amount",
            "amount_log",
            "payment_currency",
            "receiving_currency",
            "payment_type",
            "is_laundering",
            "typology",
            "source",
            "tx_hour",
            "tx_day_of_week",
            "is_weekend",
            "is_cross_currency",
            "ingested_at",
            "sender_tx_count_1h",
            "sender_amount_sum_1h",
            "sender_avg_amount",
            "payment_type_risk",
            "is_high_risk_type",
            "is_structuring",
            "is_round_amount",
            "amount_vs_sender_avg",
            "rule_score",
        ]

        WRITE_BATCH = 100_000
        offset = 0

        while True:
            sql = (
                f"SELECT {', '.join(gold_cols)} FROM gold_features "  # nosec B608
                f"LIMIT {WRITE_BATCH} OFFSET {offset}"
            )
            df_batch = duck.execute(sql).df()

            if df_batch.empty:
                break

            cur = pg_conn.cursor()
            buf = io.StringIO()
            df_batch.to_csv(buf, index=False, header=False, na_rep="\\N")
            buf.seek(0)
            cols_str = ", ".join(f'"{c}"' for c in gold_cols)
            cur.copy_expert(
                f"COPY transactions_gold_temp ({cols_str}) FROM STDIN WITH CSV NULL '\\N'",
                buf,
            )
            pg_conn.commit()
            cur.close()

            total_written += len(df_batch)
            offset += WRITE_BATCH
            logger.info(f"Written {total_written:,} rows to PostgreSQL...")
            del df_batch

        duck.close()
        logger.info(f"Total written: {total_written:,} rows")

        duration = time.time() - start_time
        log_audit(
            pg_conn,
            dag_id,
            run_id,
            task_id,
            "gold",
            "success",
            rows_processed=total_written,
            duration_seconds=round(duration, 2),
        )
        emit_lineage(
            "transactions_silver",
            "transactions_gold_temp",
            run_id,
            job_name,
            event_type="COMPLETE",
        )

    except Exception as err:
        duration = time.time() - start_time
        log_audit(
            pg_conn,
            dag_id,
            run_id,
            task_id,
            "gold",
            "failed",
            duration_seconds=round(duration, 2),
            error_message=str(err),
        )
        emit_lineage(
            "transactions_silver",
            "transactions_gold_temp",
            run_id,
            job_name,
            event_type="FAIL",
        )
        pg_conn.close()
        raise

    pg_conn.close()
    context["ti"].xcom_push(key="gold_rows", value=total_written)
    logger.info(f"Feature engineering done. {total_written:,} rows in {duration:.1f}s")
    return total_written


def validate_gold(**context):
    import sys
    import time

    sys.path.insert(0, "/opt/airflow/dags")
    from config import get_pg_conn

    start_time = time.time()
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    task_id = context["task"].task_id
    job_name = "gold.validate_gold"

    pg_conn = get_pg_conn()
    cur = pg_conn.cursor()

    emit_lineage(
        "transactions_gold_temp",
        "transactions_gold_temp",
        run_id,
        job_name,
        event_type="START",
    )

    cur.execute("SELECT COUNT(*) FROM transactions_gold_temp")
    total = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM transactions_gold_temp WHERE transaction_id IS NULL"
    )
    nulls = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM transactions_gold_temp WHERE rule_score IS NULL")
    null_score = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM transactions_gold_temp WHERE rule_score < 0 OR rule_score > 1"
    )
    invalid_score = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM transactions_gold_temp WHERE sender_tx_count_1h IS NULL"
    )
    null_tx_count = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM transactions_gold_temp WHERE amount_vs_sender_avg IS NULL"
    )
    null_avg_ratio = cur.fetchone()[0]
    cur.execute("""
        SELECT AVG(rule_score), MAX(rule_score), MIN(rule_score),
               AVG(sender_tx_count_1h), MAX(sender_tx_count_1h)
        FROM transactions_gold_temp
    """)
    avg_score, max_score, min_score, avg_tx_1h, max_tx_1h = cur.fetchone()
    cur.execute("SELECT COUNT(*) FROM transactions_gold_temp WHERE is_laundering = 1")
    laundering = cur.fetchone()[0]

    rate = laundering / total * 100 if total > 0 else 0
    duration = time.time() - start_time

    logger.info(
        f"Gold validation: {total:,} rows, laundering: {laundering:,} ({rate:.4f}%)"
    )
    logger.info(
        f"  avg rule_score: {avg_score:.4f}, max: {max_score:.4f}, min: {min_score:.4f}"
    )
    logger.info(f"  avg sender_tx_1h: {avg_tx_1h:.2f}, max: {max_tx_1h}")

    errors = []
    if nulls > 0:
        errors.append(f"null transaction_id: {nulls}")
    if null_score > 0:
        errors.append(f"null rule_score: {null_score}")
    if invalid_score > 0:
        errors.append(f"rule_score out of range: {invalid_score}")
    if null_tx_count > 0:
        errors.append(f"null sender_tx_count_1h: {null_tx_count}")
    if null_avg_ratio > 0:
        errors.append(f"null amount_vs_sender_avg: {null_avg_ratio}")
    if total < 6_000_000:
        errors.append(f"row count too low: {total:,}")

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
            "gold",
            "failed" if errors else "success",
            total,
            round(duration, 2),
            rate,
            str(errors) if errors else None,
        ),
    )
    pg_conn.commit()
    cur.close()

    if errors:
        emit_lineage(
            "transactions_gold_temp",
            "transactions_gold_temp",
            run_id,
            job_name,
            event_type="FAIL",
        )
        pg_conn.close()
        raise ValueError(f"Gold validation failed: {errors}")

    emit_lineage(
        "transactions_gold_temp",
        "transactions_gold_temp",
        run_id,
        job_name,
        event_type="COMPLETE",
    )
    pg_conn.close()
    logger.info("Gold validation passed!")
    context["ti"].xcom_push(key="validated_rows", value=total)
    context["ti"].xcom_push(key="laundering_count", value=laundering)
    context["ti"].xcom_push(key="avg_rule_score", value=float(avg_score))
    return total


def promote_to_gold(**context):
    import io
    import sys
    import time
    import time as time_module

    sys.path.insert(0, "/opt/airflow/dags")
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from config import get_pg_conn, get_s3_client

    start_time = time.time()
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    task_id = context["task"].task_id
    job_name = "gold.promote_to_gold"

    pg_conn = get_pg_conn()
    cur = pg_conn.cursor()

    emit_lineage(
        "transactions_gold_temp",
        "transactions_featured",
        run_id,
        job_name,
        event_type="START",
    )

    cur.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'transactions_gold_temp') THEN
                DROP TABLE IF EXISTS transactions_featured;
                ALTER TABLE transactions_gold_temp RENAME TO transactions_featured;
            ELSIF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'transactions_featured') THEN
                RAISE NOTICE 'transactions_featured already exists, skipping rename';
            ELSE
                RAISE EXCEPTION 'Neither transactions_gold_temp nor transactions_featured exists';
            END IF;
        END $$;
    """)
    pg_conn.commit()
    logger.info("transactions_featured ready")

    s3_client = get_s3_client()
    gold_key = "ibm_aml/year=2022/month=09/transactions.parquet"
    BATCH_SIZE = 100_000
    batch_cur = pg_conn.cursor("gold_minio_cursor")
    batch_cur.execute("SELECT * FROM transactions_featured")

    first_batch = batch_cur.fetchmany(BATCH_SIZE)
    total_written = 0

    if first_batch:
        minio_cols = [desc[0] for desc in batch_cur.description]
        df_first = pd.DataFrame(first_batch, columns=minio_cols)
        table_first = pa.Table.from_pandas(df_first, preserve_index=False)
        schema = table_first.schema

        mpu = s3_client.create_multipart_upload(Bucket="gold", Key=gold_key)
        upload_id = mpu["UploadId"]
        parts = []
        part_number = 1
        part_buf = io.BytesIO()
        part_writer = pq.ParquetWriter(part_buf, schema, compression="snappy")
        PART_SIZE_ROWS = 1_000_000

        try:
            part_writer.write_table(table_first)
            total_written += len(df_first)
            del df_first, table_first

            while True:
                rows = batch_cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                df = pd.DataFrame(rows, columns=minio_cols)
                part_writer.write_table(pa.Table.from_pandas(df, preserve_index=False))
                total_written += len(df)
                del df

                if total_written % PART_SIZE_ROWS == 0:
                    part_writer.close()
                    part_buf.seek(0)
                    data = part_buf.read()
                    part = s3_client.upload_part(
                        Bucket="gold",
                        Key=gold_key,
                        UploadId=upload_id,
                        PartNumber=part_number,
                        Body=data,
                    )
                    parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                    part_number += 1
                    part_buf = io.BytesIO()
                    part_writer = pq.ParquetWriter(
                        part_buf, schema, compression="snappy"
                    )

            batch_cur.close()
            part_writer.close()
            part_buf.seek(0)
            data = part_buf.read()
            if data:
                part = s3_client.upload_part(
                    Bucket="gold",
                    Key=gold_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=data,
                )
                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})

            s3_client.complete_multipart_upload(
                Bucket="gold",
                Key=gold_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            logger.info(f"Written: gold/{gold_key} ({total_written:,} rows)")

        except Exception:
            s3_client.abort_multipart_upload(
                Bucket="gold", Key=gold_key, UploadId=upload_id
            )
            raise

    pg_conn.commit()
    pg_conn.autocommit = True
    cur2 = pg_conn.cursor()
    cur2.execute("SET maintenance_work_mem = '256MB'")
    cur2.execute("SET max_parallel_maintenance_workers = 1")

    indexes = [
        (
            "idx_gold_transaction_id",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gold_transaction_id ON transactions_featured(transaction_id)",
        ),
        (
            "idx_gold_timestamp",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gold_timestamp ON transactions_featured(timestamp)",
        ),
        (
            "idx_gold_is_laundering",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gold_is_laundering ON transactions_featured(is_laundering)",
        ),
        (
            "idx_gold_rule_score",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gold_rule_score ON transactions_featured(rule_score)",
        ),
        (
            "idx_gold_sender",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gold_sender ON transactions_featured(sender_account_masked)",
        ),
        (
            "idx_gold_payment_type",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gold_payment_type ON transactions_featured(payment_type)",
        ),
    ]

    duration = 0
    try:
        for name, sql in indexes:
            logger.info(f"Creating index: {name}...")
            cur2.execute(sql)
            logger.info(f"Created: {name}")
            time_module.sleep(10)

        duration = time.time() - start_time
        emit_lineage(
            "transactions_gold_temp",
            "transactions_featured",
            run_id,
            job_name,
            event_type="COMPLETE",
        )
        pg_conn.autocommit = False
        log_audit(
            pg_conn,
            dag_id,
            run_id,
            task_id,
            "gold",
            "success",
            duration_seconds=round(duration, 2),
        )

    except Exception as err:
        duration = time.time() - start_time
        emit_lineage(
            "transactions_gold_temp",
            "transactions_featured",
            run_id,
            job_name,
            event_type="FAIL",
        )
        pg_conn.autocommit = False
        log_audit(
            pg_conn,
            dag_id,
            run_id,
            task_id,
            "gold",
            "failed",
            duration_seconds=round(duration, 2),
            error_message=str(err),
        )
        raise

    cur2.close()
    cur.close()
    pg_conn.close()
    total_rows = context["ti"].xcom_pull(task_ids="validate_gold", key="validated_rows")
    logger.info(f"Promote gold complete: {total_rows:,} rows in {duration:.1f}s")
    return total_rows


def generate_alerts(**context):
    import sys
    import time

    sys.path.insert(0, "/opt/airflow/dags")
    from config import get_pg_conn

    start_time = time.time()
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    task_id = context["task"].task_id
    job_name = "gold.generate_alerts"

    pg_conn = get_pg_conn()
    cur = pg_conn.cursor()

    emit_lineage(
        "transactions_featured", "aml_alerts", run_id, job_name, event_type="START"
    )

    cur.execute("""
        SELECT PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY rule_score)
        FROM transactions_featured
    """)
    threshold = float(cur.fetchone()[0])
    logger.info(f"Alert threshold (99th percentile): {threshold:.4f}")

    cur.execute("DELETE FROM aml_alerts")
    pg_conn.commit()

    cur.execute(f"""
        INSERT INTO aml_alerts (alert_id, transaction_id, risk_score, typology, status)
        SELECT
            md5(transaction_id || NOW()::text),
            transaction_id,
            rule_score,
            CASE
                WHEN is_structuring = 1 AND sender_tx_count_1h > 5 THEN 'structuring_rapid'
                WHEN is_structuring = 1 THEN 'structuring'
                WHEN is_high_risk_type = 1 AND is_cross_currency = 1 THEN 'cross_currency_high_risk'
                WHEN sender_tx_count_1h > 10 THEN 'rapid_movement'
                WHEN amount_vs_sender_avg > 10 THEN 'unusual_amount'
                WHEN is_round_amount = 1 AND amount > 50000 THEN 'large_round_amount'
                ELSE 'suspicious_pattern'
            END,
            'OPEN'
        FROM transactions_featured
        WHERE rule_score >= {threshold}
        ORDER BY rule_score DESC
        """)  # nosec B608
    pg_conn.commit()

    cur.execute("SELECT COUNT(*) FROM aml_alerts")
    alert_count = cur.fetchone()[0]

    cur.execute("""
        SELECT typology, COUNT(*), ROUND(AVG(risk_score)::numeric, 4)
        FROM aml_alerts GROUP BY typology ORDER BY COUNT(*) DESC
    """)
    logger.info(f"Generated {alert_count:,} alerts (threshold: {threshold:.4f})")
    for typology, count, avg_score in cur.fetchall():
        logger.info(f"  {typology}: {count:,} (avg score: {avg_score})")

    duration = time.time() - start_time
    log_audit(
        pg_conn,
        dag_id,
        run_id,
        task_id,
        "gold",
        "success",
        rows_processed=alert_count,
        duration_seconds=round(duration, 2),
    )
    emit_lineage(
        "transactions_featured", "aml_alerts", run_id, job_name, event_type="COMPLETE"
    )

    cur.close()
    pg_conn.close()
    context["ti"].xcom_push(key="alert_count", value=alert_count)
    context["ti"].xcom_push(key="threshold", value=threshold)
    logger.info(f"Alert generation done. {alert_count:,} alerts in {duration:.1f}s")
    return alert_count


with DAG(
    dag_id="aml_gold_transform",
    default_args=default_args,
    description="AML Gold — DuckDB feature engineering → validate → promote → alerts",
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["aml", "gold", "production"],
) as dag:
    t1 = PythonOperator(
        task_id="feature_engineering",
        python_callable=feature_engineering,
        execution_timeout=timedelta(hours=1),
    )
    t2 = PythonOperator(
        task_id="validate_gold",
        python_callable=validate_gold,
        execution_timeout=timedelta(minutes=15),
    )
    t3 = PythonOperator(
        task_id="promote_to_gold",
        python_callable=promote_to_gold,
        execution_timeout=timedelta(minutes=60),
    )
    t4 = PythonOperator(
        task_id="generate_alerts",
        python_callable=generate_alerts,
        execution_timeout=timedelta(minutes=10),
    )
    t1 >> t2 >> t3 >> t4
