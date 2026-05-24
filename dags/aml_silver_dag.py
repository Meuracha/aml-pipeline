from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'aml_pipeline',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'sla': timedelta(hours=2),
}

CURRENCY_MAP = {
    "US Dollar": "USD", "Euro": "EUR", "Bitcoin": "BTC",
    "Australian Dollar": "AUD", "Yuan": "CNY", "Rupee": "INR",
    "Ruble": "RUB", "UK Pound": "GBP", "Canadian Dollar": "CAD",
    "Swiss Franc": "CHF", "Brazilian Real": "BRL", "Mexico Peso": "MXN",
    "Shekel": "ILS", "Saudi Riyal": "SAR", "Yen": "JPY",
}


def emit_lineage(input_dataset, output_dataset, run_id, job_name,
                 input_namespace='minio', output_namespace='postgres',
                 event_type='COMPLETE'):
    import requests, uuid
    from config import MARQUEZ_URL

    unique_run_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, run_id + job_name))

    event = {
        "eventType": event_type,
        "eventTime": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z'),
        "run": {"runId": unique_run_id},
        "job": {
            "namespace": "aml_pipeline",
            "name": job_name
        },
        "inputs": [{"namespace": input_namespace, "name": input_dataset}],
        "outputs": [{"namespace": output_namespace, "name": output_dataset}],
        "producer": "aml_pipeline/airflow"
    }

    try:
        resp = requests.post(
            MARQUEZ_URL + "/api/v1/lineage",
            json=event,
            timeout=5
        )
        if resp.status_code == 201:
            logger.info(f"Lineage [{event_type}]: {job_name} (run: {unique_run_id})")
        else:
            logger.warning(f"Lineage failed: {resp.status_code} — {resp.text}")
    except Exception as e:
        logger.warning(f"Lineage emit error (non-critical): {e}")


def log_audit(pg_conn, dag_id, run_id, task_id, layer, status,
              rows_processed=0, rows_dead_letter=0, duration_seconds=0,
              laundering_rate=0.0, error_message=None):
    cur = pg_conn.cursor()
    cur.execute("""
        INSERT INTO pipeline_audit_log
            (dag_id, run_id, task_id, layer, status, rows_processed,
             rows_dead_letter, duration_seconds, laundering_rate, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (dag_id, run_id, task_id, layer, status, rows_processed,
          rows_dead_letter, duration_seconds, laundering_rate, error_message))
    pg_conn.commit()
    cur.close()


def transform_chunk(df):
    import pandas as pd
    import numpy as np
    import hashlib

    df = df.copy()
    keys = (
        'ibm_aml_' +
        df['Timestamp'].astype(str) + '_' +
        df['Account_sender'].astype(str) + '_' +
        df['Account_receiver'].astype(str) + '_' +
        df['Amount Paid'].astype(str) + '_' +
        df['Payment Format'].astype(str)
    )
    df['transaction_id'] = keys.map(lambda x: hashlib.md5(x.encode()).hexdigest())
    df['timestamp'] = pd.to_datetime(df['Timestamp'])
    df['sender_account_masked'] = '****' + df['Account_sender'].astype(str).str[-4:]
    df['receiver_account_masked'] = '****' + df['Account_receiver'].astype(str).str[-4:]
    df['sender_bank'] = df['From Bank'].astype(str)
    df['receiver_bank'] = df['To Bank'].astype(str)
    df['amount'] = df['Amount Paid'].astype(float)
    df['payment_currency'] = df['Payment Currency'].map(CURRENCY_MAP).fillna(df['Payment Currency'])
    df['receiving_currency'] = df['Receiving Currency'].map(CURRENCY_MAP).fillna(df['Receiving Currency'])
    df['payment_type'] = df['Payment Format']
    df['is_laundering'] = df['Is Laundering'].astype(int)
    df['typology'] = None
    df['source'] = 'ibm_aml'
    df['tx_hour'] = df['timestamp'].dt.hour
    df['tx_day_of_week'] = df['timestamp'].dt.dayofweek
    df['is_weekend'] = df['tx_day_of_week'].isin([5, 6]).astype(int)
    df['is_cross_currency'] = (df['payment_currency'] != df['receiving_currency']).astype(int)
    df['amount_log'] = np.log1p(df['amount'].clip(lower=0))

    return df[[
        'transaction_id', 'timestamp',
        'sender_account_masked', 'receiver_account_masked',
        'sender_bank', 'receiver_bank',
        'amount', 'amount_log',
        'payment_currency', 'receiving_currency',
        'payment_type', 'is_laundering', 'typology', 'source',
        'tx_hour', 'tx_day_of_week', 'is_weekend', 'is_cross_currency',
        'ingested_at',
    ]]


def fast_copy(df, table_name, pg_conn, if_exists='append'):
    import io
    cur = pg_conn.cursor()

    if if_exists == 'replace':
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        pg_conn.commit()
        col_defs = []
        for col, dtype in df.dtypes.items():
            if 'int' in str(dtype):
                pg_type = 'BIGINT'
            elif 'float' in str(dtype):
                pg_type = 'DOUBLE PRECISION'
            elif 'datetime' in str(dtype):
                pg_type = 'TIMESTAMP'
            else:
                pg_type = 'TEXT'
            col_defs.append(f'"{col}" {pg_type}')
        cur.execute(f"CREATE TABLE {table_name} ({', '.join(col_defs)})")
        pg_conn.commit()

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cols = ', '.join(f'"{c}"' for c in df.columns)
    cur.copy_expert(f"COPY {table_name} ({cols}) FROM STDIN WITH CSV NULL '\\N'", buf)
    pg_conn.commit()
    cur.close()


def read_transform_load(**context):
    import sys, time
    sys.path.insert(0, '/opt/airflow/dags')
    from config import get_s3_client, get_pg_conn
    import pyarrow.parquet as pq
    import io

    start_time = time.time()
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    task_id = context['task'].task_id
    job_name = 'silver.read_transform_load'

    s3 = get_s3_client()
    pg_conn = get_pg_conn()

    emit_lineage('bronze/ibm_aml', 'transactions_silver_temp', run_id, job_name,
                 input_namespace='minio', output_namespace='postgres', event_type='START')

    cur = pg_conn.cursor()
    cur.execute("DROP TABLE IF EXISTS transactions_silver_temp")
    cur.execute("DROP TABLE IF EXISTS transactions_silver_dead")
    pg_conn.commit()
    cur.close()

    objects = s3.list_objects_v2(Bucket='bronze', Prefix='ibm_aml/')
    files = [o['Key'] for o in objects.get('Contents', [])]
    logger.info(f"Processing {len(files)} bronze files")

    total_rows = 0
    dead_rows = 0
    seen_ids = set()

    try:
        for i, key in enumerate(files):
            obj = s3.get_object(Bucket='bronze', Key=key)
            df = pq.read_table(io.BytesIO(obj['Body'].read())).to_pandas()
            df_t = transform_chunk(df)

            intra_dupes = df_t.duplicated(subset=['transaction_id'], keep='first')
            df_intra_dead = df_t[intra_dupes].copy()
            df_t = df_t[~intra_dupes].copy()

            if len(df_intra_dead) > 0:
                df_intra_dead['dead_reason'] = 'intra_chunk_duplicate'
                fast_copy(df_intra_dead, 'transactions_silver_dead', pg_conn,
                    if_exists='replace' if dead_rows == 0 else 'append')
                dead_rows += len(df_intra_dead)
                logger.warning(f"Routed {len(df_intra_dead)} intra-chunk duplicates to dead letter")

            inter_dupes_mask = df_t['transaction_id'].isin(seen_ids)
            df_inter_dead = df_t[inter_dupes_mask].copy()
            df_t = df_t[~inter_dupes_mask].copy()

            if len(df_inter_dead) > 0:
                df_inter_dead['dead_reason'] = 'inter_chunk_duplicate'
                fast_copy(df_inter_dead, 'transactions_silver_dead', pg_conn,
                    if_exists='replace' if dead_rows == 0 else 'append')
                dead_rows += len(df_inter_dead)
                logger.warning(f"Routed {len(df_inter_dead)} inter-chunk duplicates to dead letter")

            seen_ids.update(df_t['transaction_id'].tolist())
            fast_copy(df_t, 'transactions_silver_temp', pg_conn,
                if_exists='replace' if i == 0 else 'append')

            total_rows += len(df_t)
            logger.info(f"[{i+1}/{len(files)}] {key}: {len(df_t):,} rows (total: {total_rows:,}, dead: {dead_rows})")
            del df, df_t

        duration = time.time() - start_time
        log_audit(pg_conn, dag_id, run_id, task_id, 'silver', 'success',
                  rows_processed=total_rows, rows_dead_letter=dead_rows,
                  duration_seconds=round(duration, 2))
        emit_lineage('bronze/ibm_aml', 'transactions_silver_temp', run_id, job_name,
                     input_namespace='minio', output_namespace='postgres', event_type='COMPLETE')

    except Exception as e:
        duration = time.time() - start_time
        log_audit(pg_conn, dag_id, run_id, task_id, 'silver', 'failed',
                  rows_processed=total_rows, duration_seconds=round(duration, 2),
                  error_message=str(e))
        emit_lineage('bronze/ibm_aml', 'transactions_silver_temp', run_id, job_name,
                     input_namespace='minio', output_namespace='postgres', event_type='FAIL')
        raise

    pg_conn.close()
    context['ti'].xcom_push(key='total_rows', value=total_rows)
    context['ti'].xcom_push(key='dead_rows', value=dead_rows)
    logger.info(f"Staging done. {total_rows:,} rows in temp, {dead_rows} dead, {duration:.1f}s")
    return total_rows


def validate_staging(**context):
    import sys, time
    sys.path.insert(0, '/opt/airflow/dags')
    from config import get_pg_conn

    start_time = time.time()
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    task_id = context['task'].task_id
    job_name = 'silver.validate_staging'

    pg_conn = get_pg_conn()

    emit_lineage('transactions_silver_temp', 'transactions_silver_temp', run_id, job_name,
                 input_namespace='postgres', output_namespace='postgres', event_type='START')

    cur = pg_conn.cursor()

    cur.execute("SELECT COUNT(*) FROM transactions_silver_temp")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM transactions_silver_temp WHERE transaction_id IS NULL")
    nulls = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM transactions_silver_temp WHERE amount < 0")
    neg_amount = cur.fetchone()[0]
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT transaction_id FROM transactions_silver_temp
            GROUP BY transaction_id HAVING COUNT(*) > 1
        ) t
    """)
    dupes = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM transactions_silver_temp WHERE is_laundering = 1")
    laundering = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM transactions_silver_temp WHERE is_laundering NOT IN (0, 1)")
    invalid_label = cur.fetchone()[0]

    dead_rows = context['ti'].xcom_pull(task_ids='read_transform_load', key='dead_rows') or 0
    rate = laundering / total * 100 if total > 0 else 0
    duration = time.time() - start_time

    logger.info(f"Staging validation:")
    logger.info(f"  total rows      : {total:,}")
    logger.info(f"  laundering rate : {rate:.4f}%")
    logger.info(f"  null tx_id      : {nulls}")
    logger.info(f"  negative amount : {neg_amount}")
    logger.info(f"  duplicate tx_id : {dupes}")
    logger.info(f"  invalid label   : {invalid_label}")
    logger.info(f"  dead letter rows: {dead_rows}")

    errors = []
    if nulls > 0:
        errors.append(f"null transaction_id: {nulls}")
    if neg_amount > 0:
        errors.append(f"negative amount: {neg_amount}")
    if dupes > 0:
        errors.append(f"duplicate transaction_id: {dupes}")
    if invalid_label > 0:
        errors.append(f"invalid is_laundering label: {invalid_label}")
    if total < 6_000_000:
        errors.append(f"row count too low: {total:,} (expected ~6.9M)")

    cur.execute("""
        INSERT INTO pipeline_audit_log
            (dag_id, run_id, task_id, layer, status, rows_processed,
             rows_dead_letter, duration_seconds, laundering_rate, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (dag_id, run_id, task_id, 'silver',
          'failed' if errors else 'success',
          total, dead_rows, round(duration, 2), rate,
          str(errors) if errors else None))
    pg_conn.commit()
    cur.close()

    if errors:
        emit_lineage('transactions_silver_temp', 'transactions_silver_temp', run_id, job_name,
                     input_namespace='postgres', output_namespace='postgres', event_type='FAIL')
        pg_conn.close()
        raise ValueError(f"Validation failed: {errors}")

    emit_lineage('transactions_silver_temp', 'transactions_silver_temp', run_id, job_name,
                 input_namespace='postgres', output_namespace='postgres', event_type='COMPLETE')
    pg_conn.close()

    logger.info("Validation passed!")
    context['ti'].xcom_push(key='validated_rows', value=total)
    context['ti'].xcom_push(key='laundering_rate', value=rate)
    return total


def promote_to_silver(**context):
    import sys, io, time
    sys.path.insert(0, '/opt/airflow/dags')
    from config import get_s3_client, get_pg_conn
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    start_time = time.time()
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    task_id = context['task'].task_id
    job_name = 'silver.promote_to_silver'

    pg_conn = get_pg_conn()
    cur = pg_conn.cursor()

    emit_lineage('transactions_silver_temp', 'silver/ibm_aml', run_id, job_name,
                 input_namespace='postgres', output_namespace='minio', event_type='START')

    cur.execute("DROP TABLE IF EXISTS transactions_silver")
    cur.execute("ALTER TABLE transactions_silver_temp RENAME TO transactions_silver")
    pg_conn.commit()
    logger.info("Renamed temp → transactions_silver")

    s3 = get_s3_client()

    cur.execute("SELECT DISTINCT source FROM transactions_silver")
    sources = [r[0] for r in cur.fetchall()]

    cur.execute("""
        SELECT DISTINCT
            EXTRACT(YEAR FROM timestamp)::int,
            EXTRACT(MONTH FROM timestamp)::int
        FROM transactions_silver ORDER BY 1, 2
    """)
    partitions = cur.fetchall()

    BATCH_SIZE = 100_000
    total_written = 0

    try:
        for source in sources:
            for year, month in partitions:
                logger.info(f"Writing MinIO: {source}/year={year}/month={month:02d}")

                batch_cur = pg_conn.cursor(f'cursor_{source}_{year}_{month}')
                batch_cur.execute("""
                    SELECT * FROM transactions_silver
                    WHERE source = %s
                    AND EXTRACT(YEAR FROM timestamp)::int = %s
                    AND EXTRACT(MONTH FROM timestamp)::int = %s
                """, (source, year, month))

                first_batch = batch_cur.fetchmany(BATCH_SIZE)
                if not first_batch:
                    batch_cur.close()
                    continue

                cols = [desc[0] for desc in batch_cur.description]
                buf = io.BytesIO()
                writer = None
                partition_rows = 0

                df = pd.DataFrame(first_batch, columns=cols)
                table = pa.Table.from_pandas(df, preserve_index=False)
                writer = pq.ParquetWriter(buf, table.schema, compression='snappy')
                writer.write_table(table)
                partition_rows += len(df)
                logger.info(f"  buffered {partition_rows:,} rows...")
                del df, table

                while True:
                    rows = batch_cur.fetchmany(BATCH_SIZE)
                    if not rows:
                        break
                    df = pd.DataFrame(rows, columns=cols)
                    table = pa.Table.from_pandas(df, preserve_index=False)
                    writer.write_table(table)
                    partition_rows += len(df)
                    logger.info(f"  buffered {partition_rows:,} rows...")
                    del df, table

                batch_cur.close()
                writer.close()
                buf.seek(0)

                silver_key = f"{source}/year={year}/month={month:02d}/transactions.parquet"
                s3.put_object(Bucket='silver', Key=silver_key, Body=buf.getvalue())
                total_written += partition_rows
                logger.info(f"Written: silver/{silver_key} ({partition_rows:,} rows, {buf.tell()/1e6:.1f} MB)")

        duration = time.time() - start_time
        emit_lineage('transactions_silver_temp', 'silver/ibm_aml', run_id, job_name,
                     input_namespace='postgres', output_namespace='minio', event_type='COMPLETE')
        log_audit(pg_conn, dag_id, run_id, task_id, 'silver', 'success',
                  rows_processed=total_written, duration_seconds=round(duration, 2))

    except Exception as e:
        duration = time.time() - start_time
        emit_lineage('transactions_silver_temp', 'silver/ibm_aml', run_id, job_name,
                     input_namespace='postgres', output_namespace='minio', event_type='FAIL')
        log_audit(pg_conn, dag_id, run_id, task_id, 'silver', 'failed',
                  duration_seconds=round(duration, 2), error_message=str(e))
        raise

    cur.close()
    pg_conn.close()

    total_rows = context['ti'].xcom_pull(task_ids='validate_staging', key='validated_rows')
    logger.info(f"Promote complete: {total_rows:,} rows in {duration:.1f}s")
    return total_rows


def create_indexes(**context):
    import sys, time
    sys.path.insert(0, '/opt/airflow/dags')
    from config import get_pg_conn

    start_time = time.time()
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    task_id = context['task'].task_id
    job_name = 'silver.create_indexes'

    pg_conn = get_pg_conn()
    pg_conn.autocommit = True
    cur = pg_conn.cursor()

    emit_lineage('transactions_silver', 'transactions_silver', run_id, job_name,
                 input_namespace='postgres', output_namespace='postgres', event_type='START')

    cur.execute("SET maintenance_work_mem = '256MB'")
    cur.execute("SET max_parallel_maintenance_workers = 1")

    indexes = [
        ("idx_silver_transaction_id", "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_silver_transaction_id ON transactions_silver(transaction_id)"),
        ("idx_silver_timestamp",      "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_silver_timestamp ON transactions_silver(timestamp)"),
        ("idx_silver_is_laundering",  "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_silver_is_laundering ON transactions_silver(is_laundering)"),
        ("idx_silver_sender",         "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_silver_sender ON transactions_silver(sender_account_masked)"),
        ("idx_silver_receiver",       "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_silver_receiver ON transactions_silver(receiver_account_masked)"),
        ("idx_silver_sender_ts",      "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_silver_sender_ts ON transactions_silver(sender_account_masked, timestamp)"),
        ("idx_silver_receiver_ts",    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_silver_receiver_ts ON transactions_silver(receiver_account_masked, timestamp)"),
    ]

    try:
        for name, sql in indexes:
            logger.info(f"Creating index: {name}...")
            cur.execute(sql)
            logger.info(f"Created: {name}")
            time.sleep(10)

        duration = time.time() - start_time
        emit_lineage('transactions_silver', 'transactions_silver', run_id, job_name,
                     input_namespace='postgres', output_namespace='postgres', event_type='COMPLETE')

        pg_conn.autocommit = False
        log_audit(pg_conn, dag_id, run_id, task_id, 'silver', 'success',
                  duration_seconds=round(duration, 2))

    except Exception as e:
        duration = time.time() - start_time
        emit_lineage('transactions_silver', 'transactions_silver', run_id, job_name,
                     input_namespace='postgres', output_namespace='postgres', event_type='FAIL')
        pg_conn.autocommit = False
        log_audit(pg_conn, dag_id, run_id, task_id, 'silver', 'failed',
                  duration_seconds=round(duration, 2), error_message=str(e))
        raise

    cur.close()
    pg_conn.close()
    logger.info("All indexes created")


with DAG(
    dag_id='aml_silver_transform',
    default_args=default_args,
    description='AML Silver — stage → validate → promote → index',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['aml', 'silver', 'production'],
) as dag:

    t1 = PythonOperator(
        task_id='read_transform_load',
        python_callable=read_transform_load,
        execution_timeout=timedelta(minutes=30),
    )
    t2 = PythonOperator(
        task_id='validate_staging',
        python_callable=validate_staging,
        execution_timeout=timedelta(minutes=10),
    )
    t3 = PythonOperator(
        task_id='promote_to_silver',
        python_callable=promote_to_silver,
        execution_timeout=timedelta(minutes=30),
    )
    t4 = PythonOperator(
        task_id='create_indexes',
        python_callable=create_indexes,
        execution_timeout=timedelta(minutes=30),
    )

    t1 >> t2 >> t3 >> t4
