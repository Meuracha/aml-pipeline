"""
src/spark_features.py
---------------------
PySpark feature engineering for AML pipeline (Gold layer).

เพิ่มก่อน t1 (feature_engineering / DuckDB) ใน aml_gold_transform DAG
อ่าน transactions_silver จาก PostgreSQL → compute rolling features →
เขียน Parquet ลง MinIO s3://gold/spark_features/

Features ที่เพิ่ม (ไม่ซ้ำกับ DuckDB ใน t1):
  - rolling_7d_tx_count   : จำนวน tx ของ sender ใน 7 วันที่ผ่านมา
  - rolling_30d_tx_count  : จำนวน tx ของ sender ใน 30 วันที่ผ่านมา
  - amount_zscore         : z-score ของ amount แยกตาม payment_type
  - time_bucket           : morning / afternoon / evening / night

Design: SparkSession local[*] — ไม่ต้องตั้ง cluster
Sampling: รันบน 15% sample ใน local Docker (RAM จำกัด 5GB)
          Production → ตั้ง SPARK_SAMPLE_FRACTION=1.0 บน EMR/Dataproc
"""

import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

# ── Config (ใช้ env var เดิมจาก config.py / docker-compose) ─────────────────
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "aml_db")
PG_USER = os.getenv("POSTGRES_USER", "")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

# MinIO ใช้ key เดิมจาก docker-compose (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
MINIO_BUCKET = "gold"

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/spark_features/"

# 1.0 = full dataset (production), 0.15 = sample (local Docker 5GB RAM)
SPARK_SAMPLE_FRACTION = float(os.getenv("SPARK_SAMPLE_FRACTION", "0.15"))


def _build_spark():
    from pyspark.sql import SparkSession

    # MinIO endpoint: ถ้าขึ้นต้นด้วย http:// ให้ตัดออก (S3A ใช้ host:port เท่านั้น)
    minio_host_port = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")

    jars = ",".join([
        "/opt/spark/jars/postgresql-42.7.3.jar",
        "/opt/spark/jars/hadoop-aws-3.3.4.jar",
        "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
    ])

    return (
        SparkSession.builder.appName("AML-SparkFeatureEngineering")
        .master("local[*]")
        # JARs — PostgreSQL JDBC + S3A (explicit classpath)
        .config("spark.jars", jars)
        .config("spark.driver.extraClassPath", jars)
        # S3A → MinIO
        .config("spark.hadoop.fs.s3a.endpoint", minio_host_port)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ลด overhead สำหรับ sample ~1M rows
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1500m")
        .getOrCreate()
    )


def _read_silver(spark):
    """อ่าน transactions_silver จาก PostgreSQL ผ่าน JDBC"""
    logger.info("Reading transactions_silver from PostgreSQL via JDBC...")
    df = spark.read.jdbc(
        url=JDBC_URL,
        table="transactions_silver",
        properties={
            "user": PG_USER,
            "password": PG_PASSWORD,
            "driver": "org.postgresql.Driver",
            # fetchsize ช่วย throughput สำหรับ large table
            "fetchsize": "50000",
        },
    )
    # Sample ก่อน count เพื่อไม่ให้ JVM กิน RAM ตอน scan full table
    if SPARK_SAMPLE_FRACTION < 1.0:
        df = df.sample(fraction=SPARK_SAMPLE_FRACTION, seed=42)
        sampled_count = df.count()
        logger.info(
            f"Sampled {SPARK_SAMPLE_FRACTION:.0%} → {sampled_count:,} rows "
            f"(local Docker mode, full dataset for production EMR/Dataproc)"
        )
    else:
        full_count = df.count()
        logger.info(f"Loaded {full_count:,} rows from transactions_silver")

    return df


def _compute_features(df):
    """
    Compute features ที่ DuckDB ใน t1 ยังไม่มี:
      1. rolling_7d_tx_count / rolling_30d_tx_count (range window บน unix timestamp)
      2. amount_zscore per payment_type
      3. time_bucket

    ใช้ column ที่มีจริงใน silver schema:
      sender_account_masked, timestamp, amount, payment_type
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    # 1. time_bucket
    df = df.withColumn(
        "time_bucket",
        F.when((F.hour("timestamp") >= 6) & (F.hour("timestamp") < 12), "morning")
        .when((F.hour("timestamp") >= 12) & (F.hour("timestamp") < 18), "afternoon")
        .when((F.hour("timestamp") >= 18) & (F.hour("timestamp") < 22), "evening")
        .otherwise("night"),
    )

    # 2. Rolling window counts ผ่าน range window (unix seconds)
    df = df.withColumn("ts_unix", F.unix_timestamp("timestamp"))

    SECONDS_7D = 7 * 24 * 3600
    SECONDS_30D = 30 * 24 * 3600

    w7 = (
        Window.partitionBy("sender_account_masked")
        .orderBy("ts_unix")
        .rangeBetween(-SECONDS_7D, 0)
    )
    w30 = (
        Window.partitionBy("sender_account_masked")
        .orderBy("ts_unix")
        .rangeBetween(-SECONDS_30D, 0)
    )

    df = df.withColumn("rolling_7d_tx_count", F.count("transaction_id").over(w7))
    df = df.withColumn("rolling_30d_tx_count", F.count("transaction_id").over(w30))

    # 3. Amount z-score per payment_type
    w_pay = Window.partitionBy("payment_type")
    df = df.withColumn("_pay_mean", F.mean("amount").over(w_pay))
    df = df.withColumn("_pay_std", F.stddev("amount").over(w_pay))
    df = df.withColumn(
        "amount_zscore",
        F.when(
            F.col("_pay_std") > 0,
            (F.col("amount") - F.col("_pay_mean")) / F.col("_pay_std"),
        ).otherwise(F.lit(0.0)),
    )
    df = df.drop("_pay_mean", "_pay_std", "ts_unix")

    return df


def _write_features(df, run_date: str) -> str:
    """เขียน enriched features ลง MinIO เป็น Parquet"""
    output = f"{OUTPUT_PATH}run_date={run_date}/"
    logger.info(f"Writing Spark features to {output} ...")
    (
        df.select(
            "transaction_id",
            "time_bucket",
            "rolling_7d_tx_count",
            "rolling_30d_tx_count",
            "amount_zscore",
        )
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(output)
    )
    logger.info(f"Done. Output: {output}")
    return output


def run_spark_feature_engineering(run_date: str = None, **context) -> str:
    """
    Entry point สำหรับ Airflow PythonOperator

    Returns output path (push ผ่าน XCom อัตโนมัติ)
    """
    if run_date is None:
        run_date = datetime.utcnow().strftime("%Y-%m-%d")

    spark = _build_spark()
    try:
        df = _read_silver(spark)
        df = _compute_features(df)
        output_path = _write_features(df, run_date)
        # push XCom ให้ task ถัดไปใช้ได้ถ้าต้องการ
        if context.get("ti"):
            context["ti"].xcom_push(key="spark_features_path", value=output_path)
        return output_path
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    path = run_spark_feature_engineering()
    print(f"Features written to: {path}")