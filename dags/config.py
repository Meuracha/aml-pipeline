import os

# MinIO
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', '')
MINIO_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')

# PostgreSQL
PG_HOST = os.getenv('POSTGRES_HOST', 'postgres')
PG_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
PG_DB = os.getenv('POSTGRES_DB', 'aml_db')
PG_USER = os.getenv('POSTGRES_USER', '')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD', '')

PG_CONN_STR = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASSWORD}"
PG_SQLALCHEMY_URI = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}/{PG_DB}"

def get_s3_client():
    import boto3
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

def get_pg_conn():
    import psycopg2
    return psycopg2.connect(PG_CONN_STR)

# Marquez
MARQUEZ_URL = os.getenv('MARQUEZ_URL', 'http://marquez:5000')
