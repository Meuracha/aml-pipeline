-- สร้าง database แยกสำหรับ MLflow
CREATE DATABASE mlflow_db;
GRANT ALL PRIVILEGES ON DATABASE mlflow_db TO aml_user;

-- สร้าง database แยกสำหรับ Marquez
CREATE DATABASE marquez_db;
GRANT ALL PRIVILEGES ON DATABASE marquez_db TO aml_user;

-- AML tables (ใน aml_db เหมือนเดิม)
\c aml_db;

CREATE TABLE IF NOT EXISTS transactions_silver (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR UNIQUE NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    sender_account VARCHAR NOT NULL,
    receiver_account VARCHAR NOT NULL,
    sender_bank VARCHAR,
    receiver_bank VARCHAR,
    amount FLOAT NOT NULL,
    payment_currency VARCHAR,
    receiving_currency VARCHAR,
    payment_type VARCHAR,
    source VARCHAR,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions_featured (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR UNIQUE NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    sender_account_masked VARCHAR,
    receiver_account_masked VARCHAR,
    amount FLOAT,
    amount_log FLOAT,
    is_cross_currency BOOLEAN,
    tx_hour INT,
    tx_day_of_week INT,
    is_weekend BOOLEAN,
    sender_tx_count_1h INT,
    sender_tx_amount_1h FLOAT,
    amount_vs_sender_avg FLOAT,
    rule_score FLOAT,
    ml_probability FLOAT,
    final_risk_score FLOAT,
    typology VARCHAR,
    is_suspicious BOOLEAN,
    processed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aml_alerts (
    id SERIAL PRIMARY KEY,
    alert_id VARCHAR UNIQUE NOT NULL,
    transaction_id VARCHAR NOT NULL,
    risk_score FLOAT NOT NULL,
    typology VARCHAR,
    status VARCHAR DEFAULT 'OPEN',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_risk_reasons (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR NOT NULL,
    reason_code VARCHAR NOT NULL,
    reason_desc VARCHAR NOT NULL,
    shap_value FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_typology (
    id SERIAL PRIMARY KEY,
    typology_code VARCHAR UNIQUE NOT NULL,
    typology_name VARCHAR NOT NULL,
    description TEXT,
    severity VARCHAR,
    source VARCHAR
);

CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT NOW(),
    action VARCHAR NOT NULL,
    transaction_id VARCHAR,
    risk_score FLOAT,
    performed_by VARCHAR DEFAULT 'system',
    details JSONB
);

CREATE TABLE IF NOT EXISTS dead_letter_log (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT NOW(),
    source VARCHAR NOT NULL,
    raw_data JSONB,
    error_message TEXT,
    retry_count INT DEFAULT 0
);
CREATE TABLE IF NOT EXISTS pipeline_audit_log (
    id SERIAL PRIMARY KEY,
    dag_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    layer TEXT NOT NULL,
    status TEXT NOT NULL,
    rows_processed BIGINT DEFAULT 0,
    rows_dead_letter BIGINT DEFAULT 0,
    duration_seconds FLOAT DEFAULT 0,
    laundering_rate FLOAT DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
