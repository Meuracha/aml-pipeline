"""
Shared test fixtures for AML Pipeline tests
"""

import os
import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch


# ─────────────────────────────────────────
# Environment
# ─────────────────────────────────────────
@pytest.fixture(scope="session", autouse=True)
def set_env():
    os.environ.setdefault("POSTGRES_HOST", "localhost")
    os.environ.setdefault("POSTGRES_PORT", "5432")
    os.environ.setdefault("POSTGRES_USER", "test_user")
    os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
    os.environ.setdefault("POSTGRES_DB", "aml_db")
    os.environ.setdefault("MINIO_ENDPOINT", "http://localhost:9000")
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test_minio_user")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test_minio_password")
    os.environ.setdefault("MLFLOW_TRACKING_URI", "http://localhost:5000")


# ─────────────────────────────────────────
# Sample Data Fixtures
# ─────────────────────────────────────────
@pytest.fixture
def sample_raw_df():
    """Raw transaction data (Bronze layer)"""
    return pd.DataFrame(
        {
            "Timestamp": ["2022-09-01 00:00:00"] * 10,
            "From Bank": ["BANK_A"] * 10,
            "Account": ["ACC001"] * 10,
            "To Bank": ["BANK_B"] * 10,
            "Account.1": ["ACC002"] * 10,
            "Amount Received": [
                1000.0,
                9000.0,
                500.0,
                9500.0,
                100.0,
                9800.0,
                200.0,
                9999.0,
                1500.0,
                5000.0,
            ],
            "Receiving Currency": ["USD"] * 10,
            "Amount Paid": [
                1000.0,
                9000.0,
                500.0,
                9500.0,
                100.0,
                9800.0,
                200.0,
                9999.0,
                1500.0,
                5000.0,
            ],
            "Payment Currency": ["USD"] * 5 + ["EUR"] * 5,
            "Payment Format": ["ACH"] * 3 + ["Wire"] * 3 + ["Cash"] * 4,
            "Is Laundering": [0, 0, 0, 0, 0, 0, 0, 1, 0, 0],  # 10% rate
        }
    )


@pytest.fixture
def sample_silver_df():
    """Cleaned transaction data (Silver layer)"""
    return pd.DataFrame(
        {
            "transaction_id": [f"tx_{i:04d}" for i in range(10)],
            "timestamp": pd.date_range("2022-09-01", periods=10, freq="h"),
            "sender_account_masked": ["****0001"] * 10,
            "receiver_account_masked": ["****0002"] * 10,
            "sender_bank": ["BANK_A"] * 10,
            "receiver_bank": ["BANK_B"] * 10,
            "amount": [
                1000.0,
                9000.0,
                500.0,
                9500.0,
                100.0,
                9800.0,
                200.0,
                9999.0,
                1500.0,
                5000.0,
            ],
            "payment_currency": ["USD"] * 5 + ["EUR"] * 5,
            "receiving_currency": ["USD"] * 10,
            "payment_type": [
                "ACH",
                "ACH",
                "Wire",
                "Wire",
                "Cash",
                "Cash",
                "ACH",
                "Wire",
                "Cash",
                "ACH",
            ],
            "is_laundering": [0, 0, 0, 0, 0, 0, 0, 1, 0, 0],  # 10% rate
        }
    )


@pytest.fixture
def sample_gold_df():
    """Feature-engineered data (Gold layer)"""
    return pd.DataFrame(
        {
            "transaction_id": [f"tx_{i:04d}" for i in range(10)],
            "timestamp": pd.date_range("2022-09-01", periods=10, freq="h"),
            "amount": [
                1000.0,
                9000.0,
                500.0,
                9500.0,
                100.0,
                9800.0,
                200.0,
                9999.0,
                1500.0,
                5000.0,
            ],
            "amount_log": np.log1p(
                [
                    1000.0,
                    9000.0,
                    500.0,
                    9500.0,
                    100.0,
                    9800.0,
                    200.0,
                    9999.0,
                    1500.0,
                    5000.0,
                ]
            ),
            "tx_hour": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            "tx_day_of_week": [3] * 10,
            "is_weekend": [0] * 10,
            "is_cross_currency": [0, 0, 0, 0, 0, 1, 1, 1, 1, 1],
            "sender_tx_count_1h": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "sender_amount_sum_1h": [
                1000.0,
                10000.0,
                10500.0,
                20000.0,
                20100.0,
                29900.0,
                30100.0,
                40100.0,
                41600.0,
                46600.0,
            ],
            "sender_avg_amount": [
                1000.0,
                5000.0,
                3500.0,
                5000.0,
                4020.0,
                5000.0,
                4300.0,
                5025.0,
                4622.0,
                4660.0,
            ],
            "amount_vs_sender_avg": [
                1.0,
                1.8,
                0.14,
                1.9,
                0.02,
                1.96,
                0.05,
                1.99,
                0.32,
                1.07,
            ],
            "payment_type_risk": [0.5, 0.5, 0.3, 0.3, 0.1, 0.1, 0.5, 0.3, 0.1, 0.5],
            "is_high_risk_type": [1, 1, 0, 0, 0, 0, 1, 0, 0, 1],
            "is_structuring": [0, 1, 0, 1, 0, 1, 0, 1, 0, 0],
            "is_round_amount": [1, 1, 1, 0, 1, 0, 1, 0, 1, 1],
            "rule_score": [0.1, 0.7, 0.05, 0.75, 0.02, 0.8, 0.06, 0.9, 0.15, 0.4],
            "is_laundering": [0, 0, 0, 0, 0, 0, 0, 1, 0, 0],  # 10% rate
        }
    )


@pytest.fixture
def sample_scores_dict():
    """In-memory scores dict"""
    np.random.seed(42)
    return {
        f"tx_{i:04d}": (float(np.random.uniform(0, 1)), float(np.random.uniform(0, 1)))
        for i in range(10)
    }


@pytest.fixture
def mock_s3():
    """Mock S3/MinIO client"""
    with patch("boto3.client") as mock:
        yield mock.return_value


@pytest.fixture
def mock_pg():
    """Mock PostgreSQL connection"""
    with patch("psycopg2.connect") as mock:
        yield mock.return_value
