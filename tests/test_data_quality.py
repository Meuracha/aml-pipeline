"""
Data Quality Tests — AML Pipeline
Tests Bronze, Silver, Gold layer data quality
"""

import numpy as np
import pandas as pd
import pytest


class TestBronzeDataQuality:
    """Bronze layer: raw data ingestion quality"""

    def test_required_columns_exist(self, sample_raw_df):
        required = [
            "Timestamp",
            "From Bank",
            "Account",
            "To Bank",
            "Account.1",
            "Amount Received",
            "Amount Paid",
            "Payment Currency",
            "Receiving Currency",
            "Payment Format",
            "Is Laundering",
        ]
        for col in required:
            assert col in sample_raw_df.columns, f"Missing column: {col}"

    def test_no_negative_amounts(self, sample_raw_df):
        assert (
            sample_raw_df["Amount Received"] >= 0
        ).all(), "Negative Amount Received found"
        assert (sample_raw_df["Amount Paid"] >= 0).all(), "Negative Amount Paid found"

    def test_is_laundering_binary(self, sample_raw_df):
        unique_values = set(sample_raw_df["Is Laundering"].unique())
        assert unique_values.issubset(
            {0, 1}
        ), f"Is Laundering must be 0 or 1, got {unique_values}"

    def test_timestamp_parseable(self, sample_raw_df):
        try:
            pd.to_datetime(sample_raw_df["Timestamp"])
        except Exception as e:
            pytest.fail(f"Timestamp not parseable: {e}")

    def test_no_empty_bank_names(self, sample_raw_df):
        assert sample_raw_df["From Bank"].notna().all(), "Null From Bank found"
        assert sample_raw_df["To Bank"].notna().all(), "Null To Bank found"

    def test_payment_format_valid(self, sample_raw_df):
        valid_formats = {
            "ACH",
            "Wire",
            "Cash",
            "Cheque",
            "Credit Card",
            "Bitcoin",
            "Reinvestment",
        }
        invalid = set(sample_raw_df["Payment Format"].unique()) - valid_formats
        assert len(invalid) == 0, f"Invalid payment formats: {invalid}"


class TestSilverDataQuality:
    """Silver layer: cleaned and standardized data"""

    def test_required_columns_exist(self, sample_silver_df):
        required = [
            "transaction_id",
            "timestamp",
            "amount",
            "payment_currency",
            "receiving_currency",
            "payment_type",
            "is_laundering",
            "sender_account_masked",
            "receiver_account_masked",
        ]
        for col in required:
            assert col in sample_silver_df.columns, f"Missing column: {col}"

    def test_no_null_critical_columns(self, sample_silver_df):
        critical = ["transaction_id", "timestamp", "amount", "is_laundering"]
        for col in critical:
            assert (
                sample_silver_df[col].notna().all()
            ), f"Null values in critical column: {col}"

    def test_transaction_id_unique(self, sample_silver_df):
        assert sample_silver_df["transaction_id"].nunique() == len(
            sample_silver_df
        ), "Duplicate transaction IDs found"

    def test_account_masked_format(self, sample_silver_df):
        pattern = r"^\*{4}[A-Z0-9]{4}$"
        sender_ok = sample_silver_df["sender_account_masked"].str.match(pattern).all()
        receiver_ok = (
            sample_silver_df["receiver_account_masked"].str.match(pattern).all()
        )
        assert sender_ok, "Sender account not properly masked"
        assert receiver_ok, "Receiver account not properly masked"

    def test_amount_positive(self, sample_silver_df):
        assert (
            sample_silver_df["amount"] > 0
        ).all(), "Non-positive amounts found in Silver"

    def test_timestamp_dtype(self, sample_silver_df):
        assert pd.api.types.is_datetime64_any_dtype(
            sample_silver_df["timestamp"]
        ), "Timestamp should be datetime type"

    def test_laundering_rate_reasonable(self, sample_silver_df):
        rate = sample_silver_df["is_laundering"].mean()
        assert rate <= 0.10, f"Laundering rate {rate:.2%} seems too high"
        assert rate >= 0, "Laundering rate cannot be negative"


class TestGoldDataQuality:
    """Gold layer: feature-engineered data"""

    FEATURE_COLS = [
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
        "is_laundering",
    ]

    def test_all_features_exist(self, sample_gold_df):
        for col in self.FEATURE_COLS:
            assert col in sample_gold_df.columns, f"Missing feature: {col}"

    def test_no_null_features(self, sample_gold_df):
        null_counts = sample_gold_df[self.FEATURE_COLS].isnull().sum()
        assert (
            null_counts.sum() == 0
        ), f"Null values in features:\n{null_counts[null_counts > 0]}"

    def test_amount_log_correct(self, sample_gold_df):
        expected = np.log1p(sample_gold_df["amount"])
        np.testing.assert_array_almost_equal(
            sample_gold_df["amount_log"],
            expected,
            decimal=4,
            err_msg="amount_log != log1p(amount)",
        )

    def test_tx_hour_range(self, sample_gold_df):
        assert (
            sample_gold_df["tx_hour"].between(0, 23).all()
        ), "tx_hour out of range [0, 23]"

    def test_tx_day_of_week_range(self, sample_gold_df):
        assert (
            sample_gold_df["tx_day_of_week"].between(0, 6).all()
        ), "tx_day_of_week out of range [0, 6]"

    def test_binary_features(self, sample_gold_df):
        binary_cols = [
            "is_weekend",
            "is_cross_currency",
            "is_high_risk_type",
            "is_structuring",
            "is_round_amount",
            "is_laundering",
        ]
        for col in binary_cols:
            unique = set(sample_gold_df[col].unique())
            assert unique.issubset({0, 1}), f"{col} must be binary, got {unique}"

    def test_rule_score_range(self, sample_gold_df):
        assert (
            sample_gold_df["rule_score"].between(0, 1).all()
        ), "rule_score out of range [0, 1]"

    def test_payment_type_risk_range(self, sample_gold_df):
        assert (
            sample_gold_df["payment_type_risk"].between(0, 1).all()
        ), "payment_type_risk out of range [0, 1]"

    def test_sender_tx_count_non_negative(self, sample_gold_df):
        assert (
            sample_gold_df["sender_tx_count_1h"] >= 0
        ).all(), "sender_tx_count_1h cannot be negative"

    def test_no_infinite_values(self, sample_gold_df):
        numeric_cols = sample_gold_df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            assert not np.isinf(
                sample_gold_df[col]
            ).any(), f"Infinite values found in {col}"


class TestDataContract:
    """Validate data contract compliance"""

    def test_schema_consistency_bronze_to_silver(self, sample_raw_df, sample_silver_df):
        """Row count should be preserved (or documented why not)"""
        assert len(sample_silver_df) <= len(
            sample_raw_df
        ), "Silver should have <= rows than Bronze (dead letter removes invalid)"

    def test_laundering_label_preserved(self, sample_silver_df, sample_gold_df):
        """Laundering label must not change through transformations"""
        silver_rate = sample_silver_df["is_laundering"].mean()
        gold_rate = sample_gold_df["is_laundering"].mean()
        assert (
            abs(silver_rate - gold_rate) < 0.01
        ), f"Laundering rate changed from Silver ({silver_rate:.4f}) to Gold ({gold_rate:.4f})"
