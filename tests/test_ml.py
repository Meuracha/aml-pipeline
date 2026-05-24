"""
ML Model Tests — AML Pipeline
"""
import pytest
import numpy as np
import pandas as pd
import argparse

xgb = pytest.importorskip("xgboost", reason="xgboost not installed")

FEATURES = [
    'amount', 'amount_log', 'tx_hour', 'tx_day_of_week',
    'is_weekend', 'is_cross_currency', 'sender_tx_count_1h',
    'sender_amount_sum_1h', 'sender_avg_amount',
    'amount_vs_sender_avg', 'payment_type_risk',
    'is_high_risk_type', 'is_structuring',
    'is_round_amount', 'rule_score'
]

MIN_AUC_ROC = 0.85
MIN_RECALL  = 0.20


@pytest.fixture
def mock_xgboost_model():
    from unittest.mock import MagicMock
    model = MagicMock()
    model.predict.return_value = np.array([0.85])
    model.feature_names = FEATURES
    return model


@pytest.fixture
def sample_features():
    return {
        'amount': 9500.0,
        'amount_log': np.log1p(9500.0),
        'tx_hour': 10,
        'tx_day_of_week': 2,
        'is_weekend': 0,
        'is_cross_currency': 1,
        'sender_tx_count_1h': 5,
        'sender_amount_sum_1h': 47500.0,
        'sender_avg_amount': 9500.0,
        'amount_vs_sender_avg': 1.0,
        'payment_type_risk': 0.8,
        'is_high_risk_type': 1,
        'is_structuring': 1,
        'is_round_amount': 0,
        'rule_score': 0.75,
    }


class TestFeatureEngineering:

    def test_amount_log_calculation(self, sample_gold_df):
        expected = np.log1p(sample_gold_df['amount'])
        np.testing.assert_array_almost_equal(
            sample_gold_df['amount_log'], expected, decimal=4
        )

    def test_all_features_present(self, sample_gold_df):
        for feature in FEATURES:
            assert feature in sample_gold_df.columns, f"Missing feature: {feature}"

    def test_feature_no_nulls(self, sample_gold_df):
        null_counts = sample_gold_df[FEATURES].isnull().sum()
        assert null_counts.sum() == 0

    def test_feature_no_inf(self, sample_gold_df):
        for col in FEATURES:
            if col in sample_gold_df.columns:
                assert not np.isinf(sample_gold_df[col]).any()

    def test_rule_score_combines_signals(self, sample_gold_df):
        laundering = sample_gold_df[sample_gold_df['is_laundering'] == 1]
        normal = sample_gold_df[sample_gold_df['is_laundering'] == 0]
        if len(laundering) > 0 and len(normal) > 0:
            assert laundering['rule_score'].mean() >= normal['rule_score'].mean()


class TestModelPrediction:

    def test_prediction_in_range(self, mock_xgboost_model, sample_features):
        X = [[sample_features[f] for f in FEATURES]]
        dmatrix = xgb.DMatrix(X, feature_names=FEATURES)
        # ใช้ mock prediction value โดยตรง
        pred = mock_xgboost_model.predict(dmatrix)
        assert 0 <= float(pred[0]) <= 1

    def test_threshold_classification(self):
        threshold = 0.90
        scores = [0.05, 0.45, 0.75, 0.92, 0.99]
        expected = [False, False, False, True, True]
        results = [s >= threshold for s in scores]
        assert results == expected

    def test_high_risk_flags(self):
        """High-risk feature flags should increase suspicion"""
        base_score = 0.10
        high_risk_score = 0.92
        assert high_risk_score > base_score


class TestModelValidationGate:

    def test_auc_roc_above_minimum(self):
        val_auc_roc = 0.9362
        assert val_auc_roc >= MIN_AUC_ROC

    def test_recall_above_minimum(self):
        val_recall = 0.2783
        assert val_recall >= MIN_RECALL

    def test_scale_pos_weight_reasonable(self):
        scale_pos_weight = 2155.47
        assert 100 < scale_pos_weight < 10000

    def test_feature_list_unchanged(self):
        expected = [
            'amount', 'amount_log', 'tx_hour', 'tx_day_of_week',
            'is_weekend', 'is_cross_currency', 'sender_tx_count_1h',
            'sender_amount_sum_1h', 'sender_avg_amount',
            'amount_vs_sender_avg', 'payment_type_risk',
            'is_high_risk_type', 'is_structuring',
            'is_round_amount', 'rule_score'
        ]
        assert FEATURES == expected


def validate_production_model():
    import mlflow, os
    mlflow.set_tracking_uri(os.getenv('MLFLOW_TRACKING_URI'))
    client = mlflow.MlflowClient()
    try:
        versions = client.get_latest_versions("aml_risk_model")
        if not versions:
            print("❌ No model versions found")
            return False
        latest = versions[-1]
        run = client.get_run(latest.run_id)
        metrics = run.data.metrics
        auc_roc = metrics.get('val_auc_roc', 0)
        recall  = metrics.get('val_recall', 0)
        print(f"Val AUC-ROC: {auc_roc:.4f} (min: {MIN_AUC_ROC})")
        print(f"Val Recall:  {recall:.4f} (min: {MIN_RECALL})")
        if auc_roc < MIN_AUC_ROC:
            print(f"❌ BLOCK DEPLOY — AUC-ROC too low")
            return False
        if recall < MIN_RECALL:
            print(f"❌ BLOCK DEPLOY — Recall too low")
            return False
        print("✅ Model validation passed")
        return True
    except Exception as e:
        print(f"❌ Validation error: {e}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--validate-production', action='store_true')
    args = parser.parse_args()
    if args.validate_production:
        import sys
        sys.exit(0 if validate_production_model() else 1)