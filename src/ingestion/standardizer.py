import pandas as pd
import numpy as np
from pathlib import Path

def mask_account(account: str) -> str:
    return "****" + str(account)[-4:]

def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['sender_account_masked'] = df['sender_account'].apply(mask_account)
    df['receiver_account_masked'] = df['receiver_account'].apply(mask_account)
    df['tx_hour'] = df['timestamp'].dt.hour
    df['tx_day_of_week'] = df['timestamp'].dt.dayofweek
    df['is_weekend'] = df['tx_day_of_week'].isin([5, 6]).astype(int)
    df['is_cross_currency'] = (df['payment_currency'] != df['receiving_currency']).astype(int)
    df['amount_log'] = np.log1p(df['amount'].clip(lower=0))
    return df

def validate(df: pd.DataFrame) -> dict:
    results = {
        'total_rows': len(df),
        'null_transaction_id': df['transaction_id'].isna().sum(),
        'null_amount': df['amount'].isna().sum(),
        'negative_amount': (df['amount'] < 0).sum(),
        'duplicate_transaction_id': df['transaction_id'].duplicated().sum(),
        'laundering_rate_pct': round(df['is_laundering'].mean() * 100, 4),
    }
    results['is_valid'] = (
        results['null_transaction_id'] == 0 and
        results['null_amount'] == 0 and
        results['negative_amount'] == 0
    )
    return results

if __name__ == "__main__":
    from collectors import load_ibm_aml, standardize, DATA_DIR
    path = DATA_DIR / "LI-Small_Trans.csv"
    raw = load_ibm_aml(str(path))
    std = standardize(raw)
    result = validate(std)
    print("Validation:", result)
    enriched = add_features(std)
    print(enriched[['sender_account_masked', 'tx_hour', 'is_weekend', 'is_cross_currency', 'amount_log']].head(3))
