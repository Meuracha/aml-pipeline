import pandas as pd
import hashlib
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).parent.parent.parent / "data" / "raw"

CURRENCY_MAP = {
    "US Dollar": "USD",
    "Euro": "EUR",
    "Bitcoin": "BTC",
    "Australian Dollar": "AUD",
    "Yuan": "CNY",
    "Rupee": "INR",
    "Ruble": "RUB",
    "UK Pound": "GBP",
    "Canadian Dollar": "CAD",
    "Swiss Franc": "CHF",
    "Brazilian Real": "BRL",
    "Mexico Peso": "MXN",
    "Shekel": "ILS",
    "Saudi Riyal": "SAR",
    "Yen": "JPY",
}


def make_transaction_id(row: pd.Series, source: str) -> str:
    key = f"{source}_{row['Timestamp']}_{row['Account_sender']}_{row['Account_receiver']}_{row['Amount Paid']}"
    return hashlib.md5(key.encode()).hexdigest()


def load_ibm_aml(filepath: str, chunksize: int = 100_000) -> pd.DataFrame:
    chunks = []
    for chunk in pd.read_csv(filepath, chunksize=chunksize):
        # rename duplicate 'Account' columns
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
        chunk["source"] = "ibm_aml"
        chunk["typology"] = None
        chunks.append(chunk)
    return pd.concat(chunks, ignore_index=True)


def standardize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # timestamp
    df["timestamp"] = pd.to_datetime(df["Timestamp"])

    # currency mapping
    df["payment_currency"] = (
        df["Payment Currency"].map(CURRENCY_MAP).fillna(df["Payment Currency"])
    )
    df["receiving_currency"] = (
        df["Receiving Currency"].map(CURRENCY_MAP).fillna(df["Receiving Currency"])
    )

    # unified schema
    df["transaction_id"] = df.apply(
        lambda r: make_transaction_id(r, r["source"]), axis=1
    )
    df["sender_account"] = df["Account_sender"].astype(str)
    df["receiver_account"] = df["Account_receiver"].astype(str)
    df["sender_bank"] = df["From Bank"].astype(str)
    df["receiver_bank"] = df["To Bank"].astype(str)
    df["amount"] = df["Amount Paid"].astype(float)
    df["payment_type"] = df["Payment Format"]
    df["is_laundering"] = df["Is Laundering"].astype(int)

    return df[
        [
            "transaction_id",
            "timestamp",
            "sender_account",
            "receiver_account",
            "sender_bank",
            "receiver_bank",
            "amount",
            "payment_currency",
            "receiving_currency",
            "payment_type",
            "is_laundering",
            "typology",
            "source",
        ]
    ]


if __name__ == "__main__":
    path = DATA_DIR / "LI-Small_Trans.csv"
    print(f"Loading {path}...")
    raw = load_ibm_aml(str(path))
    print(f"Loaded {len(raw):,} rows")
    std = standardize(raw)
    print(std.dtypes)
    print(std.head(3))
