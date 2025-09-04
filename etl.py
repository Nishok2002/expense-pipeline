import os
import glob
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Date, Numeric, Text
from dotenv import load_dotenv

# -------- Load DB creds from .env (with sensible defaults) --------
load_dotenv()
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "expenses")
PG_USER = os.getenv("PG_USER", "expense_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "expense_pass")

# SQLAlchemy engine URL using psycopg3
ENGINE_URL = f"postgresql+psycopg://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = create_engine(ENGINE_URL, future=True)

RAW_DIR = "data/raw"

# Simple merchant → category map (extend anytime)
CATEGORY_MAP = {
    "NETFLIX": "Entertainment",
    "SPOTIFY": "Entertainment",
    "STARBUCKS": "Food",
    "UBER": "Transport",
    "AMAZON": "Shopping",
    "RENT": "Housing",
    "SALARY": "Income",
}

def normalize_merchant(desc: str) -> str | None:
    """Basic merchant normalization: uppercase match on known keywords, else first token."""
    if not isinstance(desc, str):
        return None
    s = desc.strip().upper()
    for key in CATEGORY_MAP.keys():
        if key in s:
            return key.title()
    return s.title().split()[0] if s else None

def categorize(merchant: str | None) -> str | None:
    if not merchant:
        return None
    up = merchant.upper()
    for key, cat in CATEGORY_MAP.items():
        if key in up:
            return cat
    return "Other"

def read_and_transform(csv_path: str) -> pd.DataFrame:
    """Read one CSV, validate columns, coerce types, derive merchant/category, standardize."""
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip().str.lower()

    required = {"date", "description", "amount"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise ValueError(f"{csv_path} missing columns: {missing}")

    # Parse US-style dates and numeric amounts (invalid → NaT/NaN)
    df["txn_date"] = pd.to_datetime(df["date"], errors="coerce", format="%m/%d/%Y")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    before = len(df)
    df = df.dropna(subset=["txn_date", "amount"])
    dropped = before - len(df)
    if dropped:
        print(f"[{os.path.basename(csv_path)}] dropped invalid rows: {dropped}")

    df["merchant"] = df["description"].apply(normalize_merchant)
    df["category"] = df["merchant"].apply(categorize)

    out = df[["txn_date", "merchant", "description", "amount", "category"]].copy()
    out["src_file"] = os.path.basename(csv_path)
    return out

def load_to_postgres(df: pd.DataFrame) -> None:
    """Append DataFrame to raw.transactions with explicit types."""
    dtype_map = {
        "txn_date": Date(),
        "merchant": Text(),
        "description": Text(),
        "amount": Numeric(12, 2),
        "category": Text(),
        "src_file": Text(),
    }
    df.to_sql(
        name="transactions",
        schema="raw",
        con=engine,
        if_exists="append",
        index=False,
        dtype=dtype_map,
        method="multi",
        chunksize=1000,
    )

def main():
    files = sorted(glob.glob(os.path.join(RAW_DIR, "*.csv")))
    if not files:
        print("No files found in data/raw")
        return

    total_rows = 0
    for f in files:
        print(f"Processing {f}")
        df = read_and_transform(f)
        if df.empty:
            print("  -> no valid rows, skipping")
            continue
        load_to_postgres(df)
        total_rows += len(df)
    print(f"Loaded rows: {total_rows}")

if __name__ == "__main__":
    main()
