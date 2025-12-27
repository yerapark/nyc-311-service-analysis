import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
CLEAN_DIR = BASE_DIR / "data" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

RAW_PATH = RAW_DIR / "nyc_311_full_year.parquet"
OUT_PATH = CLEAN_DIR / "nyc_311_full_year_cleaned.parquet"


def main():
    print(f"Loading full-year data from: {RAW_PATH}")
    df = pd.read_parquet(RAW_PATH)

    print(f"Original shape: {df.shape}")

    # Keep a subset of useful columns (only if they exist)
    cols_to_keep = [
        "created_date",
        "closed_date",
        "complaint_type",
        "descriptor",
        "agency",
        "borough",
        "incident_zip",
        "latitude",
        "longitude",
    ]
    existing_cols = [c for c in cols_to_keep if c in df.columns]
    df = df[existing_cols].copy()
    print(f"After column filter: {df.shape}")

    # Parse dates
    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")
    df["closed_date"] = pd.to_datetime(df["closed_date"], errors="coerce")

    # Drop invalid dates
    df = df.dropna(subset=["created_date", "closed_date"])
    print(f"After dropping invalid dates: {df.shape}")

    # Compute resolution time in hours
    df["resolution_hours"] = (
        df["closed_date"] - df["created_date"]
    ).dt.total_seconds() / 3600

    # Filter unrealistic values (negatives and > 30 days)
    df = df[(df["resolution_hours"] >= 0) & (df["resolution_hours"] <= 24 * 30)]
    print(f"After filtering resolution_hours: {df.shape}")

    # Time features
    df["month"] = df["created_date"].dt.month
    df["hour"] = df["created_date"].dt.hour
    df["weekday"] = df["created_date"].dt.weekday  # 0=Mon
    df["is_weekend"] = df["weekday"].isin([5, 6]).astype(int)

    # Optional: downcast numeric columns to save space
    num_cols = ["resolution_hours", "month", "hour", "weekday", "is_weekend"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], downcast="float")

    print("Final dtypes:")
    print(df.dtypes)

    print(f"Saving cleaned data to: {OUT_PATH}")
    df.to_parquet(OUT_PATH, index=False)
    print("Done.")


if __name__ == "__main__":
    main()