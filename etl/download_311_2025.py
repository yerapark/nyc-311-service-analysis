"""
ETL: Download full-year 2025 NYC 311 Service Requests.

This script:
- Pulls data from NYC Open Data 311 endpoint (erm2-nwe9)
- Restricts to created_date in calendar year 2025
- Downloads in chunks using $limit / $offset pagination
- Saves a single Parquet file to data/raw/nyc_311_2025_raw.parquet

Usage (from project root):
    python etl/download_311_2025.py
"""

import os
import time
from pathlib import Path

import pandas as pd
import requests

# NYC 311 Service Requests endpoint (Open Data)
BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

# Optional: set an app token in your environment to avoid strict rate limiting
APP_TOKEN = os.getenv("NYC_OPEN_DATA_APP_TOKEN", None)

# Year we want to pull
YEAR = 2025

# Page size for each API call
LIMIT = 50_000


def fetch_chunk(offset: int) -> list[dict]:
    """
    Fetch a single chunk of NYC 311 data for 2025 using limit/offset pagination.
    Returns a list of dicts (rows). If empty, we are done.
    """
    where_clause = (
        "created_date >= '2025-01-01T00:00:00' AND "
        "created_date < '2026-01-01T00:00:00'"
    )

    params = {
        "$where": where_clause,
        "$order": "created_date",
        "$limit": LIMIT,
        "$offset": offset,
    }

    headers = {}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN

    resp = requests.get(BASE_URL, params=params, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data


def main():
    project_root = Path(__file__).resolve().parents[1]
    data_dir = project_root / "data" / "raw"
    data_dir.mkdir(parents=True, exist_ok=True)

    all_chunks: list[pd.DataFrame] = []
    offset = 0
    total_rows = 0

    print("Starting 2025 NYC 311 download...")
    print(f"Saving to: {data_dir}")

    while True:
        print(f"Fetching rows with offset={offset} ...")
        rows = fetch_chunk(offset)

        if not rows:
            print("No more rows returned. Download complete.")
            break

        chunk_df = pd.DataFrame.from_records(rows)
        chunk_size = len(chunk_df)
        total_rows += chunk_size
        all_chunks.append(chunk_df)

        print(f"  Retrieved {chunk_size} rows (cumulative: {total_rows})")

        # Move to next page
        offset += LIMIT

        # Be polite with the API
        time.sleep(0.5)

    if not all_chunks:
        print("No data downloaded for 2025. Check API or filters.")
        return

    df_2025 = pd.concat(all_chunks, ignore_index=True)

    # Ensure created_date is proper datetime and confirm year range
    if "created_date" in df_2025.columns:
        df_2025["created_date"] = pd.to_datetime(
            df_2025["created_date"], errors="coerce"
        )
        year_counts = df_2025["created_date"].dt.year.value_counts().sort_index()
        print("\nYear distribution in downloaded data:")
        print(year_counts)

        # Sanity filter again, just in case
        df_2025 = df_2025[
            (df_2025["created_date"].dt.year == YEAR)
            & df_2025["created_date"].notna()
        ].copy()

    # Save as Parquet
    output_path = data_dir / "nyc_311_2025_raw.parquet"
    df_2025.to_parquet(output_path, index=False)

    print(f"\nFinal row count for 2025: {len(df_2025):,}")
    print(f"Saved raw 2025 data to: {output_path}")


if __name__ == "__main__":
    main()