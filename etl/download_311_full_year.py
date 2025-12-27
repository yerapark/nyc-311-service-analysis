import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# Base directories
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# NYC 311 API
API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

def fetch_chunk(where_clause, offset, limit=50000):
    """Fetch a chunk of data using limit + offset."""
    params = {
        "$where": where_clause,
        "$limit": limit,
        "$offset": offset,
        "$order": "created_date",
    }
    print(f"Requesting rows {offset} to {offset + limit}...")
    r = requests.get(API_URL, params=params)
    r.raise_for_status()
    return r.json()

def main():
    # Calculate 1-year window
    today = datetime.utcnow()
    start_date = today - timedelta(days=365)
    start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S")

    where_clause = f"created_date >= '{start_str}'"
    print(f"Downloading NYC 311 data where: {where_clause}")

    # Paginated fetch
    offset = 0
    limit = 50000
    all_rows = []

    while True:
        chunk = fetch_chunk(where_clause, offset, limit)
        if not chunk:
            print("No more rows. Stopping.")
            break
        all_rows.extend(chunk)
        offset += limit

    print(f"Downloaded {len(all_rows)} rows total.")

    # Convert to DataFrame
    df = pd.DataFrame(all_rows)

    # Save raw full-year file
    out_path = RAW_DIR / "nyc_311_full_year.parquet"
    df.to_parquet(out_path, index=False)
    print(f"Saved full-year data to: {out_path}")

if __name__ == "__main__":
    main()
