import requests
import pandas as pd
from pathlib import Path


# Base paths
BASE_DIR = Path(__file__).resolve().parents[1]  # project root
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)


def fetch_311_sample(limit: int = 10000) -> pd.DataFrame:
    """
    Fetch a sample of NYC 311 service requests from the Open Data API.
    Grabs the most recent 'limit' records.
    """
    url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

    params = {
        "$limit": limit,
        "$order": "created_date DESC",
    }

    print("Requesting data from NYC Open Data API...")
    resp = requests.get(url, params=params)
    resp.raise_for_status()

    data = resp.json()
    print(f"Received {len(data)} records.")
    df = pd.DataFrame(data)
    return df


def main():
    df = fetch_311_sample(limit=10000)

    csv_path = RAW_DIR / "nyc_311_sample.csv"
    df.to_csv(csv_path, index=False)
    print(f"Saved raw sample to {csv_path}")


if __name__ == "__main__":
    main()

