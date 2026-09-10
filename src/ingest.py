"""Download NYC Taxi parquet files to GCS or local."""

import os
import requests
from src.utils import load_config


def download_files(dest_dir="data/raw"):
    config = load_config()
    os.makedirs(dest_dir, exist_ok=True)

    for url in config["data"]["source_urls"]:
        filename = url.split("/")[-1]
        filepath = os.path.join(dest_dir, filename)

        if os.path.exists(filepath):
            print(f"Skipping {filename} (exists)")
            continue

        print(f"Downloading {filename}...")
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Saved {filepath} ({os.path.getsize(filepath) / 1e6:.1f} MB)")

    # Download zone lookup
    zone_url = config["data"]["zone_lookup_url"]
    zone_path = os.path.join(dest_dir, "taxi_zone_lookup.csv")
    if not os.path.exists(zone_path):
        print("Downloading zone lookup...")
        resp = requests.get(zone_url)
        with open(zone_path, "w") as f:
            f.write(resp.text)

    print("Ingestion complete.")


if __name__ == "__main__":
    download_files()
