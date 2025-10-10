import gzip
import json
import pandas as pd


def read_gz_json(file_path):
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        data = json.load(f)

    return pd.json_normalize(data, sep="_")


def save_parquet(df, path):
    df.to_parquet(path, index=False)
    print(f"Saved {path}")
