from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data/raw"
PROCESSED_DIR = BASE_DIR / "data/processed"
PARQUET_DIR = BASE_DIR / "data/parquet"


for d in [RAW_DIR, PROCESSED_DIR, PARQUET_DIR]:
    d.mkdir(parents=True, exist_ok=True)
