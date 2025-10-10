from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent

RAW_DIR = BASE_DIR / "data/raw/events-json/analytics_291746817/2024/10"
PROCESSED_DIR = BASE_DIR / "data/processed"
PARQUET_DIR = BASE_DIR / "data/parquet"


for d in [RAW_DIR, PROCESSED_DIR, PARQUET_DIR]:
    d.mkdir(parents=True, exist_ok=True)
