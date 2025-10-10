import time
import pandas as pd
from src.config.paths import RAW_DIR, PARQUET_DIR
from src.utils.io_handler import read_gz_json, save_parquet
from src.utils.normalizer import normalize_nested_params


def process_file(file_path):
    df = read_gz_json(file_path)

    event_params_df = normalize_nested_params(df, "event_params", "ep_")
    user_props_df = normalize_nested_params(df, "user_properties", "user_prop_")

    final_df = pd.concat([df, event_params_df, user_props_df], axis=1)
    final_df.drop(["event_params", "user_properties"], axis=1, inplace=True)

    event_date_dt = pd.to_datetime(final_df["event_date"], format="%Y%m%d")
    final_df["year"] = event_date_dt.dt.year
    final_df["month"] = event_date_dt.dt.month

    return final_df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all columns have compatible types for Parquet."""
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)
    return df


def run_normalization():
    start = time.time()
    input_dir = RAW_DIR / "events-json/analytics_291746817/2024/10"
    output_dir = PARQUET_DIR / "events-normalized/analytics_291746817/2024/10"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Reading files from {input_dir}")

    files = list(input_dir.glob("*.json*"))
    if not files:
        print(f"No files found in {input_dir}")
        return

    for file_path in files:
        print(f"Processing {file_path.name} ...")
        df = process_file(file_path)
        df = clean_dataframe(df)
        save_parquet(df, output_dir / f"{file_path.stem}.parquet")

    print(f"Normalization completed in {time.time() - start:.2f} seconds")
