import json
import pandas as pd
import gzip
import pathlib

# Base directory = location of this script
base_dir = pathlib.Path(__file__).parent

# Input & output folders
input_dir = (
    base_dir / "../ga4-raw-data/events-json/analytics_291746817/2024/10"
).resolve()
output_dir = (base_dir / "../ga4-clean/analytics291746817/2024/10").resolve()
output_dir.mkdir(parents=True, exist_ok=True)

print("Looking in:", input_dir)


def process_file(file_path):
    """Process one GA4 gzipped JSON file into a DataFrame."""
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        data = json.load(f)

    # Flatten JSON
    data_df = pd.json_normalize(data, sep="_")

    # Extract event_params → prefix with "param_"
    params_data = [
        {item["key"]: next(v for v in item["value"].values() if v is not None)}
        for params_list in data_df["event_params"]
        for item in params_list
    ]
    params_df = pd.DataFrame(params_data).add_prefix("ep_")

    # Extract user_properties → prefix with "user_prop_"
    user_props_data = [
        {
            prop["key"]: next(
                v
                for k, v in prop["value"].items()
                if v is not None and k != "set_timestamp_micros"
            )
            for prop in props_list
        }
        for props_list in data_df["user_properties"]
    ]
    user_props_df = pd.DataFrame(user_props_data).add_prefix("user_prop_")

    # Add year & month
    event_date_dt = pd.to_datetime(data_df["event_date"], format="%Y%m%d")
    data_df["year"] = event_date_dt.dt.year
    data_df["month"] = event_date_dt.dt.month

    # Final DataFrame (safe from duplicate column names)
    final_df = pd.concat([data_df, params_df, user_props_df], axis=1)
    final_df.drop(columns=["user_properties", "event_params"], inplace=True)

    return final_df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all columns have compatible types for Parquet."""
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)
    return df


# Process all .json files in the folder
for file_path in input_dir.glob("*.json"):
    print(f"Processing {file_path.name} ...")
    df = process_file(file_path)

    # Clean dtypes for Parquet
    df = clean_dataframe(df)

    # Save with same name but .parquet extension
    output_file = output_dir / f"{file_path.stem}.parquet"
    df.to_parquet(output_file, index=False)
    print(f"Saved {output_file}")
