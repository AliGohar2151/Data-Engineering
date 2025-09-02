import json
import pandas as pd
import gzip
import pathlib
import time

start = time.time()

base_dir = pathlib.Path(__file__).parent

input_dir = (
    base_dir / "../ga4-raw-data/events-json/analytics_291746817/2024/10"
).resolve()
output_dir = (
    base_dir / "../ga4-clean/events-normalized/analytics291746817/2024/10"
).resolve()
output_dir.mkdir(parents=True, exist_ok=True)

print("Looking in:", input_dir)


def process_file(file_path):
    """Process one GA4 gzipped JSON file into a DataFrame."""
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        data = json.load(f)

    data_df = pd.json_normalize(data, sep="_")

    params_data = [
        {
            item["key"]: next(v for v in item["value"].values() if v is not None)
            for item in params_list
        }
        for params_list in data_df["event_params"]
    ]
    param_df = pd.DataFrame(params_data).add_prefix("ep_")

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

    event_date_dt = pd.to_datetime(data_df["event_date"], format="%Y%m%d")
    data_df["year"] = event_date_dt.dt.year
    data_df["month"] = event_date_dt.dt.month

    final_df = pd.concat([data_df, param_df, user_props_df], axis=1)
    final_df.drop(columns=["user_properties", "event_params"], inplace=True)

    return final_df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all columns have compatible types for Parquet."""
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)
    return df

    # df = process_file(
    #     "D:\\Data-Engineering\\ga4-data\\ga4-raw-data\\events-json\\analytics_291746817\\2024\\10\\events_20241001.json"
    # )
    # df = clean_dataframe(df)
    # df.to_parquet("output.parquet", index=False)
    print(f"Saved {output_file}")


for file_path in input_dir.glob("*.json"):
    print(f"Processing {file_path.name} ...")
    df = process_file(file_path)

    df = clean_dataframe(df)

    output_file = output_dir / f"{file_path.stem}.parquet"
    df.to_parquet(output_file, index=False)
    print(f"Saved {output_file}")

end = time.time()
print(f"Execution time: {end - start:.2f} seconds")
