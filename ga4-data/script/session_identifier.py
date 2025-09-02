import json
import pandas as pd
import pathlib
import time
import uuid

start = time.time()

base_dir = pathlib.Path(__file__).parent

input_dir = (
    base_dir / "../ga4-clean/events-normalized/analytics291746817/2024/10"
).resolve()
output_dir = (
    base_dir / "../ga4-clean/session-identifier/analytics291746817/2024/10"
).resolve()
output_dir.mkdir(parents=True, exist_ok=True)

print("Looking in:", input_dir)


def make_session_id(file_path):
    """
    Makes a session id for each row in the dataframe, based on the following rules:

    - A new session is created if the time difference between the current and previous event is > 30 minutes.
    - A new session is created for the first event of each user.
    - The session id is a uuid4 string.

    The function sorts the dataframe by user_pseudo_id and event_timestamp, adds a session_id column, and returns the dataframe.

    :param file_path: The path to a parquet file to read.
    :return: A pandas DataFrame with a session_id column.
    """
    df = pd.read_parquet(file_path)

    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], unit="us")
    df = df.sort_values(["user_pseudo_id", "event_timestamp"])
    session_timeout = pd.Timedelta(minutes=30)
    df["previous_timestamp"] = df.groupby(["user_pseudo_id"])["event_timestamp"].shift()
    df["time_diff"] = pd.to_datetime(df["event_timestamp"]) - pd.to_datetime(
        df["previous_timestamp"]
    )
    df["new_session"] = (df["time_diff"] > session_timeout) | (
        df["previous_timestamp"].isnull()
    )
    df["session_id"] = df.groupby("user_pseudo_id")["new_session"].cumsum()
    df["session_id"] = df["new_session"].apply(
        lambda x: str(uuid.uuid4()) if x else None
    )
    df["session_id"] = df.groupby("user_pseudo_id")["session_id"].ffill()
    df.drop(columns=["previous_timestamp", "time_diff", "new_session"], inplace=True)
    cols = list(df.columns)
    cols.insert(cols.index("event_timestamp") + 1, cols.pop(cols.index("session_id")))
    df = df[cols]

    return df


for file_path in input_dir.glob("*.parquet"):
    print(f"Processing {file_path.name} ....")
    df = make_session_id(file_path)

    output_file = output_dir / f"{file_path.stem}.parquet"
    df.to_parquet(output_file, index=False)
    print(f"Saved {output_file}")

end = time.time()
print(f"Execution time: {end - start:.2f} seconds")
