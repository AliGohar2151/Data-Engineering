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


def make_session_id(df):
    """
    Generate session identifiers for GA4 event data and clean missing page locations.

    Rules for session creation:
    - A new session is created if the time difference between the current and previous
      event of the same user is > 30 minutes.
    - A new session is created for the first event of each user.
    - Each new session receives a unique UUIDv4 string as the session_id.
    - The same session_id is forward-filled to all subsequent rows until a new session starts.

    Additional functionality:
    - Converts event_timestamp from microseconds to pandas datetime.
    - Replaces invalid "nan" string values in ep_page_location with proper pd.NA.
    - Flags events where ep_page_location is missing or invalid (None, empty string, "nan").
    - Identifies and removes sessions where *all* page locations are missing by setting
      their session_id to pd.NA.
    - Within valid sessions, fills missing ep_page_location values using forward-fill
      and backward-fill so that gaps are filled with nearby valid values.
    - Drops temporary helper columns (previous_timestamp, time_diff, new_session, is_missing).
    - Reorders session_id to appear immediately after event_timestamp for readability.

    :param file_path: The path to a parquet file to read.
    :return: A pandas DataFrame with a session_id column.
    """

    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], unit="us")
    df["ep_page_location"] = df["ep_page_location"].replace("nan", pd.NA)
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

    df["is_missing"] = (
        df["ep_page_location"].isna()
        | (df["ep_page_location"] == "None")
        | (df["ep_page_location"] == "")
        | (df["ep_page_location"] == "nan")
    )

    all_missing_sessions = df.groupby("session_id")["is_missing"].all()
    bad_session_ids = all_missing_sessions[all_missing_sessions].index
    df.loc[df["session_id"].isin(bad_session_ids), "session_id"] = pd.NA

    # sessions_with_some_missing = df.groupby("session_id")["is_missing"].any()
    # sessions_with_some_missing = sessions_with_some_missing & ~all_missing_sessions
    # some_missing_ids = sessions_with_some_missing[sessions_with_some_missing].index
    # sessions_some_missing = df[df["session_id"].isin(some_missing_ids)]

    df["ep_page_location"] = df.groupby("session_id")["ep_page_location"].ffill()
    df["ep_page_location"] = df.groupby("session_id")["ep_page_location"].bfill()

    df.drop(
        columns=["previous_timestamp", "time_diff", "new_session", "is_missing"],
        inplace=True,
    )
    cols = list(df.columns)
    cols.insert(cols.index("event_timestamp") + 1, cols.pop(cols.index("session_id")))
    df = df[cols]

    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all columns have compatible types for Parquet."""
    if "ep_ga_session_id" in df.columns:
        df["ep_ga_session_id"] = df["ep_ga_session_id"].astype(str)
    return df


all_files = list(input_dir.glob("*.parquet"))
event_df = pd.concat([pd.read_parquet(f) for f in all_files], ignore_index=True)
df = make_session_id(event_df)
df = clean_dataframe(df)
df.to_parquet(output_dir / f"events_session_identifier.parquet", index=False)
print(f"Saved {output_dir / f'events_session_identifier.parquet'}")

end = time.time()
print(f"Execution time: {end - start:.2f} seconds")
