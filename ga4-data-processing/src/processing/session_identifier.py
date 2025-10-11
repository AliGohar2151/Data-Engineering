import pandas as pd
import time
import uuid
from src.config.paths import PARQUET_DIR, PROCESSED_DIR
from src.utils.io_handler import save_parquet

from src.utils.cleaner import clean_dataframe, clean_missing_values


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


def run_session_identifier():
    start = time.time()

    input_dir = PARQUET_DIR / "events-normalized/analytics_291746817/2024/10"
    output_dir = PROCESSED_DIR / "session-identifier/analytics_291746817/2024/10"
    output_path = output_dir / "events_session_identifier.parquet"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Looking in: {input_dir}")

    files = list(input_dir.glob("*.parquet"))
    if not files:
        print(f"No files found in {input_dir}")
        return

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    df = make_session_id(df)
    df = clean_dataframe(df)
    save_parquet(df, output_path)

    print(f"Session identifier completed in {time.time() - start:.2f} seconds")
