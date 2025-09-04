import json
import pandas as pd
import pathlib
import time
import uuid
from urllib.parse import urlparse

start = time.time()

base_dir = pathlib.Path(__file__).parent

input_dir = (
    base_dir / "../ga4-clean/session-identifier/analytics291746817/2024/10"
).resolve()
output_dir = (
    base_dir / "../ga4-clean/pageview-identifier/analytics291746817/2024/10"
).resolve()
output_dir.mkdir(parents=True, exist_ok=True)

print("Looking in:", input_dir)


def make_pageview_id(df):
    """
    Pageview Identifier Script
    --------------------------

    This script processes GA4 sessionized event data and generates a **pageview_id**
    for each event. The pageview identifier is constructed based on the combination
    of `session_id` and normalized `ep_page_location`.

    Logic:
    - Normalize `ep_page_location` → keep only scheme, netloc, and path
      (remove query params, fragments).
    - Skip invalid/missing URLs (empty strings, "nan", None).
    - Generate a deterministic UUIDv5 using (session_id + clean_page)
      so the same session-page combination always has the same pageview_id.
    - Events without a valid `session_id` or page location get `pageview_id = NA`.

    Input:
    - A parquet file `events_session_identifier.parquet` containing GA4 events
      with session_id already assigned.

    Output:
    - A parquet file `events_pageview_identifier.parquet` with a new column `pageview_id`.
    - Columns are reordered: [event_date, event_timestamp, session_id, pageview_id, ...].

    """

    df["clean_page"] = [
        (
            f"{urlparse(url).scheme}://{urlparse(url).netloc}{urlparse(url).path}"
            if isinstance(url, str) and url not in ["", None, "nan"]
            else pd.NA
        )
        for url in df["ep_page_location"]
    ]

    df["pageview_id"] = [
        (
            str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{sid}_{page}"))
            if pd.notna(sid) and page is not None
            else pd.NA
        )
        for sid, page in zip(df["session_id"], df["clean_page"])
    ]

    cols = list(df.columns)
    desired_order = ["event_date", "event_timestamp", "session_id", "pageview_id"]
    new_order = desired_order + [c for c in cols if c not in desired_order]
    df = df[new_order]
    df.drop("clean_page", axis=1, inplace=True)
    return df


file_path = input_dir / "events_session_identifier.parquet"
df = pd.read_parquet(file_path)
df = make_pageview_id(df)
df.to_parquet(output_dir / f"events_pageview_identifier.parquet", index=False)
print(f"Saved {output_dir / f'events_pageview_identifier.parquet'}")

end = time.time()
print(f"Execution time: {end - start:.2f} seconds")
