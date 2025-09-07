import json
import pandas as pd
import pathlib
import time
import uuid

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
    of `session_id` and a per-session pageview counter.

    Logic:
    - Normalize `ep_page_location` → remove query params, fragments.
    - Skip invalid/missing URLs (empty strings, "nan", None).
    - Each time the page changes OR the session changes, increment the pageview counter.
    - Generate a deterministic UUIDv5 using (session_id + page_counter).
    - If `session_id` is missing, `pageview_id` = NA.

    Input:
    - A parquet file `events_session_identifier.parquet` containing GA4 events
      with session_id already assigned.

    Output:
    - A parquet file `events_pageview_identifier.parquet` with a new column `pageview_id`.
    - Columns are reordered: [event_date, event_timestamp, session_id, pageview_id, ...].
    """

    # Normalize URLs

    df["clean_page"] = [
        (
            url.split("?", 1)[0].split("#", 1)[0]
            if isinstance(url, str) and url not in ["", None, "nan"]
            else pd.NA
        )
        for url in df["ep_page_location"]
    ]

    df["is_new_pageview"] = (df["session_id"] != df["session_id"].shift()) | (
        df["clean_page"] != df["clean_page"].shift()
    )

    first_pageview_idx = (
        df[df["event_name"] == "page_view"]
        .groupby("session_id", sort=False)
        .head(1)
        .index
    )

    df.loc[first_pageview_idx, "is_new_pageview"] = df.loc[
        first_pageview_idx, "is_new_pageview"
    ] & (
        df.loc[first_pageview_idx, "clean_page"]
        != df.loc[first_pageview_idx].shift()["clean_page"]
    )

    mask_subsequent_pv = df["event_name"].eq("page_view") & ~df.index.isin(
        first_pageview_idx
    )
    df.loc[mask_subsequent_pv, "is_new_pageview"] = True
    df["pageview_counter"] = df.groupby("session_id")["is_new_pageview"].cumsum()
    df["pageview_id"] = [
        (
            str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{sid}_{int(cnt)}"))
            if pd.notna(sid) and pd.notna(cnt)
            else pd.NA
        )
        for sid, cnt in zip(df["session_id"], df["pageview_counter"])
    ]

    # Drop helper columns
    df.drop(columns=["clean_page", "is_new_pageview", "pageview_counter"], inplace=True)

    # Reorder columns
    cols = list(df.columns)
    desired_order = [
        "event_date",
        "event_timestamp",
        "session_id",
        "pageview_id",
        "event_name",
        "ep_page_location",
    ]
    new_order = desired_order + [c for c in cols if c not in desired_order]

    return df[new_order]


# --- Run ---
file_path = input_dir / "events_session_identifier.parquet"
df = pd.read_parquet(file_path)
df = make_pageview_id(df)
df.to_parquet(output_dir / "events_pageview_identifier.parquet", index=False)

print(f"Saved {output_dir / 'events_pageview_identifier.parquet'}")

end = time.time()
print(f"Execution time: {end - start:.2f} seconds")
