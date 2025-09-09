import pandas as pd
import os
import shutil

import numpy as np


def build_table(df, mapping_rules, table_name):
    output_df = pd.DataFrame(index=df.index)
    if table_name == "Sessions":
        for col in ["event_timestamp", "year", "month", "event_name", "session_id"]:
            output_df[col] = df[col]
    elif table_name == "Pageviews":
        for col in [
            "event_timestamp",
            "year",
            "month",
            "stream_id",
            "event_name",
            "session_id",
            "pageview_id",
        ]:
            output_df[col] = df[col]
    elif table_name == "Events":
        for col in [
            "event_timestamp",
            "year",
            "month",
            "event_name",
            "session_id",
            "pageview_id",
            # "event_id",
        ]:
            output_df[col] = df[col]

    available_model_columns = set(df.columns)
    ep_columns = [c for c in df.columns if c.startswith("ep_")]
    event_name_arr = df["event_name"].values

    for entry in mapping_rules:
        if table_name not in entry:
            continue

        field_info = entry[table_name]
        field_title = field_info["title"]
        field_priority = field_info.get("field_priority", [])

        values = np.full(len(df), None, dtype=object)
        nan_mask = pd.isna(values)

        for r in field_priority:
            if not nan_mask.any():
                break
            if r.get("data_type") == "model_column":
                col = r.get("column_name")
                if col in available_model_columns:
                    values[nan_mask] = df[col].values[nan_mask]
                    nan_mask = pd.isna(values)
            elif r.get("data_type") == "event_parameter":
                rules_map = {ev: f"ep_{ep}" for ev, ep in r["rules"].items()}
                for ev_name, ep_col in rules_map.items():
                    if not nan_mask.any():
                        break
                    if ep_col in ep_columns:
                        event_mask = event_name_arr == ev_name
                        combined_mask = event_mask & nan_mask
                        values[combined_mask] = df[ep_col].values[combined_mask]
                        nan_mask = pd.isna(values)

        output_df[field_title] = pd.Series(values, index=df.index)

    return output_df


def groupby_sessions(df, time_col="event_timestamp", session_col="session_id"):
    grouped = (
        df.groupby(session_col)
        .agg(
            session_start_time=(time_col, "min"),
            session_end_time=(time_col, "max"),
            total_events=(time_col, "count"),
            **{
                col: (col, "first")
                for col in df.columns
                if col not in [time_col, session_col]
            },
        )
        .reset_index()
    )
    return grouped


def groupby_pageview(df, time_col="event_timestamp", pageview_col="pageview_id"):
    grouped = (
        df.groupby(pageview_col)
        .agg(
            pageview_start_time=(time_col, "min"),
            pageview_end_time=(time_col, "max"),
            total_events=(time_col, "count"),
            **{
                col: (col, "first")
                for col in df.columns
                if col not in [time_col, pageview_col]
            },
        )
        .reset_index()
    )

    return grouped


def groupby_events(df, time_col="event_timestamp", event_col="event_id"):
    grouped = (
        df.groupby(event_col)
        .agg(
            event_start_time=(time_col, "min"),
            event_end_time=(time_col, "max"),
            total_events=(time_col, "count"),
            **{
                col: (col, "first")
                for col in df.columns
                if col not in [time_col, event_col]
            },
        )
        .reset_index()
    )
    return grouped


import os
import shutil
import pandas as pd


def save_partitioned_parquet(df, base_path, partition_cols):
    """
    Save a DataFrame as partitioned parquet files.
    Deletes old partitions before writing.

    Args:
        df (pd.DataFrame): DataFrame to save
        base_path (str): Output directory
        partition_cols (list[str]): Columns to partition by
    """

    # Delete existing partitions for the values in df
    for _, row in df[partition_cols].drop_duplicates().iterrows():
        partition_path = base_path
        for col in partition_cols:
            partition_path = os.path.join(partition_path, f"{col}={row[col]}")
        if os.path.exists(partition_path):
            shutil.rmtree(partition_path)

    # Write parquet
    df.to_parquet(
        base_path,
        engine="pyarrow",
        index=False,
        partition_cols=partition_cols,
    )
