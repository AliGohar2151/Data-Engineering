import pandas as pd
import os
import shutil


def map_rules_to_df(df, mapping_rules, table_name):
    """
    Map rules to build a DataFrame with columns defined by mapping_rules
    for a given table_name.
    Does NOT apply any grouping/aggregation — that must be done outside.
    """
    output_df = pd.DataFrame(index=df.index)

    for entry in mapping_rules:
        if table_name not in entry:
            continue

        field_info = entry[table_name]
        resolved_column_name = field_info.get(
            "resolved_column_name", field_info.get("title")
        )
        field_priority = field_info.get("field_priority", [])
        value_series = pd.Series([None] * len(df), index=df.index)

        for r in field_priority:
            if r.get("data_type") == "model_column":
                col = r.get("column_name")
                if col in df:
                    value_series = df[col]
                    break

        output_df[resolved_column_name] = value_series

        output_df["event_timestamp"] = df["event_timestamp"]
        output_df["year"] = df["year"]
        output_df["month"] = df["month"]

        if "Session" in table_name:
            output_df["session_id"] = df["session_id"]
        elif "Pageviews" in table_name:
            output_df["session_id"] = df["session_id"]
            output_df["pageview_id"] = df["pageview_id"]
            output_df["stream_id"] = df["stream_id"]
        elif "Events" in table_name:
            output_df["session_id"] = df["session_id"]
            output_df["pageview_id"] = df["pageview_id"]
            output_df["stream_id"] = df["stream_id"]
            output_df["event_id"] = df["event_id"]
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
            }
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
            }
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
            }
        )
        .reset_index()
    )
    return grouped


def save_partitioned_parquet(df, base_path):
    """
    Save DataFrame partitioned by stream_id, year, month.
    Overwrites existing parquet dataset each run.
    """
    if os.path.exists(base_path):
        shutil.rmtree(base_path)

    df.to_parquet(
        base_path,
        engine="pyarrow",
        index=False,
        partition_cols=["stream_id", "year", "month"],
    )
