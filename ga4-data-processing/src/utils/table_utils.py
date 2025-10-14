import pandas as pd
import numpy as np


def build_table(df, mapping_rules, table_name):
    output_df = pd.DataFrame(index=df.index)
    if table_name == "Sessions":
        output_df = df[
            ["event_timestamp", "year", "month", "event_name", "session_id"]
        ].copy()
    elif table_name == "Pageviews":
        output_df = df[
            [
                "event_timestamp",
                "year",
                "month",
                "stream_id",
                "event_name",
                "session_id",
                "pageview_id",
            ]
        ].copy()
    elif table_name == "Events":
        output_df = df[
            [
                "event_timestamp",
                "year",
                "month",
                "event_name",
                "stream_id",
                "session_id",
                "pageview_id",
            ]
        ].copy()

    available_model_columns = set(df.columns)
    ep_columns = [c for c in df.columns if c.startswith("ep_")]
    event_name_arr = df["event_name"].values

    for entry in mapping_rules:
        if table_name not in entry:
            continue

        field_info = entry[table_name]
        field_title = field_info["title"]
        field_priority = field_info.get("field_priority", [])
        column_data_type = field_info.get("column_data_type")

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
        output_df[field_title] = pd.Series(values, index=df.index)
        if column_data_type:
            output_df[field_title] = convert_column_dtype(
                output_df[field_title], column_data_type, field_title
            )

    return output_df


def convert_column_dtype(series, dtype_name, field_title):
    dtype_name = str(dtype_name).lower().strip()

    try:
        if dtype_name == "integer":
            try:
                return series.astype("Int64")
            except Exception:
                print(f"Non-numeric value fount in {field_title}, keeping as string.")
                return series.astype("string")
        elif dtype_name == "float":
            return pd.to_numeric(series, errors="coerce")
        elif dtype_name == "string":
            return series.astype("string")
        elif dtype_name == "date":
            return pd.to_datetime(series, errors="coerce").dt.date
        elif dtype_name == "datetime":
            return pd.to_datetime(series, errors="coerce")
        else:
            print(
                f"Unknown data type {dtype_name} for column {field_title}, Skipping conversion."
            )
            return series
    except Exception as e:
        print(f"Failed to convert column {field_title} to {dtype_name}: {e}")
        return series.astype("string")


def groupby_sessions(df, time_col="event_timestamp", session_col="session_id"):
    grouped = (
        df.groupby(session_col)
        .agg(
            event_timestamp=(time_col, "first"),
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

    representative_col = ["pageview_id", "event_name", "event_timestamp"]

    firsts = df[representative_col].groupby(pageview_col).first()
    pageviews = (
        df.loc[df["event_name"] == "page_view", representative_col]
        .groupby(pageview_col)
        .first()
    )

    representative_rows = firsts.copy()
    representative_rows.update(pageviews)

    agg_dict = {
        col: (col, "first") for col in df.columns if col not in representative_col
    }
    agg_dict["pageview_start_time"] = ("event_timestamp", "min")
    agg_dict["pageview_end_time"] = ("event_timestamp", "max")
    agg_dict["total_events"] = ("event_timestamp", "count")

    grouped = df.groupby("pageview_id").agg(**agg_dict).reset_index()
    grouped = grouped.merge(
        representative_rows.reset_index(), on=pageview_col, how="left"
    )

    return grouped
