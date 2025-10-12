import pandas as pd
import numpy as np


def clean_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace all variants of missing values (nan, 'nan', 'None', '', etc.)
    with consistent pandas NA for easier processing.
    """
    df = df.replace(
        to_replace=["None", "none", "NaN", "nan", "NAN", "", "null", "Null", "NULL"],
        value=pd.NA,
    )
    df = df.replace({np.nan: pd.NA, None: pd.NA})
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if "ep_ga_session_id" in df.columns:
        df["ep_ga_session_id"] = df["ep_ga_session_id"].astype("string")
    if "ep_session_engaged" in df.columns:
        df["ep_session_engaged"] = df["ep_session_engaged"].astype("string")

    if "Event Text" in df.columns:
        df["Event Text"] = df["Event Text"].astype("string")

    return df


# def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
#     """Ensure all columns have compatible types for Parquet."""
#     for col in df.columns:
#         if df[col].dtype == "object":
#             df[col] = df[col].astype("str")
#     return df

# def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
#     """Ensure all columns have compatible types for Parquet."""
#     for col in df.columns:
#         if df[col].dtype == "object":
#             df[col] = df[col].astype("string")
#     return df
