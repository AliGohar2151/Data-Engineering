import pandas as pd
from typing import List, Optional, Union


def reorder_columns(
    df: pd.DataFrame,
    desired_order: Optional[List[str]] = None,
    insert_after: Optional[Union[str, List[str]]] = None,
    target_col: Optional[str] = None,
) -> pd.DataFrame:
    """
    Reorder DataFrame columns efficiently.

    Parameters
    ----------
    df : pd.DataFrame
    desired_order : list of str, optional
        Columns to appear first; others follow in original order.
    insert_after : str or list of str, optional
        Column(s) to move immediately after target_col.
    target_col : str, optional
        The column after which insert_after columns will be placed.

    Returns
    -------
    pd.DataFrame
        DataFrame with reordered columns.
    """
    cols = list(df.columns)

    # Apply desired order
    if desired_order:
        first_cols = [c for c in desired_order if c in cols]
        remaining_cols = [c for c in cols if c not in first_cols]
        cols = first_cols + remaining_cols

    # Apply insert_after for one or multiple columns
    if insert_after and target_col:
        if isinstance(insert_after, str):
            insert_after = [insert_after]
        insert_after = [c for c in insert_after if c in cols and target_col in cols]

        # Remove and insert each column in order
        for c in insert_after:
            cols.insert(cols.index(target_col) + 1, cols.pop(cols.index(c)))
            target_col = c  # Update target for sequential insertion

    return df[cols]
