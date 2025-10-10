import pandas as pd


def normalize_nested_params(df, column_name, prefix):
    data = []
    for items_list in df[column_name]:
        row_dict = {
            item["key"]: next(
                v
                for k, v in item["value"].items()
                if v is not None and k != "set_timestamp_micros"
            )
            for item in items_list
        }
        data.append(row_dict)

    return pd.DataFrame(data).add_prefix(prefix)
