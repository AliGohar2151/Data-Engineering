import pandas as pd
import ast
import json
from .MariaDB import MariaDBClient


class MappingRules:
    def __init__(self, host, user, password, database):
        self.db = MariaDBClient(
            host=host, user=user, password=password, database=database
        )

    def load_data(self):
        query = """
        SELECT
            tf.id,
            tf.title as field_title,
            mt.title,
            tf.field_priority,
            ep.event_name,
            ep.ep
        FROM
            ga4_org_model_table_fields tf
        LEFT JOIN ga4_org_model_tables mt 
            ON tf.associated_table_id = mt.id
        LEFT JOIN ga4_org_model_event_parameters_types ept 
            ON tf.id = ept.table_field_id
        LEFT JOIN ga4_model_event_parameters ep 
            ON ept.id = ep.event_parameter_type_id;
        """

        df = pd.DataFrame(self.db.read_query(query))

        query_cols = "Select id,column_name from ga4_model_columns"
        model_col_df = pd.DataFrame(self.db.read_query(query_cols))

        return df, model_col_df

    def build_rules(self):

        df, model_col_df = self.load_data()

        df["field_priority"] = df["field_priority"].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) else x
        )

        col_dict = dict(zip(model_col_df["id"], model_col_df["column_name"]))

        combined_dict = {}

        for _, row in df.iterrows():
            key = (row["title"], row["field_title"])
            entry = combined_dict.setdefault(
                key, {"title": row["field_title"], "field_priority": []}
            )

            ep_entry = next(
                (
                    x
                    for x in entry["field_priority"]
                    if x["data_type"] == "event_parameter"
                ),
                None,
            )

            for fp in row["field_priority"]:
                if fp["data_type"] == "model_column":
                    column_id = fp["column_id"]
                    column_name = col_dict.get(int(column_id))

                    if not any(
                        c.get("column_id") == column_id
                        for c in entry["field_priority"]
                        if c["data_type"] == "model_column"
                    ):
                        entry["field_priority"].append(
                            {
                                "data_type": "model_column",
                                "column_id": column_id,
                                "column_name": column_name,
                            }
                        )

                elif fp["data_type"] == "event_parameter":
                    if not ep_entry:
                        ep_entry = {"data_type": "event_parameter", "rules": {}}
                        entry["field_priority"].append(ep_entry)
                    # Now safe
                    ep_entry["rules"][row["event_name"]] = row["ep"]

        final_json = [{table: info} for (table, _), info in combined_dict.items()]
        return final_json

    def save_rules(self, filepath="mapping_rules.json"):
        final_json = self.build_rules()
        with open(filepath, "w") as f:
            json.dump(final_json, f, indent=4)
        return filepath
