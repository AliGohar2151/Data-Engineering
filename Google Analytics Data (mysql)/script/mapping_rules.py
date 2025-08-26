import pandas as pd
from sqlalchemy import create_engine
import json


def build_ga4_mapping(
    user="root", password="admin", host="localhost", port="3306", database="ga4_db"
):
    """
    Build GA4 mapping JSON from MySQL database.

    Args:
        user (str): MySQL username
        password (str): MySQL password
        host (str): MySQL host
        port (str): MySQL port
        database (str): MySQL database name

    Returns:
        list: JSON-like Python object (list of dicts)
    """
    try:
        # Connect to database
        engine = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        )
        engine.connect()

        # Load tables
        ga4_org_model_tables = pd.read_sql(
            "SELECT id, title, parent_model_table_id FROM ga4_org_model_tables", engine
        )
        ga4_org_model_table_fields = pd.read_sql(
            "SELECT title, associated_table_id, field_priority FROM ga4_org_model_table_fields",
            engine,
        )
        ga4_model_columns = pd.read_sql(
            "SELECT id, column_name FROM ga4_model_columns", engine
        )

        # Parse JSON in field_priority
        ga4_org_model_table_fields["field_priority"] = ga4_org_model_table_fields[
            "field_priority"
        ].apply(json.loads)

        # Merge
        merged = ga4_org_model_table_fields.merge(
            ga4_org_model_tables,
            left_on="associated_table_id",
            right_on="id",
            how="left",
        )
        col_map = dict(zip(ga4_model_columns["id"], ga4_model_columns["column_name"]))

        # Build JSON structure
        output = []
        for _, row in merged.iterrows():
            root = row["title_y"]
            field_priority = row["field_priority"]

            for fp in field_priority:
                if "column_id" in fp:
                    col_id = int(fp["column_id"])
                    fp["column_name"] = col_map.get(col_id, None)

            entry = {
                root: {
                    "title": row["title_x"],
                    "field_priority": row["field_priority"],
                    "resolved_column_name": row["title_x"].lower().replace(" ", "_"),
                }
            }
            output.append(entry)

        return output

    except Exception as e:
        raise Exception(f"Error building GA4 mapping: {e}")
