import pandas as pd
import json
from sqlalchemy import create_engine  # type: ignore


def build_ga4_mapping(
    user="root", password="admin", host="localhost", port="3306", database="ga4_db"
):
    try:
        engine = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        )
        engine.connect()
    except Exception as e:
        return e

    try:
        ga4_org_model_tables = pd.read_sql(
            "SELECT id, title, parent_model_table_id FROM ga4_org_model_tables", engine
        )
        ga4_model_columns = pd.read_sql(
            "SELECT id, column_name FROM ga4_model_columns", engine
        )
        ga4_org_model_table_fields = pd.read_sql(
            "SELECT id,title,associated_table_id,field_priority FROM ga4_org_model_table_fields",
            engine,
        )
        ga4_org_model_event_parameters_types = pd.read_sql(
            "SELECT id,event_type_id,table_field_id FROM ga4_org_model_event_parameters_types",
            engine,
        )
        ga4_model_event_parameters = pd.read_sql(
            "SELECT id,event_name,ep,event_parameter_type_id FROM ga4_model_event_parameters",
            engine,
        )
    except Exception as e:
        return e

    try:
        # Convert field_priority JSON strings into Python lists/dicts
        ga4_org_model_table_fields["field_priority"] = ga4_org_model_table_fields[
            "field_priority"
        ].apply(json.loads)

        # Map column_id -> column_name
        col_map = dict(zip(ga4_model_columns["id"], ga4_model_columns["column_name"]))

        output = []

        # Merge tables to align fields with their parent model tables
        mergerd = ga4_org_model_table_fields.merge(
            ga4_org_model_tables,
            how="left",
            left_on="associated_table_id",
            right_on="id",
        )

        for _, row in mergerd.iterrows():
            root = row["title_y"]  # Table name (e.g., "Sessions")
            field_priority = row["field_priority"]

            new_priority = []

            for fp in field_priority:
                if fp["data_type"] == "model_column" and "column_id" in fp:
                    # Replace column_id with actual column_name
                    col_id = int(fp["column_id"])
                    fp["column_name"] = col_map.get(col_id)
                    new_priority.append(fp)

                elif fp["data_type"] == "event_parameter":
                    # Find matching event parameter types for this field
                    field_id = row["id_x"]
                    param_type_ids = ga4_org_model_event_parameters_types.loc[
                        ga4_org_model_event_parameters_types["table_field_id"]
                        == field_id,
                        "id",
                    ].tolist()

                    # Fetch actual event_name + ep mappings
                    rules = ga4_model_event_parameters.loc[
                        ga4_model_event_parameters["event_parameter_type_id"].isin(
                            param_type_ids
                        ),
                        ["event_name", "ep"],
                    ].to_dict(orient="records")

                    # Instead of putting them under "rules", expand them as separate entries
                    for rule in rules:
                        new_priority.append(
                            {
                                "data_type": "event_parameter",
                                "event_name": rule["event_name"],
                                "ep": rule["ep"],
                            }
                        )

                else:
                    # For custom_function or other types
                    new_priority.append(fp)

            entry = {
                root: {
                    "title": row["title_x"],
                    "field_priority": new_priority,
                    "resolved_column_name": row["title_x"].lower().replace(" ", "_"),
                }
            }

            output.append(entry)

        return output

    except Exception as e:
        return e


# import pandas as pd
# import json
# from sqlalchemy import create_engine


# def build_ga4_mapping(
#     user="root", password="admin", host="localhost", port="3306", database="ga4_db"
# ):

#     try:
#         engine = create_engine(
#             f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
#         )
#         engine.connect()
#     except Exception as e:
#         return e

#     try:
#         ga4_org_model_tables = pd.read_sql(
#             "SELECT id, title, parent_model_table_id FROM ga4_org_model_tables", engine
#         )
#         ga4_model_columns = pd.read_sql(
#             "SELECT id, column_name FROM ga4_model_columns", engine
#         )
#         ga4_org_model_table_fields = pd.read_sql(
#             "SELECT id,title,associated_table_id,field_priority FROM ga4_org_model_table_fields",
#             engine,
#         )
#         ga4_org_model_event_parameters_types = pd.read_sql(
#             "SELECT id,event_type_id,table_field_id FROM ga4_org_model_event_parameters_types",
#             engine,
#         )
#         ga4_model_event_parameters = pd.read_sql(
#             "SELECT id,event_name,ep,event_parameter_type_id FROM ga4_model_event_parameters",
#             engine,
#         )
#     except Exception as e:
#         return e

#     try:
#         ga4_org_model_table_fields["field_priority"] = ga4_org_model_table_fields[
#             "field_priority"
#         ].apply(json.loads)

#         col_map = dict(zip(ga4_model_columns["id"], ga4_model_columns["column_name"]))

#         output = []

#         mergerd = ga4_org_model_table_fields.merge(
#             ga4_org_model_tables,
#             how="left",
#             left_on="associated_table_id",
#             right_on="id",
#         )

#         for _, row in mergerd.iterrows():
#             root = row["title_y"]
#             field_priority = row["field_priority"]

#             new_priority = []

#             for fp in field_priority:
#                 if fp["data_type"] == "model_column" and "column_id" in fp:
#                     col_id = int(fp["column_id"])
#                     fp["column_name"] = col_map.get(col_id)

#                 elif fp["data_type"] == "event_parameter":
#                     field_id = row["id_x"]
#                     param_type_ids = ga4_org_model_event_parameters_types.loc[
#                         ga4_org_model_event_parameters_types["table_field_id"]
#                         == field_id,
#                         "id",
#                     ].tolist()
#                     rules = ga4_model_event_parameters.loc[
#                         ga4_model_event_parameters["event_parameter_type_id"].isin(
#                             param_type_ids
#                         ),
#                         ["event_name", "ep"],
#                     ].to_dict(orient="records")
#                     fp["rules"] = rules

#                 new_priority.append(fp)

#             entry = {
#                 root: {
#                     "title": row["title_x"],
#                     "field_priority": new_priority,
#                     "resolved_column_name": row["title_x"].lower().replace(" ", "_"),
#                 }
#             }

#             output.append(entry)

#         return output

#     except Exception as e:
#         return e
