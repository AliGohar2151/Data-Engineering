import pandas as pd
import time
from src.rules.mapping_rules import MappingRules
from src.config.settings import DB_CONFIG
from src.config.paths import PROCESSED_DIR
from src.utils.io_handler import save_partitioned_parquet
from src.utils.table_utils import build_table, groupby_pageview, groupby_sessions
from src.utils.cleaner import clean_dataframe
from IPython.display import display


def process_table(df, mapping_rules, name):

    temp_df = build_table(df, mapping_rules, name)
    if name == "Sessions":
        temp_df = groupby_sessions(temp_df)
    elif name == "Pageviews":
        temp_df = groupby_pageview(temp_df)

    return temp_df


def run_build_tables():
    start = time.time()
    connection = MappingRules(**DB_CONFIG)
    mapping_rules = connection.build_rules()
    # connection.save_rules()

    input_dir = PROCESSED_DIR / "pageview-identifier/analytics_291746817/2024/10"
    input_path = input_dir / "events_pageview_identifier.parquet"

    output_dir = PROCESSED_DIR / "tables/analytics_291746817/2024/10"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Looking in: {input_dir}")
    df = pd.read_parquet(input_path)

    print(f"Processing Sessions table ...")
    sessions_df = process_table(df, mapping_rules, "Sessions")

    print(f"Processing Pageviews table ...")
    pageviews_df = process_table(df, mapping_rules, "Pageviews")

    print(f"Processing Events table ...")
    events_df = process_table(df, mapping_rules, "Events")
    events_df = clean_dataframe(events_df)

    print(f"Tables completed in {time.time() - start:.2f} seconds")

    print(f"Saving Sessions table ...")
    save_partitioned_parquet(
        sessions_df, output_dir / "sessions", ["Stream ID", "year", "month"]
    )

    print(f"Saving Pageviews table ...")
    save_partitioned_parquet(
        pageviews_df, output_dir / "pageviews", ["stream_id", "year", "month"]
    )

    print(f"Saving Events table ...")
    save_partitioned_parquet(
        events_df, output_dir / "events", ["stream_id", "year", "month"]
    )

    print(f"Tables saved in {time.time() - start:.2f} seconds")
