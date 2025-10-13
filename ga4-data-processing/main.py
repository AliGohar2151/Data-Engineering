import time
from src.processing.events_normalization import run_normalization
from src.processing.session_identifier import run_session_identifier
from src.processing.pageview_identifier import run_pageview_identifier
from src.processing.build_tables import run_build_tables
from src.rules.mapping_rules import MappingRules

if __name__ == "__main__":

    start_time = time.time()

    run_normalization()
    run_session_identifier()
    run_pageview_identifier()
    run_build_tables()

    print(f"\n\nExecution time: {time.time() - start_time:.2f} seconds")
