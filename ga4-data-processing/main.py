import time
from src.processing.events_normalization import run_normalization
from src.processing.session_identifier import run_session_identifier
from src.processing.pageview_identifier import run_pageview_identifier
from src.db.mariadb_client import MariaDBClient
from src.config.settings import DB_CONFIG
from src.rules.mapping_rules import MappingRules

if __name__ == "__main__":

    start_time = time.time()

    run_normalization()
    run_session_identifier()
    run_pageview_identifier()

    print(f"Execution time: {time.time() - start_time:.2f} seconds")
