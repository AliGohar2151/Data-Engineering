import time
from src.processing.events_normalization import run_normalization
from src.processing.session_identifier import run_session_identifier
from src.processing.pageview_identifier import run_pageview_identifier

if __name__ == "__main__":

    start_time = time.time()

    # run_normalization()
    run_session_identifier()
    # run_pageview_identifier()

    print(f"Execution time: {time.time() - start_time:.2f} seconds")
