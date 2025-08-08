import time
from edgar import get_current_filings, set_identity, CurrentFilings
from edgar._filings import get_current_entries_on_page
import os
from watchdog.events import FileSystemEventHandler
import hashlib
import argparse
from pathlib import Path
from watchdog.observers import Observer
from bs4 import BeautifulSoup
from ratelimit import limits, sleep_and_retry
import requests
from threading import Thread
from circuitbreaker import circuit

from utils import (
    connect_db,
    filing_exists,
    get_manager_by_cik,
    insert_filing,
    gather_holdings_from_soup,
    get_or_create_issuer,
    insert_holdings_batch,
)
from get_logging import get_logger
from sec_models import Manager

logger = get_logger(__name__)

# User Agent
set_identity(os.getenv("SEC_IDENTITY", "example1.company@access.com"))

HEALTHCHECK_URL = os.getenv("HEALTHCHECK_URL", "https://hc-ping.com/example")


class CikFileHandler(FileSystemEventHandler):
    def __init__(self, callback, cik_file_path):
        self.callback = callback
        self.cik_file_path = cik_file_path
        self.last_hash = self.get_file_hash()
        self.last_mtime = 0

    def check_for_changes(self):
        current_mtime = os.path.getmtime(self.cik_file_path)
        if current_mtime > self.last_mtime:
            self.last_mtime = current_mtime
            current_hash = self.get_file_hash()
            if current_hash != self.last_hash:
                self.last_hash = current_hash
                self.callback()
                logger.info("CIK file modified - reloading CIKs")

    def get_file_hash(self):
        with open(self.cik_file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()


@sleep_and_retry
@limits(calls=10, period=1)  # Limit to 10 calls per second
def process_filings(conn, seen: set, filings: CurrentFilings):
    for filing in filings:
        accession_number = filing.accession_number
        cik = filing.cik

        # if not CIK we care about, skip!
        if cik not in relevant_ciks:
            continue

        # if already dealt with, skip!
        if accession_number in seen:
            continue

        # check if filing already in database:
        if filing_exists(conn, accession_number):
            logger.info(f"Filing {accession_number} already exists in database, skip!")
            seen.add(accession_number)
            continue

        # Process the filing
        logger.info(f"Processing filing {accession_number} for CIK {cik}")

        manager = get_manager_by_cik(conn, cik)
        if not manager:
            logger.error("Manager not found for CIK: %s", cik)
            continue

        manager_id = manager.manager_id
        new_filing_id = insert_filing(conn, manager_id, filing)

        holding_inserted_count = 0

        # insert holdings
        # Looping through attachments to find XML files
        for attachment in filing.attachments:
            if (
                attachment.document.endswith('.xml')
                and 'primary_doc' not in attachment.document
            ):
                assert attachment.content

                soup = BeautifulSoup(attachment.content, 'xml')
                holdings = []

                for holding in gather_holdings_from_soup(conn, soup):

                    assert holding._issuer_cusip, "CUSIP is required for each holding"
                    assert (
                        holding._issuer_name
                    ), "Issuer name is required for each holding"

                    issuer = get_or_create_issuer(
                        conn,
                        holding._issuer_cusip,
                        holding._issuer_name,
                    )

                    holding.issuer_id = issuer.issuer_id
                    holding.filing_id = new_filing_id
                    holdings.append(holding)

                    # insert_holding(conn, holding)
                    holding_inserted_count += 1

                if holdings:
                    insert_holdings_batch(conn, holdings)

        logger.info(
            f"Inserted filling_id {new_filing_id} for {cik}. "
            f"Total holdings inserted: {holding_inserted_count}"
        )
        seen.add(accession_number)


def ping_healthchecks():
    while True:
        try:
            requests.get(HEALTHCHECK_URL, timeout=10)
        except Exception as e:
            logger.error(f"Healthcheck ping failed: {str(e)}")
        time.sleep(60)  # Ping every minute


@circuit(recovery_timeout=5)
def get_current_filings_with_circuit_breaker(form: str):
    return get_current_filings(form=form)


if __name__ == "__main__":
    health_thread = Thread(target=ping_healthchecks, daemon=True)
    health_thread.start()

    # keep track of seen filings to avoid duplicates
    seen_filings = set()

    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cik', default=os.getenv('CIK_FILE', "ciks.txt"), help="Path to CIK file"
    )
    args = parser.parse_args()

    logger.info(f"Reading: {args.cik}")
    with open(args.cik, "r") as f:
        ciks = f.read().splitlines()

    # Initialise database connection
    conn = connect_db(
        db_host=os.getenv("DB_HOST", "localhost"),
        db_port=os.getenv("DB_PORT", "5432"),
        db_user=os.getenv("DB_USER", "postgres"),
        db_password=os.getenv("DB_PASSWORD", ""),
        db_name=os.getenv("DB_NAME", "sec"),
    )

    state = {'current_ciks': [], 'conn': conn}

    def reload_ciks():
        with open(args.cik, "r") as f:
            state['current_ciks'] = [line.strip() for line in f if line.strip()]
        logger.info(f"Reloaded {len(state['current_ciks'])} CIKs")

    # Initial load
    reload_ciks()

    relevant_ciks = set(state['current_ciks'])

    # Set up file watcher
    event_handler = CikFileHandler(reload_ciks, args.cik)
    observer = Observer()
    watch_path = str(Path(args.cik).parent)
    logger.info(f"Watching for changes in {watch_path}")
    observer.schedule(event_handler, path=watch_path)
    observer.start()

    try:
        while True:
            event_handler.check_for_changes()

            get_current_entries_on_page.cache_clear()

            new_filings = get_current_filings(form="13F-HR")
            new_filings_a = get_current_filings(form="13F-HR/A")

            process_filings(conn, seen_filings, new_filings)
            process_filings(conn, seen_filings, new_filings_a)
            time.sleep(1)  # rate limit

    except KeyboardInterrupt:
        logger.info("Shutdown signal received, stopping observer...")
    finally:
        observer.stop()
        observer.join()

        state['conn'].close()
