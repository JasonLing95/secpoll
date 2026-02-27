import time
from edgar import get_current_filings, set_identity, CurrentFilings, httpclient
from edgar.current_filings import get_current_entries_on_page
import os
import argparse
from ratelimit import limits, sleep_and_retry
import requests
from threading import Thread
from lxml import etree  # type: ignore
import logging
import httpx

from utils import (
    connect_db,
    filing_exists,
    insert_filing,
    gather_holdings_using_lxml,
    gather_data_with_bs4,
    find_namespaces,
    insert_holdings_batch,
    publish_to_firehose,
)
from get_logging import get_logger, configure_logging
from models import SecFilingSchema

configure_logging(level=logging.INFO)
logger = get_logger(__name__)

HEALTHCHECK_URL = os.getenv("HEALTHCHECK_URL", "https://hc-ping.com/example")


# Disable caching
httpclient.CACHE_DIRECTORY = None  # type: ignore

# Close the client to apply changes
httpclient.close_clients()


@sleep_and_retry
@limits(calls=10, period=1)  # Limit to 10 calls per second
def process_filings(conn, seen: set, filings: CurrentFilings, relevant_ciks: set):
    logger.info(f"Processing {len(filings)} filings")

    for filing in filings:
        accession_number = filing.accession_number
        cik = filing.cik

        # sec.gov CIKS have leading zeros, but our database does not
        trimmed_cik = str(cik).lstrip("0")

        # if not CIK we care about, skip!
        if trimmed_cik not in relevant_ciks:
            logger.info(f"Skipping filing {accession_number} for CIK {cik}")
            continue

        # if already dealt with, skip!
        if accession_number in seen:
            continue

        # check if filing already in database:
        if filing_exists(conn, accession_number):
            logger.info(f"Filing {accession_number} exists in database, skip!")
            seen.add(accession_number)
            continue

        # must be a new filing we care about

        # Process the filing
        logger.info(f"Processing filing {accession_number} for CIK {cik}")

        # raise Exception if no company found (should not happen)
        _ = insert_filing(conn, trimmed_cik, filing)

        for attachment in filing.attachments:
            if (
                attachment.document.endswith(".xml")
                and "primary_doc" not in attachment.document
            ):
                logger.info(f"Processing XML attachment: {attachment.url}")
                assert attachment.content

                if isinstance(attachment.content, str):
                    # Convert string to bytes
                    xml_content = attachment.content.encode("utf-8")
                else:
                    xml_content = attachment.content

                try:
                    tree = etree.fromstring(xml_content)
                    nsmap = tree.nsmap
                    ns = {
                        "ns": (
                            nsmap[None]
                            if None in nsmap
                            else (
                                nsmap["ns1"]
                                if "ns1" in nsmap
                                else nsmap[find_namespaces(tree)[0]]
                            )
                        )
                    }
                    info_tables = tree.xpath("//ns:infoTable", namespaces=ns)
                    raw_holdings = gather_holdings_using_lxml(
                        info_tables,
                        ns,
                        trimmed_cik,
                        accession_number,
                    )
                except:
                    # try parsing with BeautifulSoup
                    raw_holdings = gather_data_with_bs4(
                        attachment.content,
                        trimmed_cik,
                        accession_number,
                    )

                if raw_holdings:
                    insert_holdings_batch(conn, raw_holdings)

        try:
            logger.info(f"Publishing new filing {accession_number} to firehose...")
            filing_schema = SecFilingSchema.from_filing(filing)
            print(filing_schema.json())

            publish_to_firehose(filing_schema)

        except Exception as e:
            logger.error(f"Failed to publish {accession_number} to firehose: {e}")

        seen.add(accession_number)

    return None


def ping_healthchecks():
    while True:
        try:
            requests.get(HEALTHCHECK_URL, timeout=10)
        except Exception as e:
            logger.error(f"Healthcheck ping failed: {str(e)}")
        time.sleep(60)  # Ping every minute


def main():

    logger.info("--- SCRIPT EXECUTION STARTED ---")

    # Initialize your application
    health_thread = Thread(target=ping_healthchecks, daemon=True)
    health_thread.start()
    logger.debug("Healthcheck thread started.")

    seen_filings = set()

    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cik", default=os.getenv("CIK_FILE", "ciks.txt"), help="Path to CIK file"
    )
    args = parser.parse_args()

    logger.info(f"Reading: {args.cik}")

    # initialize database connection
    conn = connect_db()

    # Read CIKs from file
    try:
        with open(args.cik, "r") as f:
            # need to zfill to 10 digits
            # because CIKs from sec.gov have leading zeros
            ciks = [line.strip() for line in f.read().splitlines()]

            import random

            logger.info(f"Random CIK: {random.choice(ciks)}")

            relevant_ciks = set(ciks)
        logger.info(f"Loaded {len(relevant_ciks)} CIKs")
    except FileNotFoundError:
        logger.critical(f"CIK file not found at {args.cik}. Exiting.")
        exit(1)

    logger.info("--- Entering main processing loop ---")

    try:
        while True:
            try:
                # Clear the cache to ensure fresh data is fetched
                get_current_entries_on_page.cache_clear()

                new_filings = get_current_filings(form="13F-HR")
                logger.info(f"Fetched {len(new_filings)} new filings")
                new_filings_a = get_current_filings(form="13F-HR/A")
                logger.info(f"Fetched {len(new_filings_a)} new filings")
                new_filings_aa = get_current_filings(form="13F-HR/A/A")
                logger.info(f"Fetched {len(new_filings_aa)} new filings")

                process_filings(conn, seen_filings, new_filings, relevant_ciks)
                process_filings(conn, seen_filings, new_filings_a, relevant_ciks)
                process_filings(conn, seen_filings, new_filings_aa, relevant_ciks)

                logger.info("Sleeping for 15 seconds")
                time.sleep(15)

            except httpx.ReadTimeout:
                logger.warning(
                    "SEC Network Timeout (httpx.ReadTimeout). Backing off for 60 seconds..."
                )
                time.sleep(60)
                continue  # Skips back to the top of the while loop

            except Exception as e:
                logger.error(f"Unexpected error during fetch loop: {str(e)}")
                # Even for other random errors, don't crash. Just wait and retry.
                logger.info("Backing off for 60 seconds before retrying...")
                time.sleep(60)
                continue

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise e


if __name__ == "__main__":
    try:
        set_identity(os.getenv("SEC_IDENTITY", "c90d56807e6b.company@access.com"))
        main()
    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
        exit(1)  # Force a non-zero exit code so Docker knows it crashed
    finally:
        logger.warning("Script reached the end of execution unexpectedly!")
