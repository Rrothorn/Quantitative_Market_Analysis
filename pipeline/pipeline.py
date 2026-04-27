# -*- coding: utf-8 -*-
"""
Created on Tue Apr 21 19:54:58 2026

@author: Gebruiker
"""

import ib_market_data_ingestion
import create_ss_signals
import create_trend_report

from sqlalchemy import create_engine, text

import logging
import datetime
import os

    
# =========================
# LOGGING SETUP
# =========================
def setup_logging():
    os.makedirs("logs", exist_ok=True)

    log_filename = f"logs/pipeline_{datetime.now().strftime('%Y-%m-%d')}.log"

    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


# =========================
# DATA ACCESS
# =========================
def get_client_names(engine):
    query = text("""
        SELECT client_name
        FROM client_list
        WHERE status = 'active'
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        return [row[0] for row in result.fetchall()]


# =========================
# PIPELINE EXECUTION
# =========================
def run_pipeline():
    logging.info("Pipeline started")

    # Use environment variable (GitHub-safe)
    engine = create_engine(os.getenv("DB_CONNECTION_STRING"))

    try:
        logging.info("Step 1: Download data")
        ib_market_data_ingestion.run_ingestion_pipeline()

        logging.info("Step 2: Generate signals")
        create_ss_signals.run_signals()

        logging.info("Step 3: Fetch active clients")
        client_names = get_client_names(engine)

        logging.info(f"Generating reports for {len(client_names)} clients")

        for client_name in client_names:
            logging.info(f"Generating report for {client_name}")
            create_trend_report.run(client_name)

        logging.info("Pipeline completed successfully")

    except Exception:
        logging.exception("Pipeline failed")
        raise


# =========================
# ENTRYPOINT
# =========================
if __name__ == "__main__":
    setup_logging()
    run_pipeline()