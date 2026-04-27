 # -*- coding: utf-8 -*-

"""
IB Market Data Ingestion Module

- Connects to Interactive Brokers API
- Downloads historical daily OHLCV data
- Performs incremental updates
- Stores data in PostgreSQL (ss_daily)
- Handles failures and deactivates invalid tickers

Designed for batch processing across global equity markets.
"""

#=================================
# N.B.  For this script to run it requires IB's TraderWorkStation to be active
#================================

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading

import pandas as pd
import datetime
from datetime import timedelta

import os
import time

from sqlalchemy import create_engine, text
import yaml
import logging

logger = logging.getLogger(__name__)

# =========================
# IB API WRAPPER
# =========================
class IBApp(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = {}
        self.req_count = 0
        self.completed_requests = 0
        self.pending_requests = set()
        self.failed_tickers = []  # Store tickers that failed

    def historicalData(self, reqId, bar):
        if reqId not in self.data:
            self.data[reqId] = []
    
        self.data[reqId].append({
            "datetime": bar.date,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume
        })

    def historicalDataEnd(self, reqId, start, end):
        """Marks request completion."""
        self.completed_requests += 1
        logger.info(f"Data request {reqId} completed.")
        
    def error(self, reqId, errorCode, errorString):
        if reqId == -1:
            return
    
        fatal_errors = {200, 162, 354, 366}
    
        if errorCode in fatal_errors:
            print(f"Error {errorCode}: {errorString}")
    
            ticker = self.req_to_ticker.get(reqId, "Unknown")
            self.failed_tickers.append(ticker)
    
            self.completed_requests += 1

# =========================
# DOWNLOAD LOGIC
# =========================       
def create_contract(symbol):
    """Creates an IB contract for stocks."""
    contract = Contract()
    contract.symbol = symbol['symbol']
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = symbol['currency']
    contract.primaryExchange = symbol['exchange']
    return contract

def build_download_plan(engine, tickers):
    """Determine download range based on latest available data."""
    plan = []

    for ticker in tickers:
        last_date = get_last_date_for_symbol(engine, ticker['symbol'])

        if last_date:
            days_missing = (datetime.datetime.today() - last_date).days
            days_missing = days_missing + 3
            if days_missing < 1:
                continue
            duration = f"{days_missing} D"
        else:
            duration = "7 Y"
        
        plan.append((ticker, duration))

    return plan

def request_historical_data(app, download_plan, bar_size="1 day", max_parallel=10):
    """Requests historical data in parallel, checking for missing data first."""
    batch_size = min(len(download_plan), max_parallel)
    app.req_to_ticker = {}  # Map reqId to ticker

    for i, (ticker, duration) in enumerate(download_plan):

        contract = create_contract(ticker)
        app.req_to_ticker[i] = ticker  # Track ticker for error handling
        app.reqHistoricalData(reqId=i, contract=contract, endDateTime='',
                              durationStr=duration, barSizeSetting=bar_size,
                              whatToShow='ADJUSTED_LAST', useRTH=1, formatDate=1,
                              keepUpToDate=0, chartOptions=[])
        
        app.req_count += 1
        time.sleep(0.2)  

        # ---- FIXED WAIT LOGIC ----
        if (i + 1) % batch_size == 0 or i == len(download_plan) - 1:
            start_time = time.time()
            timeout = 90  # Maximum wait time (adjustable)
                
            while app.completed_requests < app.req_count:
                time.sleep(0.1)
            
                if time.time() - start_time > timeout:
                    logger.warning("Warning: Timeout reached, moving to next batch.")
                    break

# =========================
# DATA ACCESS (SQL)
# =========================                

def get_last_date_for_symbol(engine, ticker):
    """ Finds till when dataset is up to date. """
    query = text("""
        SELECT MAX(datetime)
        FROM ss_daily
        WHERE symbol = :ticker
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"ticker": ticker}).fetchone()

    if result[0] is None:
        return None

    return result[0]  # already datetime in Postgres!

def upsert_data(engine, df):
    ''' uploads data into db '''
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO ss_daily (
                    symbol, country_code, datetime,
                    open, high, low, close, volume
                )
                VALUES (
                    :symbol, :country_code, :datetime,
                    :open, :high, :low, :close, :volume
                )
                ON CONFLICT (symbol, datetime)
                DO UPDATE SET
                    country_code = EXCLUDED.country_code,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
            """), row.to_dict())
            
def deactivate_tickers(engine, failed_tickers):
    """ if a ticker is no longer existent it gets deactivated """
    if not failed_tickers:
        return
    
    query = text("""
        UPDATE ss_universe
        SET active = FALSE
        WHERE symbol = ANY(:symbols)
    """)
    
    with engine.begin() as conn:
        conn.execute(query, {"symbols": failed_tickers})
                
def save_data(app, engine):

    all_dfs = []

    for reqId, rows in app.data.items():
        ticker = app.req_to_ticker[reqId]

        df = pd.DataFrame(rows)
        df["symbol"] = ticker['symbol']
        df['country_code'] = ticker['country_code']

        all_dfs.append(df)

    if not all_dfs:
        logger.warning("No data to save.")
        return

    final_df = pd.concat(all_dfs, ignore_index=True)

    # Optional: clean datetime
    final_df["datetime"] = pd.to_datetime(final_df["datetime"])

    upsert_data(engine, final_df)

    logger.info(f"Saved {len(final_df)} rows.")
    
# =========================
# PIPELINE ENTRYPOINT
# =========================

# Batch request logic:
# IB API has rate limits and unstable response timing.
# This implementation ensures:
# - Controlled parallel requests
# - Timeout handling
# - Completion tracking via request counters

def load_config(path="config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def run_ingestion_pipeline():
    logger.info("Download script started")
    
    #Reads which data to download
    config = load_config()
    country_table = config["ingestion"]["countries"]
    
    # SQL DATABASE
    conn_str = os.getenv("DB_CONNECTION_STRING")
    if not conn_str:
        raise ValueError("DB_CONNECTION_STRING not set")
    
    engine = create_engine(conn_str)

    logger.info("Connected to database")
    
    ticker_query = text("""
        SELECT symbol,
               country_code,
               currency,
               exchange
        FROM ss_universe
        WHERE country_code = :country
        AND active = TRUE
        """)
    
    tickers = []
    for cc in country_table:    
        with engine.connect() as conn:
            result = conn.execute(ticker_query, {"country": cc} )
            rows = result.fetchall()
        
        tickers = tickers + [
            {
                "symbol": row[0],
                "country_code": row[1],
                "currency": row[2],
                "exchange": row[3]
            }
            for row in rows
        ]
    
    # Connect to Download Source IBAPI
    app = IBApp()
    app.connect('127.0.0.1', 7497, clientId=1)
    
    # Start IB API in a separate thread
    thread = threading.Thread(target=app.run, daemon=True)
    thread.start()
        
    download_plan = build_download_plan(engine, tickers)

    request_historical_data(app, download_plan)
    
    start_time = time.time()
    timeout = 300  # 5 minutes

    while app.completed_requests < app.req_count:
        time.sleep(0.1)
    
        if time.time() - start_time > timeout:
            logger.warning("Global timeout reached during download")
            break
        while app.completed_requests != app.req_count:
            time.sleep(0.1)

    if app.completed_requests == app.req_count:
        save_data(app, engine)
        
    app.disconnect()
    
    # Print failed tickers at the end
    failed_tickerlist = [
        ticker['symbol']
        for ticker in app.failed_tickers
    ]
    logger.info("\nThe following tickers failed and were skipped:")
    logger.info(", ".join(failed_tickerlist))
    
    deactivate_tickers(engine, failed_tickerlist)


