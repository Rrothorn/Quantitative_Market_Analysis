# Quant Market Data Pipeline

End-to-end data engineering pipeline for ingesting, processing,
 and reporting financial market data across global equities.

The system automates:
- Data ingestion via Interactive Brokers API
- Incremental storage in PostgreSQL
- Feature engineering using SQL window functions
- Signal generation using pandas
- Automated PDF report generation

Designed as a modular, production-style pipeline.


## Architecture

                ┌────────────────────┐
                │ Interactive Brokers│
                │      (IB API)      │
                └─────────┬──────────┘
                          │
                          ▼
                ┌────────────────────┐
                │  Download Script   │
                │   (Python)         │
                └─────────┬──────────┘
                          │
                          ▼
                ┌────────────────────┐
                │   PostgreSQL DB    │
                │--------------------│
                │ ss_universe        │
                │ ss_daily           │
                │ ss_features        │
                │ ss_signals         │
                │ client_* tables    │
                └─────────┬──────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼                               ▼
┌────────────────────┐         ┌────────────────────┐
│ Feature Engineering│         │ Signal Generation  │
│ (SQL + Pandas)     │         │ (Pandas logic)     │
└─────────┬──────────┘         └─────────┬──────────┘
          │                              │
          └───────────────┬──────────────┘
                          ▼
                ┌────────────────────┐
                │   Report Script    │
                │ (HTML → PDF)       │
                └─────────┬──────────┘
                          ▼
                ┌────────────────────┐
                │   Client Reports   │
                │     (PDF)          │
                └────────────────────┘



## Pipeline Overview

1. **Ingestion Layer**
   - Connects to IB API
   - Downloads OHLCV data
   - Handles batching, rate limits, and failures
   - Performs incremental updates

2. **Storage Layer**
   - PostgreSQL database
   - Tables: ss_universe, ss_daily, ss_features, ss_signals

3. **Feature Engineering**
   - SQL-based transformations
   - Rolling metrics, returns, Donchian channels
   - State price calculation

4. **Signal Generation**
   - Market structure detection (weekly resampling)
   - Trend classification combining structure + price state

5. **Reporting**
   - Generates a PDF report per client
   - HTML templating
   - PDF export with conditional formatting

## 🗄️ Database Schema (Core Tables)

| Table               | Description                       |
| ------------------- | --------------------------------- |
| `ss_universe`       | Master list of tradable tickers   |
| `ss_daily`          | Historical OHLCV data             |
| `ss_signals`        | Computed signals and trend states |
| `client_list`       | Client metadata                   |
| `client_portfolios` | Mapping of clients to tickers     |

## Example: IB Data Ingestion

## 📑 Report Features

Each generated report includes:

* **Latest Market Overview** (per symbol)
* **Trend Change Detection** (latest signal shifts)
* Color-coded signals for quick interpretation
* Clean HTML → PDF layout (via `pdfkit`)
* Client-specific portfolio filtering

## Configuration

Pipeline is configurable via YAML:

```yaml
ingestion:
  countries: [US, JP, EU]


## Key Features

- Incremental data ingestion
- Upsert logic (PostgreSQL ON CONFLICT)
- Parallel API request handling
- Timeout and failure management
- Config-driven pipeline design
- Modular architecture
- Multi-tenant (multi-client) architecture
- HTML → PDF reporting automation
- Logging and error handling

## Challenges & Learnings

- Handling asynchronous IB API responses and request tracking
- Managing rate limits and incomplete data scenarios
- Ensuring data consistency across multiple global exchanges
- Combining SQL and pandas for efficient feature engineering
- Designing a pipeline structure for automation and scalability