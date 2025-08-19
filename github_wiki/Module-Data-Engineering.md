# Module: Data Engineering

This note collects the current first-pass content for Data Engineering from the sprint/design doc. We will refine structure and details later.

## A1. Market Data APIs (prices/volume)
- yfinance (historical) via `yfinance` — GitHub: https://github.com/ranaroussi/yfinance
  - How up to date is Yahoo Finance?
    - Real time streaming quotes, as well as news.
  - Interface overview:
    - `Ticker`: single ticker data
    - `Tickers`: multiple tickers' data
    - `download`: download market data for multiple tickers
    - `Market`: market info
    - `WebSocket` / `AsyncWebSocket`: streaming data
    - `Search`: quotes and news from search
    - `Sector` / `Industry`: classification
    - `EquityQuery` / `Screener`: screening
  - Volume
    - yfinance returns OHLCV including `volume` (maps to `ohlcv_daily.volume`).
- Optional premium: Polygon.io — Docs: https://polygon.io/docs/stocks

- Ingestion target
  - `ingest_market.py` to pull OHLCV for a ticker list, store as Parquet partitioned by `{date}/{symbol}.parquet`, register to DuckDB.

## A2. Fundamentals
- Financial Modeling Prep (FMP) — Docs: https://site.financialmodelingprep.com/developer/docs
- SEC EDGAR (10‑K/10‑Q) — https://www.sec.gov/edgar
- Ingestion target
  - `ingest_fundamentals.py` to fetch quarterly fundamentals → normalized table per schema.
- Notes (first pass)
  - FMP: statements/ratios/metadata; cache/backfill offline; verify coverage.
  - SEC EDGAR: authoritative filings; respect rate limits; parsing later.

## A3. News / Social
- NewsAPI (headlines) — https://newsapi.org
- Reddit API (r/stocks, r/investing) — https://www.reddit.com/dev/api
- X/Twitter (if available) — https://developer.x.com
- Ingestion target
  - `ingest_news.py` with ticker mapping & dedup, and `normalize_text.py` for cleaning.
- Notes (first pass)
  - Normalize Text: lowercasing, Unicode normalization, URL stripping, deduplication.

## A4. Storage & Registry
- DuckDB file DB (`./data/trading.duckdb`) + Parquet lake at `./data/lake/`.
- Dataset registry `catalog.yml` and data validation with Great Expectations.
- Notes (first pass)
  - DuckDB: register Parquet directories as views; SQL for joins/rolling windows.
  - Parquet: zstd compression; partition by `{date}/{symbol}`; compact periodically.
  - GE: suites per dataset; nightly checks; fail on schema drift/nulls.

## Deliverables (from doc)
- `data/` with OHLCV, fundamentals, news partitions.
- `catalog.yml`; validation reports in `reports/data_quality/`. 

## Terminology and Glossary (first pass)
- **OHLCV**: Open, High, Low, Close, Volume (daily bars).
  - **Why it matters**: Base inputs for returns, momentum, volatility, and risk calculations.
  - **Where used**:
    - Returns: simple/log returns, rolling windows (e.g., 20D return r20)
    - Momentum: moving averages, RSI14, MACD
    - Risk: rolling volatility, ATR; volume for liquidity filters
  - **Data notes**:
    - Use adjusted close when computing returns to account for splits/dividends.
    - `volume` drives turnover and liquidity constraints.
- **Adjusted Close**: Close price adjusted for splits/dividends for consistent returns.
  - **What it means/how it's calculated**: 
  - **Why it matters**: Prevents spurious jumps in returns; enables consistent backtests.
  - **Where used**: All return/momentum features; benchmark performance calculations.
- **Corporate Actions**: Splits, dividends, symbol changes; require adjustment factors and mapping.
  - **What it means/how it's calculated**: Splits, dividends, symbol changes, mergers.
  - **Why it matters**: Affects price/volume history and ticker identity.
  - **Handling**: Prefer adjusted prices; maintain mapping tables for symbol changes.
- **Trading Day**: NYSE calendar session date.
  - **Why it matters**: Aligns features/decisions/evaluations to the same daily cut.
  - **Data notes**:
    - Store timestamps in UTC; cast to `date` for daily joins.
    - Source: Exchange calendars (e.g., NYSE).
- **Universe**: The configured set of symbols (e.g., S&P 500) driving ingestion scope.
  - **Why it matters**: Controls API costs, compute load, and liquidity assumptions.
  - **Data notes**:
    - Source/definition stored in config (YAML/CSV); versioned.
    - Used to parameterize all ingestion tasks.
- **Partitioning (Parquet)**: Directory layout like `{date}/{symbol}` for fast selective reads.
  - **Why it matters**: Enables predicate pushdown and reduces I/O at scale.
  - **Data notes**:
    - Choose partitions that match query patterns (date, symbol).
    - Consider compaction jobs to reduce small-file counts.
- **Dataset Registry**: `catalog.yml` describing datasets (URIs, schema, freshness SLA).
  - **Why it matters**: Single source of truth for dataset discovery and validation.
  - **Data notes**:
    - Used by loaders/validators; ensures consumers rely on contracts.
- **Data Contract**: Schema + constraints (types, nullability, keys) promised by each dataset.
  - **Why it matters**: Prevents drift and silent corruption across modules.
  - **Data notes**:
    - Enforced via validation suites and type checks before write.
- **Time Causality**: Ensure no feature uses data with a timestamp after the decision time.
  - **Why it matters**: Prevents leakage; ensures realistic evaluation.
  - **Data notes**:
    - Forward-only joins; windowing with inclusive lower bounds and exclusive uppers as needed.
- **Idempotency**: Re-running the same date/window produces identical outputs.
  - **Why it matters**: Reliability for scheduled and replayed runs.
  - **Data notes**:
    - Deduplicate by keys; write to temp paths then rename; checksum partitions if needed.

## Design Choices (Data Engineering)
- Frequency support:
  - MVP: daily bars and daily sentiment; ingestion designed for end-of-day processing.
  - Path to intraday: partitioning supports finer grains; optional Polygon aggregates; heavy NLP remains decoupled.
- Scalability:
  - Parquet + DuckDB for local analytics (columnar, vectorized, predicate pushdown).
  - Mitigate small-file proliferation with periodic compaction jobs.
  - Multi-user: Parquet lake as source of truth; avoid concurrent writes to `.duckdb` file.
- Cost considerations:
  - Storage: local Parquet is low-cost; optional cloud object storage later.
  - APIs: yfinance free tier for MVP; NewsAPI free tier limits; Polygon/FMP are paid (gated by config).
- Reliability and quality:
  - Great Expectations suites per dataset; nightly checks fail on schema drift/null anomalies.
  - Retries with backoff for API calls; cache responses; offline backfills to remove latency from daily runs.

## Module APIs (first pass)
- Ingestion APIs
```python
# ingestion/ingest_market.py
from typing import Iterable, Optional

def ingest_market(symbols: Iterable[str], start_date: str, end_date: Optional[str] = None, adjusted: bool = True) -> None:
    """Pull daily OHLCV (yfinance). Write Parquet to data/lake/ohlcv/{date}/{symbol}.parquet and register in DuckDB."""

# ingestion/ingest_fundamentals.py
def ingest_fundamentals(symbols: Iterable[str]) -> None:
    """Fetch quarterly statements (FMP). Normalize schema, write to data/lake/fundamentals, register in DuckDB."""

# ingestion/ingest_news.py
def ingest_news(symbols: list[str], start_ts: str, end_ts: str) -> None:
    """Fetch headlines (NewsAPI/Reddit). Write raw to news_raw/."""

# ingestion/normalize_text.py
def normalize_text() -> None:
    """Clean, deduplicate, map tickers; write normalized records to data/lake/news_norm/."""
```
- Loader/Access APIs
```python
# data/loaders.py
import pandas as pd
from typing import Iterable

def load_ohlcv(symbols: Iterable[str], start_date: str, end_date: str) -> pd.DataFrame: ...

def load_news_norm(symbols: Iterable[str], start_ts: str, end_ts: str) -> pd.DataFrame: ...

def register_views() -> None:
    """Create DuckDB views over Parquet directories for SQL access."""
```
- Registry/Contracts APIs
```python
# data/catalog.py
from typing import Dict, Any

def list_datasets() -> Dict[str, Any]: ...

def get_schema(dataset_name: str) -> Dict[str, Any]: ...
```

## Access Patterns (examples)
```sql
-- DuckDB SQL over Parquet views
CREATE VIEW ohlcv_daily AS SELECT * FROM parquet_scan('data/lake/ohlcv/**/**/*.parquet');
CREATE VIEW news_norm AS SELECT * FROM parquet_scan('data/lake/news_norm/**/*.parquet');
```
```python
# Join news to rolling returns for feature building
import duckdb
con = duckdb.connect('data/trading.duckdb')
con.execute(
    """
    WITH r20 AS (
      SELECT symbol, date,
             (close / lag(close, 20) OVER (PARTITION BY symbol ORDER BY date) - 1) AS r20
      FROM ohlcv_daily
    )
    SELECT n.ts::DATE AS date, n.symbol, r.r20
    FROM news_norm n
    LEFT JOIN r20 r ON r.symbol = n.symbol AND r.date = n.ts::DATE
    """
).df()
```

## Conventions
- Timestamps stored in UTC; `date` denotes trading day.
- No forward-fill at storage layer; downstream explicitly handles gaps.
- Symbol canonicalization maintained centrally; avoid over-matching in news mapping. 

## Glossary — Detailed

### OHLCV
- Definition: Open, High, Low, Close, Volume for each trading day.


### Adjusted Close


### Corporate Actions


### Trading Day
- Definition: NYSE calendar day; `date` is the trading session date.
- Why it matters: Aligns features/decisions/evaluations to the same daily cut.
- Handling: Store timestamps in UTC; cast to `date` for daily joins.

### Universe
- Definition: Configurable list of symbols in scope (e.g., S&P 500).
- Why it matters: Ingestion scope, capacity planning, API rate limits.
- Handling: Managed via config; used to parameterize all ingestion tasks.

### Partitioning (Parquet)
- Definition: Directory layout like `{date}/{symbol}` for fast selective reads.
- Why it matters: Predicate pushdown reduces I/O; essential for scale.
- Handling: Choose partition columns that match query patterns (date, symbol).

### Dataset Registry / Data Contracts
- Definition: `catalog.yml` enumerating dataset URIs, schemas, freshness policies.
- Why it matters: Enforces consistency across modules; enables validation and discoverability.
- Handling: All readers depend on the registry; validation checks against contracts.

### Time Causality
- Definition: No feature may include information later than the decision timestamp.
- Why it matters: Prevents leakage; ensures realistic backtests.
- Handling: Use forward-only joins; timestamp guards in pipelines; validation checks.

### Idempotency
- Definition: Re-running the same date range yields identical outputs.
- Why it matters: Reliability in scheduled runs and recovery workflows.
- Handling: Deterministic fetches; deduplication; atomic writes (temp + rename).

## Design Choices — Rationale (Data Engineering)

### Frequency Target: Daily first, intraday later
- Why: Balances signal freshness with cost/latency of NLP; 250 decisions/year support robust evaluation.
- How: Ingest daily bars; precompute NLP overnight; architecture allows finer partitions for future intraday.

### Storage: Parquet + DuckDB
- Why: Columnar performance, local analytics, minimal ops; ideal for research iteration.
- How: Partition Parquet by `date/symbol`; DuckDB views for joins/rolling windows; compaction jobs to reduce small files.
- Trade-offs: Not a multi-writer DB; coordinate write jobs; treat DuckDB as embedded analytics, not OLTP.

### Cost & Provider Strategy
- Why: Keep MVP free/low-cost; enable premium data via config when needed.
- How: yfinance + NewsAPI free tiers for baseline; gate Polygon/FMP behind config flags; cache aggressively to reduce calls.

### Quality & Reliability
- Why: Catch issues early; reproducible runs.
- How: Great Expectations suites per dataset; nightly checks; retries/backoff; offline backfills to decouple from API latency.

## News Storage and Integration

### Raw vs. Normalized
- Raw (`news_raw/`): Provider payloads stored as-is for auditability and reprocessing.
- Normalized (`news_norm/`): Contract schema
  - `{ts: timestamp (UTC), symbol: string, source: string, title: string, text: string, url: string}`
  - Keys/identity: `(symbol, ts, source, hash(title+source+ts))` used for dedup.
  - Indexing strategy: Partition by date (derived from `ts::DATE`) and optionally by symbol.

### Preprocessing
- Cleaning: lowercasing, Unicode normalization, URL/symbol stripping, stopword-light (preserve finance terms).
- Deduplication: hash of canonicalized `title+source+ts`.
- Ticker mapping: strict allowlist based on configured universe; fallback to company name match if needed.
- See also: [[Module-NLP-and-LLM-Agents]] for sentiment interface and downstream consumption.

### Integration Points
- Sentiment Agent reads `news_norm` for T‑1/T‑0 windows; outputs `news_sent`, `news_conf` to `features_daily` via the features pipeline.
- Features builder joins OHLCV and sentiment (e.g., r20 + news_sent) using daily `date` alignment.
- Backtester/Executor never writes into news tables; reads finalized features and signals.

## Module APIs — Additions (reads/writes)

```python
# data/loaders.py (reads)
from typing import Iterable
import pandas as pd

def load_fundamentals(symbols: Iterable[str], start_date: str, end_date: str) -> pd.DataFrame: ...

def load_features_daily(symbols: Iterable[str], start_date: str, end_date: str) -> pd.DataFrame: ...
```

```python
# data/writers.py (writes)
from pandas import DataFrame

def write_parquet_atomic(df: DataFrame, base_path: str, partition_cols: list[str]) -> None:
    """Write to a temp path then rename to ensure atomicity; enforce schema before write."""
```

## Access Patterns — Examples
```sql
-- Daily alignment join (news to returns) via DuckDB
WITH r20 AS (
  SELECT symbol, date,
         (close / lag(close, 20) OVER (PARTITION BY symbol ORDER BY date) - 1) AS r20
  FROM ohlcv_daily
)
SELECT n.ts::DATE AS date, n.symbol, r.r20, n.source, n.title
FROM news_norm n
LEFT JOIN r20 r ON r.symbol = n.symbol AND r.date = n.ts::DATE;
```

```python
# Loader usage pattern
features = load_features_daily(["AAPL","MSFT"], start_date="2023-01-01", end_date="2023-12-31")
```