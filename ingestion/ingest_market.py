from typing import Iterable, Optional
import yfinance as yf
import pandas as pd
import duckdb
import os

def ingest_market(symbols: Iterable[str], start_date: str, end_date: Optional[str] = None, adjusted: bool = True) -> None:
    """Pull daily OHLCV (yfinance). Write Parquet to data/lake/ohlcv/{date}/{symbol}.parquet and register in DuckDB."""

    if end_date is None:
        end_date = pd.Timestamp.now().strftime('%Y-%m-%d')

    conn = duckdb.connect(database='./data/trading.duckdb', read_only=False)

    for symbol in symbols:
        print(f"Ingesting market data for {symbol} from {start_date} to {end_date}")
        data = yf.download(symbol, start=start_date, end=end_date, auto_adjust=adjusted)
        if not data.empty:
            data = data.reset_index()
            data['date'] = data['Date'].dt.strftime('%Y-%m-%d')
            data['symbol'] = symbol
            data = data[['date', 'symbol', 'Open', 'High', 'Low', 'Close', 'Volume']]
            data.columns = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']

            # Write to Parquet
            for date in data['date'].unique():
                date_path = os.path.join('data', 'lake', 'ohlcv', date)
                os.makedirs(date_path, exist_ok=True)
                file_path = os.path.join(date_path, f'{symbol}.parquet')
                data[data['date'] == date].to_parquet(file_path, index=False)

    # Register Parquet directories as views in DuckDB (assuming data/lake/ohlcv already exists)
    conn.execute("CREATE OR REPLACE VIEW ohlcv_daily AS SELECT * FROM parquet_scan('data/lake/ohlcv/**/*.parquet');")
    conn.close()

if __name__ == "__main__":
    # Example Usage:
    # Ensure 'data/lake/ohlcv' directory exists before running
    os.makedirs(os.path.join('data', 'lake', 'ohlcv'), exist_ok=True)
    ingest_market(symbols=["AAPL", "MSFT"], start_date="2023-01-01", end_date="2023-01-31")
