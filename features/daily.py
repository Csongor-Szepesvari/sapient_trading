import pandas as pd
import duckdb
import os

def calculate_daily_features() -> None:
    """Calculate daily features like RSI14 and 20-day return, and write to data/lake/features/daily."""

    conn = duckdb.connect(database='./data/trading.duckdb', read_only=False)

    # Ensure ohlcv_daily and news_norm views are registered
    conn.execute("CREATE OR REPLACE VIEW ohlcv_daily AS SELECT * FROM parquet_scan('data/lake/ohlcv/**/*.parquet');")
    conn.execute("CREATE OR REPLACE VIEW news_norm AS SELECT * FROM parquet_scan('data/lake/news_norm/*.parquet');")

    # Calculate 20-day return (r20)
    # Using SQL for simplicity and leveraging DuckDB's window functions
    r20_query = """
    WITH r20_calc AS (
      SELECT 
        symbol, 
        date, 
        (close / LAG(close, 20) OVER (PARTITION BY symbol ORDER BY date) - 1) AS r20
      FROM ohlcv_daily
    )
    SELECT 
      CAST(date AS DATE) AS date, 
      symbol, 
      r20
    FROM r20_calc
    WHERE r20 IS NOT NULL
    """
    df_r20 = conn.execute(r20_query).fetchdf()

    # Calculate RSI14
    # RSI calculation is more complex in SQL, often easier in Python/Pandas
    # For simplicity, let's fetch OHLCV and calculate in pandas for now.
    # In a production system, this might be a more optimized DuckDB UDF or a specialized feature store.
    ohlcv_data = conn.execute("SELECT date, symbol, close FROM ohlcv_daily ORDER BY symbol, date").fetchdf()
    ohlcv_data['date'] = pd.to_datetime(ohlcv_data['date'])
    ohlcv_data = ohlcv_data.set_index(['symbol', 'date'])

    def calculate_rsi(series, window):
        diff = series.diff().dropna()
        gain = diff.mask(diff < 0, 0)
        loss = -diff.mask(diff > 0, 0)
        avg_gain = gain.ewm(com=window - 1, adjust=False).mean()
        avg_loss = loss.ewm(com=window - 1, adjust=False).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    df_rsi = ohlcv_data.groupby(level='symbol')['close'].apply(lambda x: calculate_rsi(x, 14)).reset_index()
    df_rsi.rename(columns={0: 'rsi14'}, inplace=True)

    # Merge r20 and rsi14
    df_features = pd.merge(df_r20, df_rsi, on=['date', 'symbol'], how='left')

    # Placeholder for news sentiment, which will be joined later
    # For now, let's assume news_sent and news_conf are not yet available from an agent
    # We'll join with news_norm if we want to include news features directly here for testing

    # Create the output directory if it doesn't exist
    output_dir = 'data/lake/features/daily'
    os.makedirs(output_dir, exist_ok=True)

    # Write to Parquet
    df_features.to_parquet(os.path.join(output_dir, 'features_daily.parquet'), index=False)
    print(f"Successfully calculated daily features and saved to {output_dir}/features_daily.parquet")

    # Register in DuckDB
    conn.execute("CREATE OR REPLACE VIEW features_daily AS SELECT * FROM parquet_scan('data/lake/features/daily/*.parquet');")
    conn.close()

if __name__ == "__main__":
    # Example Usage:
    # This script assumes that ingest_market.py has been run to populate data/lake/ohlcv
    calculate_daily_features()
