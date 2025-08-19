import pandas as pd
import os
import re
import hashlib
import duckdb

def normalize_text() -> None:
    """Clean, deduplicate, map tickers; write normalized records to data/lake/news_norm/."""

    input_path = 'data/lake/news_raw/news_raw.parquet'
    output_path = 'data/lake/news_norm'
    os.makedirs(output_path, exist_ok=True)

    if not os.path.exists(input_path):
        print(f"Raw news data not found at {input_path}. Skipping normalization.")
        return

    df_raw_news = pd.read_parquet(input_path)

    if df_raw_news.empty:
        print("Raw news data is empty. Skipping normalization.")
        return

    # Cleaning: lowercasing, Unicode normalization, URL stripping
    df_raw_news['title_clean'] = df_raw_news['title'].astype(str).str.lower()
    df_raw_news['description_clean'] = df_raw_news['description'].astype(str).str.lower()
    df_raw_news['content_clean'] = df_raw_news['content'].astype(str).str.lower()

    # Simple URL stripping (more robust regex might be needed for production)
    df_raw_news['title_clean'] = df_raw_news['title_clean'].apply(lambda x: re.sub(r'https?://\S+|www\.\S+', '', x))
    df_raw_news['description_clean'] = df_raw_news['description_clean'].apply(lambda x: re.sub(r'https?://\S+|www\.\S+', '', x))
    df_raw_news['content_clean'] = df_raw_news['content_clean'].apply(lambda x: re.sub(r'https?://\S+|www\.\S+', '', x))

    # Deduplication: hash of canonicalized title+source+ts
    # Create a unique hash for deduplication based on relevant fields
    def create_hash(row):
        # Use 'ts' (timestamp) and 'source' as they are directly available from NewsAPI, and cleaned title
        return hashlib.sha256(f"{row['ts']}{row['source']}{row['title_clean']}".encode()).hexdigest()

    df_raw_news['hash'] = df_raw_news.apply(create_hash, axis=1)
    df_deduplicated = df_raw_news.drop_duplicates(subset=['hash']).copy()

    # Ticker mapping: For MVP, we'll rely on the symbol passed during ingestion. 
    # In a real system, this would involve a more sophisticated NLP model or a comprehensive ticker mapping service.
    # We ensure the 'symbol' column exists and is consistent.
    df_deduplicated['symbol'] = df_deduplicated['symbol'].fillna('UNKNOWN') # Handle potential missing symbols

    # Select and rename columns to match the news_norm schema
    df_normalized = df_deduplicated[['ts', 'symbol', 'source', 'title', 'content_clean', 'url']].copy()
    df_normalized.rename(columns={'content_clean': 'text'}, inplace=True)

    # Write normalized records to data/lake/news_norm/
    df_normalized.to_parquet(os.path.join(output_path, 'news_norm.parquet'), index=False)
    print(f"Successfully normalized {len(df_normalized)} news articles.")

    # Register in DuckDB
    conn = duckdb.connect(database='./data/trading.duckdb', read_only=False)
    conn.execute("CREATE OR REPLACE VIEW news_norm AS SELECT * FROM parquet_scan('data/lake/news_norm/*.parquet');")
    conn.close()

if __name__ == "__main__":
    # This script expects data/lake/news_raw/news_raw.parquet to exist from ingest_news.py
    normalize_text()
