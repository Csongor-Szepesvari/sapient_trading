from typing import List
import requests
import pandas as pd
import os

# Placeholder for NewsAPI key. Use environment variables in production.
NEWSAPI_API_KEY = os.environ.get("NEWSAPI_API_KEY")
NEWSAPI_BASE_URL = "https://newsapi.org/v2"

def ingest_news(symbols: List[str], start_ts: str, end_ts: str) -> None:
    """Fetch headlines (NewsAPI/Reddit). Write raw to news_raw/."""

    if not NEWSAPI_API_KEY:
        print("NEWSAPI_API_KEY environment variable not set. Skipping news ingestion.")
        return

    all_articles = []
    for symbol in symbols:
        print(f"Fetching news for {symbol} from {start_ts} to {end_ts}")
        try:
            # NewsAPI doesn't directly support symbol filtering, so we search by company name or a broader query.
            # For MVP, let's search by symbol for simplicity, acknowledging it's not perfect.
            params = {
                "q": symbol,
                "from": start_ts.split('T')[0],  # NewsAPI uses YYYY-MM-DD format
                "to": end_ts.split('T')[0],
                "sortBy": "relevancy",
                "apiKey": NEWSAPI_API_KEY,
                "language": "en",
                "pageSize": 100  # Max articles per request
            }
            response = requests.get(f"{NEWSAPI_BASE_URL}/everything", params=params)
            response.raise_for_status()
            articles = response.json().get('articles', [])

            for article in articles:
                all_articles.append({
                    "ts": pd.to_datetime(article.get('publishedAt')).isoformat(),
                    "symbol": symbol, # Assuming basic mapping or will be refined later
                    "source": article.get('source', {}).get('name'),
                    "title": article.get('title'),
                    "description": article.get('description'),
                    "url": article.get('url'),
                    "content": article.get('content')
                })

        except requests.exceptions.RequestException as e:
            print(f"Error fetching news for {symbol}: {e}")

    if all_articles:
        df = pd.DataFrame(all_articles)
        output_path = 'data/lake/news_raw'
        os.makedirs(output_path, exist_ok=True)
        # Save as a single parquet file for simplicity in MVP, partition by date later if needed.
        df.to_parquet(os.path.join(output_path, 'news_raw.parquet'), index=False)
        print(f"Successfully ingested {len(all_articles)} raw news articles.")

if __name__ == "__main__":
    # Example Usage:
    # os.environ["NEWSAPI_API_KEY"] = "YOUR_NEWSAPI_API_KEY"
    ingest_news(symbols=["AAPL", "MSFT"], start_ts="2023-01-01T00:00:00Z", end_ts="2023-01-02T23:59:59Z")
