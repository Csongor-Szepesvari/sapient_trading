from typing import Iterable, Dict, Any
import requests
import pandas as pd
import duckdb
import os

# This is a placeholder for your FMP API key. In a real application, use environment variables.
FMP_API_KEY = os.environ.get("FMP_API_KEY")
BASE_URL = "https://financialmodelingprep.com/api/v3"

def ingest_fundamentals(symbols: Iterable[str]) -> None:
    """Fetch quarterly statements (FMP). Normalize schema, write to data/lake/fundamentals, register in DuckDB."""

    if not FMP_API_KEY:
        print("FMP_API_KEY environment variable not set. Skipping fundamentals ingestion.")
        return

    conn = duckdb.connect(database='./data/trading.duckdb', read_only=False)

    all_fundamentals_data = []

    for symbol in symbols:
        print(f"Fetching fundamentals for {symbol}")
        try:
            # Fetch income statement
            income_statement_url = f"{BASE_URL}/income-statement/{symbol}?period=quarter&apikey={FMP_API_KEY}"
            income_data = requests.get(income_statement_url).json()

            # Fetch balance sheet
            balance_sheet_url = f"{BASE_URL}/balance-sheet-statement/{symbol}?period=quarter&apikey={FMP_API_KEY}"
            balance_data = requests.get(balance_sheet_url).json()

            # Fetch cash flow statement
            cash_flow_url = f"{BASE_URL}/cash-flow-statement/{symbol}?period=quarter&apikey={FMP_API_KEY}"
            cash_flow_data = requests.get(cash_flow_url).json()

            # For simplicity, let's just combine some key fields. In a real scenario, this would be more robust.
            combined_data = {}
            for item in income_data:
                date = item.get("date")
                if date:
                    combined_data.setdefault(date, {}).update({
                        "symbol": symbol,
                        "date": date,
                        "revenue": item.get("revenue"),
                        "netIncome": item.get("netIncome"),
                        "eps": item.get("eps")
                    })
            for item in balance_data:
                date = item.get("date")
                if date:
                    combined_data.setdefault(date, {}).update({
                        "totalAssets": item.get("totalAssets"),
                        "totalLiabilities": item.get("totalLiabilities")
                    })
            for item in cash_flow_data:
                date = item.get("date")
                if date:
                    combined_data.setdefault(date, {}).update({
                        "cashFlowFromOperatingActivities": item.get("cashFlowFromOperatingActivities")
                    })
            
            for date_key, data_values in combined_data.items():
                all_fundamentals_data.append(data_values)

        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")

    if all_fundamentals_data:
        df = pd.DataFrame(all_fundamentals_data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(by=['symbol', 'date']).reset_index(drop=True)

        # Write to Parquet
        output_path = 'data/lake/fundamentals'
        os.makedirs(output_path, exist_ok=True)
        df.to_parquet(os.path.join(output_path, 'fundamentals.parquet'), index=False)

        # Register in DuckDB
        conn.execute("CREATE OR REPLACE VIEW fundamentals AS SELECT * FROM parquet_scan('data/lake/fundamentals/*.parquet');")

    conn.close()

if __name__ == "__main__":
    # Example Usage:
    # Set FMP_API_KEY environment variable before running
    # os.environ["FMP_API_KEY"] = "YOUR_FMP_API_KEY"
    ingest_fundamentals(symbols=["AAPL", "MSFT"])
