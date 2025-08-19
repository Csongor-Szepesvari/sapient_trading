from prefect import flow, task
from datetime import date, timedelta
from typing import List
import os

# Import the functions from our modules
from ingestion.ingest_market import ingest_market
from ingestion.ingest_fundamentals import ingest_fundamentals
from ingestion.ingest_news import ingest_news
from ingestion.normalize_text import normalize_text
from features.daily import calculate_daily_features
from agents.sentiment.finbert_agent import FinbertSentimentAgent # Assuming direct use for now
from decision.aggregator_v0 import aggregate_signals
from exec.backtester import run_backtest # For backtesting mode
from exec.alpaca_client import AlpacaClient # For paper trading mode
from eval.metrics import calculate_metrics, log_metrics_to_mlflow

# Placeholder for a universe of symbols. In a real system, this would be loaded from config.
DEFAULT_SYMBOLS = ["AAPL", "MSFT"]

@task
def run_ingestion(symbols: List[str], start_date_str: str, end_date_str: str):
    print(f"Running ingestion for {symbols} from {start_date_str} to {end_date_str}")
    ingest_market(symbols=symbols, start_date=start_date_str, end_date=end_date_str)
    ingest_fundamentals(symbols=symbols) # Fundamentals are usually less frequent, but included for completeness
    ingest_news(symbols=symbols, start_ts=f"{start_date_str}T00:00:00Z", end_ts=f"{end_date_str}T23:59:59Z")
    normalize_text()
    print("Ingestion complete.")

@task
def run_feature_engineering():
    print("Calculating daily features...")
    calculate_daily_features()
    print("Feature engineering complete.")

@task
def run_sentiment_analysis(target_date: date):
    print("Running sentiment analysis...")
    # In a full pipeline, this task would fetch news_norm data for the target_date
    # and write sentiment scores back to the features store.
    # For this MVP, let's assume news_norm is loaded internally by the agent example or features join it.
    # As per Module-Data-Engineering, sentiment agent output goes to features_daily.
    
    # For direct usage within the flow, we might need to load and pass data explicitly.
    # Since the current `finbert_agent.py` example directly loads from `news_norm.parquet`
    # and `features/daily.py` expects it to be joined, we'll simplify this task for the flow.
    print("Sentiment analysis task is a placeholder. FinBERT agent runs as part of feature building pipeline conceptually.")

@task
def run_decision_making():
    print("Aggregating signals and making decisions...")
    aggregate_signals()
    print("Decision making complete.")

@task
def run_execution(mode: str, symbols: List[str], start_date_str: str, end_date_str: str):
    if mode == "backtest":
        print(f"Running backtest for {symbols} from {start_date_str} to {end_date_str}...")
        run_backtest(symbols=symbols, start_date=start_date_str, end_date=end_date_str)
        print("Backtest complete.")
    elif mode == "paper":
        print("Executing paper trades...")
        # In a real scenario, you'd load the daily signals and execute orders
        # For MVP, this is a placeholder.
        alpaca_client = AlpacaClient()
        account_info = alpaca_client.get_account_information()
        print(f"Alpaca Account Cash: {account_info.get('cash')}")
        print("Paper trading execution logic goes here.")
    else:
        print(f"Unknown execution mode: {mode}")

@task
def run_evaluation():
    print("Running evaluation and logging metrics...")
    # To get the equity curve for evaluation, backtester needs to return it.
    # For simplicity, assume a way to get portfolio values from backtest results or a simulated run.
    # In a real setup, backtrader would generate a Cerebro object with trades/portfolio_value.
    
    # For MVP, we'll simulate some data for demonstration purposes or expect it from a prior backtest run
    # that might have saved it. For this flow, we need to adapt `run_backtest` to return portfolio value.
    print("Evaluation task is a placeholder. Metrics and equity curve logging from backtest output.")
    
    # Example: If run_backtest could return equity_curve_series
    # metrics = calculate_metrics(equity_curve_series)
    # log_metrics_to_mlflow(metrics, equity_curve_df=pd.DataFrame({'Date': equity_curve_series.index, 'PortfolioValue': equity_curve_series.values}))

@flow(name="Daily Trading Pipeline")
def daily_trading_pipeline(run_date: date = date.today(), mode: str = "backtest", symbols: List[str] = DEFAULT_SYMBOLS):
    print(f"Starting daily trading pipeline for {run_date} in {mode} mode.")
    
    # Define start and end dates for data ingestion and backtesting
    # For daily runs, ingest T-1 to T-0 (or a small window around run_date)
    # For MVP, let's assume a fixed window for initial data population
    ingestion_start_date = (run_date - timedelta(days=30)).isoformat() # Last 30 days for fresh data
    ingestion_end_date = run_date.isoformat()
    
    backtest_start_date = (run_date - timedelta(days=7)).isoformat() # Last 7 days for dry run
    backtest_end_date = run_date.isoformat()

    run_ingestion(symbols=symbols, start_date_str=ingestion_start_date, end_date_str=ingestion_end_date)
    run_feature_engineering()
    run_sentiment_analysis(target_date=run_date)
    run_decision_making()
    run_execution(mode=mode, symbols=symbols, start_date_str=backtest_start_date, end_date_str=backtest_end_date)
    run_evaluation()

    print(f"Daily trading pipeline for {run_date} completed.")

if __name__ == "__main__":
    # To run this flow, ensure Prefect is installed and a Prefect server is running (prefect server start)
    # Then deploy the flow: prefect deploy ./flows/daily_run.py:daily_trading_pipeline -n daily-trading-pipeline
    # Then run: prefect run daily-trading-pipeline
    
    # For local testing without a Prefect server (just runs Python functions directly)
    daily_trading_pipeline(run_date=date(2023, 1, 31), mode="backtest", symbols=["AAPL", "MSFT"])
