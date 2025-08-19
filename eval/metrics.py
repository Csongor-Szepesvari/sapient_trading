import pandas as pd
import numpy as np
import mlflow
from typing import Optional

def calculate_metrics(portfolio_value_series: pd.Series) -> dict:
    """Calculates key trading performance metrics from a portfolio value series."""

    if portfolio_value_series.empty or len(portfolio_value_series) < 2:
        return {"total_return": 0, "sharpe_ratio": 0, "max_drawdown": 0, "annualized_volatility": 0}

    # Calculate daily returns
    returns = portfolio_value_series.pct_change().dropna()

    # Total Return
    total_return = (portfolio_value_series.iloc[-1] / portfolio_value_series.iloc[0]) - 1

    # Annualized Volatility (assuming daily data, 252 trading days in a year)
    annualized_volatility = returns.std() * np.sqrt(252)

    # Sharpe Ratio (assuming risk-free rate is 0 for simplicity in MVP)
    # (Mean daily return - risk_free_rate) / Std Dev of daily return * sqrt(252)
    sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(252) if returns.std() != 0 else 0

    # Max Drawdown
    # Calculate the running maximum
    running_max = portfolio_value_series.cummax()
    # Calculate the daily drawdown
    drawdown = (portfolio_value_series / running_max) - 1.0
    # Calculate the maximum drawdown
    max_drawdown = drawdown.min()

    return {
        "total_return": total_return,
        "annualized_volatility": annualized_volatility,
        "sharpe_ratio": sharpe_ratio,
        "max_drawdown": max_drawdown
    }

def log_metrics_to_mlflow(metrics: dict, equity_curve_df: Optional[pd.DataFrame] = None, run_name: str = "trading_run"):
    """Logs calculated metrics and optionally the equity curve to MLflow."""
    with mlflow.start_run(run_name=run_name) as run:
        print(f"MLflow Run ID: {run.info.run_id}")
        mlflow.log_metrics(metrics)

        if equity_curve_df is not None and not equity_curve_df.empty:
            # Save equity curve as an artifact
            equity_curve_path = "equity_curve.csv"
            equity_curve_df.to_csv(equity_curve_path, index=False)
            mlflow.log_artifact(equity_curve_path)
            print(f"Equity curve logged as artifact: {equity_curve_path}")

if __name__ == "__main__":
    # Example Usage:
    # Simulate a portfolio value series
    dates = pd.date_range(start='2023-01-01', periods=100, freq='D')
    portfolio_values = pd.Series(np.random.rand(100).cumsum() + 1000, index=dates)

    # Calculate metrics
    metrics = calculate_metrics(portfolio_values)
    print("Calculated Metrics:", metrics)

    # Log to MLflow
    equity_curve_df = pd.DataFrame({'Date': portfolio_values.index, 'PortfolioValue': portfolio_values.values})
    log_metrics_to_mlflow(metrics, equity_curve_df, run_name="example_daily_run")
