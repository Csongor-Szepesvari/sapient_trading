# Module: Execution and Evaluation

First-pass consolidation of section D content. We will refine later.

## D1. Execution
- Alpaca Paper integration — Trading API: https://alpaca.markets/docs/api-references/trading-api/
- Orders: market (MVP). Config for slippage assumptions in backtest.

Tech & Integration (first pass)
- How we use it: Paper execution for MVP; same signals→orders interface as backtest.

## D2. Backtesting
- Backtrader — Docs: https://www.backtrader.com/docu/
- Adapter: same signals → orders interface for both backtest and live.

Tech & Integration (first pass)
- How we use it: Execute generated orders; track trades and equity.

## D3. Evaluation & Tracking
- Metrics: return, vol, Sharpe, max DD, turnover; per‑asset PnL.
- MLflow experiment tracking — https://mlflow.org (equity curve, trade logs).
- Optional dashboard: Streamlit — https://streamlit.io

Tech & Integration (first pass)
- How we use it: Log metrics/artifacts; simple dashboard for visualization.

Deliverables (from doc)
- `exec/alpaca_client.py`, `exec/backtester.py`
- `eval/metrics.py`, `eval/report.ipynb` and `mlruns/` populated. 