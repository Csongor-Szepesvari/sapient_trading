import backtrader as bt
import pandas as pd
import duckdb
import os
from typing import List

class CustomSizer(bt.Sizer): # Simple sizer for MVP
    params = (('stake', 1),)

    def _getsizing(self, comminfo, cash, data, isbuy):
        if isbuy:
            return self.p.stake
        return self.p.stake # Sell all if stake is 1, otherwise this logic needs refinement

class SimpleStrategy(bt.Strategy):
    params = (('stake', 1),
              ('long_threshold', 0.5),
              ('short_threshold', -0.5))

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()} {txt}')

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.order = None # To keep track of pending orders
        self.buys = []
        self.sells = []

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'BUY EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.2f}')
                self.buys.append(order.executed.price)
            elif order.issell():
                self.log(f'SELL EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.2f}')
                self.sells.append(order.executed.price)
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        self.order = None

    def next(self):
        if self.order:
            return

        # Get the aggregated signal for the current date and symbol
        current_date_str = self.data.datetime.date(0).isoformat()
        current_symbol = self.data._name

        # Fetch alpha for current symbol and date from DuckDB view
        conn = duckdb.connect(database='./data/trading.duckdb', read_only=True)
        query = f"SELECT alpha, side FROM aggregated_signals WHERE date = '{current_date_str}' AND symbol = '{current_symbol}'"
        result = conn.execute(query).fetchdf()
        conn.close()

        if not result.empty:
            alpha_score = result['alpha'].iloc[0]
            signal_side = result['side'].iloc[0]

            # Implement the simple rules:
            if signal_side == "BUY" and not self.position:
                self.log(f'BUY CREATE, {self.dataclose[0]:.2f} Alpha: {alpha_score:.2f}')
                self.order = self.buy(size=self.p.stake)
            elif signal_side == "SELL" and self.position:
                self.log(f'SELL CREATE, {self.dataclose[0]:.2f} Alpha: {alpha_score:.2f}')
                self.order = self.close()
        else:
            # If no signal for the day, remain in position or do nothing
            pass

def run_backtest(symbols: List[str], start_date: str, end_date: str, 
                 cash: float = 100000.0, commission: float = 0.001,
                 min_alpha_buy: float = 0.5, max_alpha_sell: float = -0.5) -> None:
    """Runs a backtest using Backtrader with data from DuckDB and signals from aggregated_signals."""
    cerebro = bt.Cerebro()
    cerebro.broker.setcash(cash)
    cerebro.broker.setcommission(commission=commission)

    # Add the strategy with parameters
    cerebro.addstrategy(SimpleStrategy, long_threshold=min_alpha_buy, short_threshold=max_alpha_sell)
    cerebro.addsizer(CustomSizer) # Add custom sizer

    # Add data feeds
    conn = duckdb.connect(database='./data/trading.duckdb', read_only=True)
    for symbol in symbols:
        query = f"SELECT date, open, high, low, close, volume FROM ohlcv_daily WHERE symbol = '{symbol}' AND date >= '{start_date}' AND date <= '{end_date}' ORDER BY date"
        df_ohlcv = conn.execute(query).fetchdf()
        if df_ohlcv.empty:
            print(f"No OHLCV data found for {symbol} in the specified date range. Skipping.")
            continue

        df_ohlcv['date'] = pd.to_datetime(df_ohlcv['date'])
        df_ohlcv = df_ohlcv.set_index('date')
        df_ohlcv.columns = [col.capitalize() for col in df_ohlcv.columns] # Backtrader expects capitalized columns

        data = bt.feeds.PandasData(
            dataname=df_ohlcv,
            fromdate=pd.to_datetime(start_date),
            todate=pd.to_datetime(end_date),
            name=symbol
        )
        cerebro.adddata(data)
    conn.close()

    if not cerebro.datas: # Check if any data feeds were added
        print("No data feeds added. Exiting backtest.")
        return

    print(f'Starting Portfolio Value: {cerebro.broker.getvalue():.2f}')
    cerebro.run()
    print(f'Final Portfolio Value: {cerebro.broker.getvalue():.2f}')

    # You can also get analysis from cerebro if needed
    # cerebro.plot()

if __name__ == "__main__":
    # Example Usage:
    # This assumes ingest_market.py and decision/aggregator_v0.py have been run
    # to populate ohlcv_daily and aggregated_signals in data/trading.duckdb
    
    # For a quick test, you might need to run the full data pipeline first:
    # 1. ingest_market.py (to get OHLCV)
    # 2. ingest_news.py (to get raw news)
    # 3. normalize_text.py (to get normalized news)
    # 4. agents/sentiment/finbert_agent.py (to get sentiment scores, then integrate into features)
    # 5. features/daily.py (to get r20, rsi14, and join sentiment)
    # 6. decision/aggregator_v0.py (to get alpha and signals)

    # Example dry-run for AAPL for a week
    # Ensure data/lake/ohlcv and data/lake/aggregated_signals exist and have data for this range
    # And data/trading.duckdb is populated.
    
    # If running in isolation, ensure dummy data exists for features_daily and ohlcv_daily
    # This part would typically be handled by a complete daily flow script.

    run_backtest(symbols=["AAPL"], start_date="2023-01-01", end_date="2023-01-07")
