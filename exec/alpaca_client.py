import os
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
import pandas as pd
from typing import List

class AlpacaClient:
    def __init__(self):
        self.api_key = os.environ.get("ALPACA_API_KEY")
        self.secret_key = os.environ.get("ALPACA_SECRET_KEY")
        self.paper = True # Always use paper trading for this MVP

        if not self.api_key or not self.secret_key:
            raise ValueError("ALPACA_API_KEY and ALPACA_SECRET_KEY environment variables must be set.")

        self.trading_client = TradingClient(self.api_key, self.secret_key, paper=self.paper)

    def place_market_order(self, symbol: str, qty: float, side: str) -> dict:
        """Places a market order for a given symbol, quantity, and side (BUY/SELL)."""
        if side.upper() == "BUY":
            order_side = OrderSide.BUY
        elif side.upper() == "SELL":
            order_side = OrderSide.SELL
        else:
            raise ValueError(f"Invalid order side: {side}. Must be 'BUY' or 'SELL'.")

        market_order_data = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=order_side,
            time_in_force=TimeInForce.DAY # Orders are good for the day
        )

        try:
            order = self.trading_client.submit_order(market_order_data)
            print(f"Placed {side} order for {qty} shares of {symbol}. Order ID: {order.id}")
            return order.dict()
        except Exception as e:
            print(f"Error placing order for {symbol}: {e}")
            return {"error": str(e)}

    def get_account_information(self) -> dict:
        """Retrieves account information."""
        try:
            account = self.trading_client.get_account()
            return account.dict()
        except Exception as e:
            print(f"Error fetching account information: {e}")
            return {"error": str(e)}

    def get_open_positions(self) -> List[dict]:
        """Retrieves all open positions."""
        try:
            positions = self.trading_client.get_all_positions()
            return [p.dict() for p in positions]
        except Exception as e:
            print(f"Error fetching open positions: {e}")
            return {"error": str(e)}

if __name__ == "__main__":
    # Example Usage:
    # Set ALPACA_API_KEY and ALPACA_SECRET_KEY environment variables before running
    # os.environ["ALPACA_API_KEY"] = "YOUR_ALPACA_API_KEY"
    # os.environ["ALPACA_SECRET_KEY"] = "YOUR_ALPACA_SECRET_KEY"

    try:
        alpaca_client = AlpacaClient()

        # Get account information
        account_info = alpaca_client.get_account_information()
        if "error" not in account_info:
            print("Account Information:")
            print(f"Cash: {account_info.get('cash')}")
            print(f"Equity: {account_info.get('equity')}")
        
        # Place a dummy buy order (ensure you have enough buying power)
        # For testing, ensure these symbols are valid and you're using paper trading.
        # order_response = alpaca_client.place_market_order(symbol="AAPL", qty=1, side="BUY")
        # print("Order Response:", order_response)

        # Get open positions
        # open_positions = alpaca_client.get_open_positions()
        # print("Open Positions:", open_positions)

    except ValueError as e:
        print(f"Configuration Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
