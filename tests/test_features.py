import os
import pandas as pd
import duckdb
import pytest
import numpy as np

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from features.daily import calculate_daily_features

@pytest.fixture(scope="module")
def setup_features_environment():
    test_data_dir = "test_features_data"
    os.makedirs(os.path.join(test_data_dir, "lake", "ohlcv"), exist_ok=True)
    os.makedirs(os.path.join(test_data_dir, "lake", "news_norm"), exist_ok=True)
    os.makedirs(os.path.join(test_data_dir, "lake", "features", "daily"), exist_ok=True)
    
    test_duckdb_path = os.path.join(test_data_dir, "test_trading.duckdb")

    # Create dummy OHLCV data
    dates = pd.date_range(start="2023-01-01", periods=30, freq='D')
    ohlcv_data = {
        'date': dates,
        'symbol': ['AAPL'] * 30,
        'open': np.random.rand(30) * 100 + 100,
        'high': np.random.rand(30) * 100 + 105,
        'low': np.random.rand(30) * 100 + 95,
        'close': np.random.rand(30) * 100 + 100,
        'volume': np.random.randint(100000, 1000000, 30)
    }
    df_ohlcv = pd.DataFrame(ohlcv_data)
    df_ohlcv_path = os.path.join(test_data_dir, "lake", "ohlcv", "ohlcv.parquet")
    df_ohlcv.to_parquet(df_ohlcv_path, index=False)

    # Create dummy news_norm data (simplified)
    news_data = {
        'ts': dates.map(lambda x: x.isoformat() + "T10:00:00Z"),
        'symbol': ['AAPL'] * 30,
        'source': ['test_source'] * 30,
        'title': [f'news {i}' for i in range(30)],
        'text': [f'news text {i}' for i in range(30)],
        'url': [f'http://test.com/{i}' for i in range(30)],
    }
    df_news_norm = pd.DataFrame(news_data)
    df_news_norm_path = os.path.join(test_data_dir, "lake", "news_norm", "news_norm.parquet")
    df_news_norm.to_parquet(df_news_norm_path, index=False)

    original_duckdb_connect = duckdb.connect

    def mock_duckdb_connect(*args, **kwargs):
        conn = original_duckdb_connect(database=test_duckdb_path, read_only=kwargs.get('read_only', False))
        # Register views directly in the test connection
        conn.execute(f"CREATE OR REPLACE VIEW ohlcv_daily AS SELECT * FROM parquet_scan('{df_ohlcv_path.replace(os.sep, '/')}');")
        conn.execute(f"CREATE OR REPLACE VIEW news_norm AS SELECT * FROM parquet_scan('{df_news_norm_path.replace(os.sep, '/')}');")
        return conn

    duckdb.connect = mock_duckdb_connect
    
    # Yield the test environment details
    yield test_data_dir, test_duckdb_path

    # Teardown
    duckdb.connect = original_duckdb_connect # Restore original connect

    for root, dirs, files in os.walk(test_data_dir, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    os.rmdir(test_data_dir)

def test_calculate_daily_features(setup_features_environment):
    test_data_dir, test_duckdb_path = setup_features_environment

    calculate_daily_features()

    expected_features_file = os.path.join(test_data_dir, "lake", "features", "daily", "features_daily.parquet")
    assert os.path.exists(expected_features_file)

    df_features = pd.read_parquet(expected_features_file)
    assert not df_features.empty
    assert "r20" in df_features.columns
    assert "rsi14" in df_features.columns
    assert "date" in df_features.columns
    assert "symbol" in df_features.columns

    # Verify data in DuckDB view
    conn = duckdb.connect(database=test_duckdb_path, read_only=True)
    df_from_duckdb = conn.execute("SELECT * FROM features_daily").fetchdf()
    conn.close()

    assert not df_from_duckdb.empty
    assert len(df_from_duckdb) == len(df_features)
    assert "r20" in df_from_duckdb.columns
    assert "rsi14" in df_from_duckdb.columns
