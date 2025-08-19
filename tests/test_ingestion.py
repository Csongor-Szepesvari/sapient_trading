import os
import pandas as pd
import duckdb
import pytest

# Adjusting the import path to access modules from the parent directory
# This assumes tests are run from the project root or poetry is configured correctly
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ingestion.ingest_market import ingest_market

# Define test parameters (symbols and date range)
TEST_SYMBOLS = ["AAPL"]
TEST_START_DATE = "2023-01-01"
TEST_END_DATE = "2023-01-05"

# Fixture to set up and tear down a temporary data directory and DuckDB connection
@pytest.fixture(scope="module")
def setup_teardown_data_environment():
    # Setup: Create a temporary directory for test data
    test_data_dir = "test_data"
    os.makedirs(os.path.join(test_data_dir, "lake", "ohlcv"), exist_ok=True)
    
    # Use a unique DuckDB file for testing to avoid conflicts
    test_duckdb_path = os.path.join(test_data_dir, "test_trading.duckdb")

    # Patch the ingest_market function to use the test paths
    original_duckdb_connect = duckdb.connect
    original_os_makedirs = os.makedirs
    original_os_path_join = os.path.join

    def mock_duckdb_connect(*args, **kwargs):
        return original_duckdb_connect(database=test_duckdb_path, read_only=kwargs.get('read_only', False))

    def mock_os_makedirs(name, exist_ok=False):
        if "data/lake/ohlcv" in name:
            name = name.replace("data/lake/ohlcv", os.path.join(test_data_dir, "lake", "ohlcv"))
        return original_os_makedirs(name, exist_ok=exist_ok)

    def mock_os_path_join(*args):
        # Modify paths to use test_data_dir only for data/lake related paths
        if "data/lake" in args[0]: # Check if the base path contains data/lake
            # Reconstruct the path relative to test_data_dir
            relative_path_parts = []
            found_data_lake = False
            for arg in args:
                if "data/lake" in arg:
                    found_data_lake = True
                    # Split the arg by 'data/lake/' and take the part after it
                    parts = arg.split('data/lake/', 1)
                    if len(parts) > 1: # If 'data/lake/' was found and there's a part after it
                        relative_path_parts.append(parts[1])
                    elif parts[0] == 'data/lake': # Case where arg is exactly 'data/lake'
                        pass # Don't add anything, it will be handled by test_data_dir/lake
                    else: # Handle paths like 'data' itself
                        relative_path_parts.append(arg)
                elif found_data_lake:
                    relative_path_parts.append(arg)
                else:
                    relative_path_parts.append(arg) # Keep other parts as is
            
            if found_data_lake:
                final_path = os.path.join(test_data_dir, "lake", *relative_path_parts)
            else: # If not related to data/lake, use original join
                final_path = original_os_path_join(*args)

            return final_path
        else:
            return original_os_path_join(*args)

    duckdb.connect = mock_duckdb_connect
    os.makedirs = mock_os_makedirs
    os.path.join = mock_os_path_join

    yield test_data_dir, test_duckdb_path

    # Teardown: Clean up the temporary directory and DuckDB file
    conn = duckdb.connect(database=test_duckdb_path, read_only=False)
    conn.close()
    
    # Restore original functions
    duckdb.connect = original_duckdb_connect
    os.makedirs = original_os_makedirs
    os.path.join = original_os_path_join

    # Clean up test data files and directories
    for root, dirs, files in os.walk(test_data_dir, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    os.rmdir(test_data_dir)

def test_ingest_market_creates_parquet_files(setup_teardown_data_environment):
    test_data_dir, test_duckdb_path = setup_teardown_data_environment

    # Run the ingestion function
    ingest_market(symbols=TEST_SYMBOLS, start_date=TEST_START_DATE, end_date=TEST_END_DATE)

    # Verify that Parquet files are created in the expected location
    expected_dir = os.path.join(test_data_dir, "lake", "ohlcv", TEST_START_DATE)
    assert os.path.exists(expected_dir)
    
    expected_file = os.path.join(expected_dir, f'{TEST_SYMBOLS[0]}.parquet')
    assert os.path.exists(expected_file)

    # Verify that data can be read from the Parquet file
    df_ingested = pd.read_parquet(expected_file)
    assert not df_ingested.empty
    assert "symbol" in df_ingested.columns
    assert (df_ingested['symbol'] == TEST_SYMBOLS[0]).all()

    # Verify that the DuckDB view is created and contains data
    conn = duckdb.connect(database=test_duckdb_path, read_only=True)
    df_from_duckdb = conn.execute(f"SELECT * FROM ohlcv_daily WHERE symbol = '{TEST_SYMBOLS[0]}'").fetchdf()
    conn.close()
    assert not df_from_duckdb.empty
    assert len(df_from_duckdb) == len(df_ingested)
    assert "symbol" in df_from_duckdb.columns
    assert (df_from_duckdb['symbol'] == TEST_SYMBOLS[0]).all()
