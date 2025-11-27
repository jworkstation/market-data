#!/usr/bin/env python3
"""
Download OHLCV Data Script

This script downloads OHLCV (Open, High, Low, Close, Volume) data for:
- BTCUSDT and ETHUSDT from Binance API
- XAUUSD from Yahoo Finance

Data can be downloaded for all assets or individually, with customizable date ranges.

Usage:
    # Download all assets with default date range (2025-01-01 to present)
    python download_ohlcv_2025.py

    # Download specific asset(s)
    python download_ohlcv_2025.py --asset btcusdt
    python download_ohlcv_2025.py --asset ethusdt --asset xauusd

    # Download with custom date range
    python download_ohlcv_2025.py --start 2025-01-15 --end 2025-03-01

    # Download specific asset with custom date range
    python download_ohlcv_2025.py --asset btcusdt --start 2025-02-01 --end 2025-02-28

Output files:
    - btcusdt_ohlcv_2025.csv
    - ethusdt_ohlcv_2025.csv
    - xauusd_ohlcv_2025.csv
"""

import argparse
import sys
from datetime import datetime

import pandas as pd
import yfinance as yf
from binance.client import Client
from binance.exceptions import BinanceAPIException


# Default date range
DEFAULT_START_DATE = "2025-01-01"
DEFAULT_END_DATE = datetime.now().strftime("%Y-%m-%d")

# Supported assets
SUPPORTED_ASSETS = ["btcusdt", "ethusdt", "xauusd"]

# Output file names
OUTPUT_FILES = {
    "btcusdt": "btcusdt_ohlcv_2025.csv",
    "ethusdt": "ethusdt_ohlcv_2025.csv",
    "xauusd": "xauusd_ohlcv_2025.csv",
}


def download_binance_ohlcv(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Download OHLCV data from Binance API for a given symbol.

    Args:
        symbol: Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format

    Returns:
        DataFrame containing OHLCV data with columns:
        [Open Time, Open, High, Low, Close, Volume, Close Time]

    Raises:
        BinanceAPIException: If there's an error with the Binance API
        Exception: For other unexpected errors
    """
    print(f"Downloading {symbol} data from Binance...")

    try:
        # Initialize Binance client (no API key needed for public data)
        client = Client()

        # Convert dates to milliseconds timestamp
        start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000)

        # Fetch historical klines (candlestick data)
        # Using daily interval for OHLCV data
        klines = client.get_historical_klines(
            symbol=symbol,
            interval=Client.KLINE_INTERVAL_1DAY,
            start_str=start_ts,
            end_str=end_ts
        )

        # Define column names for the klines data
        columns = [
            "Open Time",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "Close Time",
            "Quote Asset Volume",
            "Number of Trades",
            "Taker Buy Base Asset Volume",
            "Taker Buy Quote Asset Volume",
            "Ignore"
        ]

        # Create DataFrame from klines data
        df = pd.DataFrame(klines, columns=columns)

        # Convert timestamp to datetime
        df["Open Time"] = pd.to_datetime(df["Open Time"], unit="ms")
        df["Close Time"] = pd.to_datetime(df["Close Time"], unit="ms")

        # Convert numeric columns to appropriate types
        numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Select only the OHLCV columns
        df = df[["Open Time", "Open", "High", "Low", "Close", "Volume"]]

        print(f"Successfully downloaded {len(df)} records for {symbol}")
        return df

    except BinanceAPIException as e:
        print(f"Binance API error for {symbol}: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error downloading {symbol}: {e}")
        raise


def download_yahoo_ohlcv(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Download OHLCV data from Yahoo Finance for a given symbol.

    Args:
        symbol: Yahoo Finance symbol (e.g., 'GC=F' for Gold futures)
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format

    Returns:
        DataFrame containing OHLCV data with columns:
        [Date, Open, High, Low, Close, Volume]

    Raises:
        Exception: If there's an error downloading data
    """
    print(f"Downloading {symbol} data from Yahoo Finance...")

    try:
        # Create ticker object
        ticker = yf.Ticker(symbol)

        # Download historical data
        df = ticker.history(start=start_date, end=end_date)

        if df.empty:
            raise ValueError(f"No data returned for {symbol}")

        # Reset index to have Date as a column
        df = df.reset_index()

        # Rename columns for consistency
        df = df.rename(columns={"Date": "Open Time"})

        # Select only OHLCV columns
        df = df[["Open Time", "Open", "High", "Low", "Close", "Volume"]]

        # Remove timezone info from datetime for cleaner CSV output
        df["Open Time"] = df["Open Time"].dt.tz_localize(None)

        print(f"Successfully downloaded {len(df)} records for {symbol}")
        return df

    except Exception as e:
        print(f"Error downloading {symbol} from Yahoo Finance: {e}")
        raise


def save_to_csv(df: pd.DataFrame, filename: str) -> None:
    """
    Save DataFrame to a CSV file.

    Args:
        df: DataFrame to save
        filename: Output filename
    """
    try:
        df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
    except Exception as e:
        print(f"Error saving to {filename}: {e}")
        raise


def parse_arguments():
    """
    Parse command-line arguments.

    Returns:
        Namespace object with parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Download OHLCV data for BTCUSDT, ETHUSDT, and XAUUSD",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Download all assets with default date range (2025-01-01 to present)
    python download_ohlcv_2025.py

    # Download specific asset(s)
    python download_ohlcv_2025.py --asset btcusdt
    python download_ohlcv_2025.py --asset ethusdt --asset xauusd

    # Download with custom date range
    python download_ohlcv_2025.py --start 2025-01-15 --end 2025-03-01

    # Download specific asset with custom date range
    python download_ohlcv_2025.py --asset btcusdt --start 2025-02-01 --end 2025-02-28
        """
    )

    parser.add_argument(
        "--asset", "-a",
        action="append",
        choices=SUPPORTED_ASSETS,
        help="Asset to download (can be specified multiple times). "
             "Options: btcusdt, ethusdt, xauusd. "
             "If not specified, downloads all assets."
    )

    parser.add_argument(
        "--start", "-s",
        type=str,
        default=DEFAULT_START_DATE,
        help=f"Start date in YYYY-MM-DD format (default: {DEFAULT_START_DATE})"
    )

    parser.add_argument(
        "--end", "-e",
        type=str,
        default=DEFAULT_END_DATE,
        help=f"End date in YYYY-MM-DD format (default: {DEFAULT_END_DATE})"
    )

    return parser.parse_args()


def validate_date(date_str: str) -> bool:
    """
    Validate that a date string is in YYYY-MM-DD format.

    Args:
        date_str: Date string to validate

    Returns:
        True if valid, raises ValueError if invalid
    """
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD format.")


def download_asset(asset: str, start_date: str, end_date: str) -> bool:
    """
    Download OHLCV data for a specific asset.

    Args:
        asset: Asset name (btcusdt, ethusdt, or xauusd)
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        True if download was successful, False otherwise
    """
    try:
        if asset == "btcusdt":
            df = download_binance_ohlcv("BTCUSDT", start_date, end_date)
        elif asset == "ethusdt":
            df = download_binance_ohlcv("ETHUSDT", start_date, end_date)
        elif asset == "xauusd":
            # Using 'GC=F' which is the Gold futures symbol on Yahoo Finance
            df = download_yahoo_ohlcv("GC=F", start_date, end_date)
        else:
            print(f"Unknown asset: {asset}")
            return False

        save_to_csv(df, OUTPUT_FILES[asset])
        return True

    except Exception as e:
        print(f"Failed to download {asset}: {e}")
        return False


def main():
    """
    Main function to download and save OHLCV data for specified assets.
    """
    # Parse command-line arguments
    args = parse_arguments()

    # Validate dates
    try:
        validate_date(args.start)
        validate_date(args.end)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Determine which assets to download
    assets_to_download = args.asset if args.asset else SUPPORTED_ASSETS

    print("=" * 60)
    print("OHLCV Data Downloader")
    print(f"Period: {args.start} to {args.end}")
    print(f"Assets: {', '.join(assets_to_download)}")
    print("=" * 60)

    # Track success/failure
    success_count = 0
    total_count = len(assets_to_download)

    # Download each asset
    for i, asset in enumerate(assets_to_download):
        if i > 0:
            print("-" * 60)

        if download_asset(asset, args.start, args.end):
            success_count += 1

    print("=" * 60)
    print(f"Download complete: {success_count}/{total_count} successful")
    print("=" * 60)

    # Exit with error code if any downloads failed
    if success_count < total_count:
        sys.exit(1)


if __name__ == "__main__":
    main()
