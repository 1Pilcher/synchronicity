import os.path
import yfinance as yf
import pandas as pd
from tqdm import tqdm
import time

# Set up file paths, mostly not used, but generally useful
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
BASE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
SRC_DIR = os.path.join(BASE_DIR, "Source Data")
HIST_DIR = os.path.join(SRC_DIR, "History")
FLAGS_DIR = os.path.join(BASE_DIR, "Flags")
LOCK_DIR = os.path.join(BASE_DIR, "Locks")
EXTRA_DIR = os.path.join(BASE_DIR, "Extra")
SOUND_DIR = os.path.join(EXTRA_DIR, "sounds")
SOUND = os.path.join(SOUND_DIR, "success_jingle.wav")
FILTERED_TICKERS_PATH = os.path.join(SRC_DIR, "filtered_tickers.csv")
LATEST_PRICES_PATH = os.path.join(SRC_DIR, "latest_prices.csv")

# Read in Ticker list from latest_prices.csv
tickers = pd.read_csv(LATEST_PRICES_PATH)
tickers = tickers['Ticker'].tolist()

# Create the iterable function to get OHLC data
def fetch_and_save_history(ticker):
    data = yf.Ticker(ticker).history(period="7d", interval="1m")
    data = pd.DataFrame(data)
    data.reset_index(inplace=True)
    data = data[['Datetime', 'Volume', 'Open', 'High', 'Low', 'Close', 'Dividends', 'Stock Splits']]
    data.rename(columns={'Datetime': "Date"}, inplace=True)

    FILE = os.path.join(HIST_DIR, f"{ticker}_history.csv")
    data.to_csv(FILE)
    #print(data)

with tqdm(tickers, desc="Processing", ncols=200) as pbar:
    for ticker in tickers:
        pbar.set_description(f"{ticker}")
        fetch_and_save_history(ticker)
        pbar.update(1)
