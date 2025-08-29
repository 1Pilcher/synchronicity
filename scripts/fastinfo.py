import json
import yfinance as yf
import pandas as pd
import time
from tqdm import tqdm
import shutil
import argparse
import os
from datetime import datetime

# CLI setup
parser = argparse.ArgumentParser(description="Fetch latest stock prices.")
parser.add_argument('--commit', action='store_true', help='Overwrite old_prices.csv with latest data')
args = parser.parse_args()

# ---- PATH SETUP ----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))     # e.g. Market/scripts
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))  # Market folder

def rel(*parts):
    return os.path.join(ROOT_DIR, *parts)

# ---- Constants with relative paths ----
SEC_JSON_PATH = rel("Source Data", "company_tickers.json")
FAILED_LOG_PATH = rel("Logs", "latest_prices_failed_tickers.csv")
OUTPUT_CSV_PATH = rel("Source Data", "latest_prices.csv")
OLD_PRICES = rel("Source Data", "old_latest_prices.csv")
FLAG_PATH = rel("Flags", f"all_prices_done_{os.getpid()}.flag")

# Load tickers from JSON
with open(SEC_JSON_PATH, 'r') as f:
    data = json.load(f)

tickers = ['APUS'] + [v['ticker'] for v in data.values()]
total = len(tickers)

all_prices = []
failed = []

pbar = tqdm(total=total, unit="companies")

time.sleep(1.5)

for ticker in tickers:
    try:
        price = yf.Ticker(ticker).fast_info.last_price
        if price is None:
            raise ValueError("No price data returned")
        all_prices.append({
            'Ticker': ticker,
            'Price': price,
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    except Exception as e:
        print(f"Failed for {ticker}: {e}")
        failed.append({
            'Ticker': ticker,
            'Reason': str(e),
            'When': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        all_prices.append({
            'Ticker': ticker,
            'Price': None,
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    pbar.update(1)
    pbar.set_description(f"Scanned {pbar.n} / {total} companies")

pbar.close()

# Log failed tickers if any
if failed:
    fail_df = pd.DataFrame(failed)
    file_exists = os.path.exists(FAILED_LOG_PATH)
    fail_df.to_csv(FAILED_LOG_PATH, mode='a', index=False, header=not file_exists)
    print(f"Logged {len(failed)} failed tickers to {FAILED_LOG_PATH}")

# Step 0: Load current latest prices (if any)
if os.path.exists(OUTPUT_CSV_PATH):
    existing_df = pd.read_csv(OUTPUT_CSV_PATH)
else:
    existing_df = pd.DataFrame(columns=["Ticker", "Price", "Timestamp"])

existing_dict = existing_df.set_index("Ticker").to_dict("index")

# Step 1: Archive existing latest_prices.csv as old_prices.csv BEFORE writing new data
if os.path.exists(OUTPUT_CSV_PATH):
    shutil.copyfile(OUTPUT_CSV_PATH, OLD_PRICES)
    print(f"📦 Archived previous latest_prices.csv to old_prices.csv")

# Step 2: Save merged data
merged_rows = []

for row in all_prices:
    ticker = row["Ticker"]
    price = row["Price"]
    timestamp = row["Timestamp"]

    if price is None and ticker in existing_dict:
        merged_rows.append({
            "Ticker": ticker,
            "Price": existing_dict[ticker].get("Price"),
            "Timestamp": existing_dict[ticker].get("Timestamp")
        })
    else:
        merged_rows.append(row)

# Save merged result
df_final = pd.DataFrame(merged_rows)
df_final.to_csv(OUTPUT_CSV_PATH, index=False)
print(f"✅ Saved latest prices to {OUTPUT_CSV_PATH}")

# Write the flag LAST, after everything is done
with open(FLAG_PATH, "w") as f:
    f.write("done")
print(f"🚩 Dropped completion flag at {FLAG_PATH}")
