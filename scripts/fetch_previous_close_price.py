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
parser = argparse.ArgumentParser(description="Fetch previous close stock prices.")
parser.add_argument('--commit', action='store_true', help='Overwrite old_prices.csv with latest data')
args = parser.parse_args()

# ---- PATH SETUP ----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))     # e.g. Market/scripts
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))  # Market folder

def rel(*parts):
    return os.path.join(ROOT_DIR, *parts)

# ---- Constants with relative paths ----
SEC_JSON_PATH = rel("Source Data", "company_tickers.json")
FAILED_LOG_PATH = rel("Logs", "previous_close_prices_failed_tickers.csv")
OUTPUT_CSV_PATH = rel("Source Data", "previous_close_prices.csv")
OLD_PRICES = rel("Source Data", "previous_previous_close_prices.csv")
FLAG_PATH = rel("Flags", f"previous_close_prices_done_{os.getpid()}.flag")

# Load tickers from JSON
with open(SEC_JSON_PATH, 'r') as f:
    data = json.load(f)

tickers = ['APUS'] + [v['ticker'] for v in data.values()]
total = len(tickers)

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

all_prices = []
failed = []

pbar = tqdm(total=total, unit="companies")

time.sleep(1.5)
# Fetch prices in batches
for batch in chunk_list(tickers, 50):
    tickers_str = " ".join(batch)

    try:
        yf_data = yf.download(
            tickers_str,
            period='1d',
            interval='1m',
            progress=False,
            threads=True,
            group_by='ticker',
            auto_adjust=False
        )
    except Exception as e:
        print(f"Batch fetch failed: {e}")
        time.sleep(4)
        for ticker in batch:
            failed.append({
                'Ticker': ticker,
                'Reason': f"Batch fetch failed: {e}",
                'When': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            all_prices.append({'Ticker': ticker, 'PreviousClose': None, 'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
            pbar.update(1)
        continue

    for ticker in batch:
        try:
            # When only 1 ticker is passed, yf.download returns a DataFrame, not multiindex
            if isinstance(yf_data.columns, pd.MultiIndex):
                if ticker not in yf_data.columns.levels[0]:
                    all_prices.append({'Ticker': ticker, 'PreviousClose': None, 'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
                    failed.append({
                        'Ticker': ticker,
                        'Reason': 'Ticker missing from batch data',
                        'When': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    pbar.update(1)
                    pbar.set_description(f"Scanned {pbar.n} / {total} companies")
                    continue
                close_prices = yf_data[ticker]['Close'].dropna()
            else:
                # Single ticker batch returns normal df
                close_prices = yf_data['Close'].dropna()

            if close_prices.empty:
                all_prices.append({'Ticker': ticker, 'PreviousClose': None, 'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
                failed.append({
                    'Ticker': ticker,
                    'Reason': 'No close prices returned',
                    'When': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                pbar.update(1)
                pbar.set_description(f"Scanned {pbar.n} / {total} companies")
                continue

            last_close = close_prices.iloc[-1]
            all_prices.append({'Ticker': ticker, 'PreviousClose': last_close, 'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
            pbar.update(1)
            pbar.set_description(f"Scanned {pbar.n} / {total} companies")

        except Exception as e:
            print(f"Failed for {ticker}: {e}")
            all_prices.append({'Ticker': ticker, 'PreviousClose': None, 'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
            failed.append({
                'Ticker': ticker,
                'Reason': str(e),
                'When': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            pbar.update(1)
            pbar.set_description(f"Scanned {pbar.n} / {total} companies")

    time.sleep(1)

pbar.close()

# Archive previous CSV before writing new one
if os.path.exists(OUTPUT_CSV_PATH):
    shutil.copyfile(OUTPUT_CSV_PATH, OLD_PRICES)
    print(f"📦 Archived previous {os.path.basename(OUTPUT_CSV_PATH)} to {os.path.basename(OLD_PRICES)}")

# Save results
df_final = pd.DataFrame(all_prices)
df_final.to_csv(OUTPUT_CSV_PATH, index=False)
print(f"✅ Saved previous close prices to {OUTPUT_CSV_PATH}")

# Log failures
if failed:
    df_failed = pd.DataFrame(failed)
    file_exists = os.path.exists(FAILED_LOG_PATH)
    df_failed.to_csv(FAILED_LOG_PATH, mode='a', index=False, header=not file_exists)
    print(f"Logged {len(failed)} failed tickers to {FAILED_LOG_PATH}")

# Write completion flag last
with open(FLAG_PATH, "w") as f:
    f.write("done")
print(f"🚩 Dropped completion flag at {FLAG_PATH}")
