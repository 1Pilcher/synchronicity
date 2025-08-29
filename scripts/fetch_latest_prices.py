import json
import yfinance as yf
import pandas as pd
import time
from tqdm import tqdm
import shutil
import argparse
from datetime import datetime, timedelta
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import platform
import winsound
import xlwings as xw
import os
import sys

# Change working dir to the folder of the script so relative paths work
os.chdir(os.path.dirname(os.path.abspath(__file__)))
print(f"Current working dir: {os.getcwd()}", file=sys.stderr)


# CLI setup
parser = argparse.ArgumentParser(description="Fetch latest stock prices.")
parser.add_argument('--commit', action='store_true', help='Overwrite old_prices.csv with latest data')
args = parser.parse_args()

# ---- PATH SETUP ----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

def rel(*parts):
    return os.path.join(ROOT_DIR, *parts)

SEC_JSON_PATH = rel("Source Data", "company_tickers.json")
FAILED_LOG_PATH = rel("Logs", "latest_prices_failed_tickers.csv")
OUTPUT_CSV_PATH = rel("Source Data", "latest_prices.csv")
OLD_PRICES = rel("Source Data", "old_latest_prices.csv")
FLAG_PATH = rel("Flags", f"all_prices_done_{os.getpid()}.flag")
EXTRA_DIR = os.path.join(ROOT_DIR, "Extra")
SOUND_DIR = os.path.join(EXTRA_DIR, "sounds")
SOUND = os.path.join(SOUND_DIR, "success_jingle.wav")

# Load tickers from JSON
with open(SEC_JSON_PATH, 'r') as f:
    data = json.load(f)

tickers = ['APUS'] + [v['ticker'] for v in data.values()]
#tickers = ['AAPL', 'MSFT', 'TSLA', 'APUS']


# Timezone aware timestamp
EST = pytz.timezone("US/Eastern")
def get_est_timestamp():
    return datetime.now(EST).strftime("%Y-%m-%d %H:%M:%S")


def countdown_timer(seconds):
    for remaining in range(seconds, 0, -1):
        mins, secs = divmod(remaining, 60)
        time_str = f"\r⏳ Retrying in {mins:02}:{secs:02}... "
        print(time_str, end="", flush=True)
        time.sleep(1)
    print("\r✅ Retry time reached!")




# Check to see if rate limited
def preflight_test(tickers=['AAPL', 'MSFT', 'APUS'], max_retries=3, wait_minutes=15):
    for attempt in range(1, max_retries + 1):
        print(f"🔎 Pre-flight test attempt {attempt} for tickers: {tickers}")
        failures = []
        for ticker in tickers:
            try:
                info = yf.Ticker(ticker).get_info()
                if info and info != {}:
                    print(f"✅ {ticker} fetch successful. Proceeding with full fetch.")
                    return True
                else:
                    raise Exception("Empty response (possible rate limit)")
            except Exception as e:
                print(f"⚠️ {ticker} fetch failed: {e}")
                failures.append(ticker)

        if len(failures) == len(tickers):  # both/all tickers failed
            if attempt < max_retries:
                wait_time = wait_minutes * 60
                print(f"⏳ Both failed. Waiting {wait_minutes} minutes before retrying pre-flight test...")
                countdown_timer(wait_time)
                beep()
            else:
                print(f"❌ Failed to fetch all tickers after {max_retries} attempts. Exiting.")
                return False
        else:
            # At least one ticker succeeded, no wait needed
            return True

#if not preflight_test():
#    print("Exiting script due to repeated pre-flight fetch failures.")
#    exit(1)


# Load existing prices (for skip logic & fallback)
if os.path.exists(OUTPUT_CSV_PATH):
    existing_df = pd.read_csv(OUTPUT_CSV_PATH, low_memory=False)
    existing_df = existing_df.drop_duplicates(subset="Ticker", keep="last")  # <--- Add this
    if 'Timestamp' in existing_df.columns:
        existing_df['Timestamp'] = pd.to_datetime(existing_df['Timestamp'], errors='coerce')
        if existing_df['Timestamp'].dt.tz is None:
            existing_df['Timestamp'] = existing_df['Timestamp'].dt.tz_localize('US/Eastern', ambiguous='NaT', nonexistent='NaT')
else:
    existing_df = pd.DataFrame(columns=['Ticker', 'Price', 'Type', 'Exchange', 'Prev Close', 'Open', 'High', 'Low', 'Mkt Cap', 'Vol', '10d Avg Vol', '3m Avg Vol', 'Sh', '52w High', '52w Low', '52w Chg', '50d Avg', '200d Avg', 'Timestamp'])

existing_dict = existing_df.set_index("Ticker").to_dict("index")


# Beep
def beep():
    if platform.system() == "Windows":
        winsound.Beep(1000, 500)
    else:
        print("\a", end="")  # System bell (may or may not work depending on terminal)

# Start results with recovered tickers so we don't fetch them twice
results = []
tickers_to_fetch = tickers
print(f"Will fetch {len(tickers_to_fetch)} tickers. No skipping based on freshness.")

# Define market open cutoff (9:30 AM EST today) for freshness check
now_est = datetime.now(EST)
market_open_today = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
if now_est < market_open_today:
    market_open_today -= timedelta(days=1)

# Fetch function with retries
def fetch_price(ticker, retries=3, max_wait=15):


    start_time = time.time()
    for attempt in range(1, retries+1):
        try:
            fast = yf.Ticker(ticker).fast_info
            price = getattr(fast, 'last_price', None)
            if price is None:
                raise Exception("No price data")
            previous_close = getattr(fast, 'previous_close', None)
            open_price = getattr(fast, 'open', None)
            day_high = getattr(fast, 'day_high', None)
            day_low = getattr(fast, 'day_low', None)
            last_volume = getattr(fast, 'last_volume', None)
            ten_day_avg_volume = getattr(fast, 'ten_day_average_volume', None)
            three_month_avg_volume = getattr(fast, 'three_month_average_volume', None)
            mkt_cap = getattr(fast, 'market_cap', None)
            exchange = getattr(fast, 'exchange', None)
            type = getattr(fast, 'quote_type', None)
            shares = getattr(fast, 'shares', None)
            year_change = getattr(fast, 'year_change', None)
            year_high = getattr(fast, 'year_high', None)
            year_low = getattr(fast, 'year_low', None)
            fifty_day_avg = getattr(fast, 'fifty_day_average', None)
            two_hundred_day_avg = getattr(fast, 'two_hundred_day_average', None)
            ts = get_est_timestamp()
            return {
                'Ticker': ticker,
                'Price': price,
                'Type': type,
                'Exchange': exchange,
                'Prev Close': previous_close,
                'Open': open_price,
                'High': day_high,
                'Low': day_low,
                'Mkt Cap': mkt_cap,
                'Vol': last_volume,
                '10d Avg Vol': ten_day_avg_volume,
                '3m Avg Vol': three_month_avg_volume,
                'Sh': shares,
                '52w High': year_high,
                '52w Low': year_low,
                '52w Chg': year_change,
                '50d Avg': fifty_day_avg,
                '200d Avg': two_hundred_day_avg,
                'Timestamp': ts
            }, None
        except Exception as e:
            elapsed = time.time() - start_time
            if elapsed > max_wait or attempt == retries:
                print(f"❌ {ticker} failed: {e} (after {elapsed:.1f}s)")
                return {'Ticker': ticker, 'Price': None, 'Timestamp': get_est_timestamp()}, {'Ticker': ticker, 'Reason': str(e), 'When': get_est_timestamp()}
            else:
                wait = random.uniform(1.5,3.5) * attempt
                print(f"⚠️ {ticker} retry {attempt} in {wait:.1f}s")
                time.sleep(wait)

MAX_WORKERS = 5
failures_main = []
success_count = 0
fail_count = 0

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(fetch_price, t): t for t in tickers_to_fetch}
    with tqdm(total=len(futures), desc="Multithreaded Price Fetch") as pbar:
        for future in as_completed(futures):
            row, fail = future.result()
            results.append(row)
            if fail:
                failures_main.append(fail)
                fail_count += 1
            else:
                success_count += 1
            pbar.update(1)

print(f"\n✅ Fetched: {success_count} | ❌ Failed: {fail_count} | Total requested: {len(tickers_to_fetch)}")

def write_to_excel(df, workbook_path, sheet_name="data_LatestPrices"):
    try:
        app = xw.apps.active # attach to running Excel
    except IndexError:
        app = xw.App(visible=True) # fallback, start Excel if none open

    # Try to get already opened workbook
    wb = None
    for book in app.books:
        if book.name == "Screener.xlsm":
            wb = book
            break
    if wb is None:
        wb = app.books.open(EXCEL_PATH)

    sht = wb.sheets[sheet_name]
    sht.used_range.clear_contents()
    sht.range("A1").value = df.columns.tolist()
    sht.range("A2").value = df.values
    wb.save()

# Final failures logged
all_failures = failures_main
if all_failures:
    pd.DataFrame(all_failures).to_csv(FAILED_LOG_PATH, index=False)
    print(f"📝 Logged {len(all_failures)} failed tickers to {FAILED_LOG_PATH}")
else:
    if os.path.exists(FAILED_LOG_PATH):
        os.remove(FAILED_LOG_PATH)
    print("🎉 All tickers recovered!")

# Backup old prices before overwrite
if os.path.exists(OUTPUT_CSV_PATH):
    shutil.copyfile(OUTPUT_CSV_PATH, OLD_PRICES)
    print(f"📦 Archived previous latest_prices.csv to old_latest_prices.csv")

# Save final merged data, preferring new data over old if available
existing_dict.update({r['Ticker']: r for r in results})
merged_rows = []
for ticker, data in existing_dict.items():
    data["Ticker"] = ticker
    merged_rows.append(data)


df_final = pd.DataFrame(merged_rows)

EXCEL_PATH = rel("Screener.xlsm")  # use the real filename here
write_to_excel(df_final, EXCEL_PATH)

df_final.to_csv(OUTPUT_CSV_PATH, index=False)
print(f"✅ Saved latest prices to {OUTPUT_CSV_PATH}")

# Write completion flag
with open(FLAG_PATH, "w") as f:
    f.write("done")
print(f"🚩 Dropped completion flag at {FLAG_PATH}")

if os.path.exists(SOUND):
    winsound.PlaySound(SOUND, winsound.SND_FILENAME | winsound.SND_ASYNC)
    time.sleep(4)
else:
    print(f"Sound file not found: {SOUND}")