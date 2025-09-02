import pandas as pd
import yfinance as yf
import os
from datetime import datetime
from tqdm import tqdm
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import pytz
import platform
import winsound
import xlwings as xw
import sys

# Change working dir to the folder of the script so relative paths work
os.chdir(os.path.dirname(os.path.abspath(__file__)))
print(f"Current working dir: {os.getcwd()}", file=sys.stderr)


eastern = pytz.timezone("US/Eastern")

# Base folder for your Market project
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
BASE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
SRC_DIR = os.path.join(BASE_DIR, "Source Data")
FLAGS_DIR = os.path.join(BASE_DIR, "Flags")
FILTERED_TICKERS_PATH = os.path.join(SRC_DIR, "filtered_tickers.csv")
LATEST_PRICES_PATH = os.path.join(SRC_DIR, "latest_prices.csv")
FAILED_TICKERS_PATH = os.path.join(BASE_DIR, "Logs", "latest_filtered_failed_tickers.csv")
LOCK_DIR = os.path.join(BASE_DIR, "Locks")
EXTRA_DIR = os.path.join(BASE_DIR, "Extra")
SOUND_DIR = os.path.join(EXTRA_DIR, "sounds")
SOUND = os.path.join(SOUND_DIR, "success_jingle.wav")


os.makedirs(LOCK_DIR, exist_ok=True)
os.makedirs(FLAGS_DIR, exist_ok=True)

def active_instance_count():
    return len([f for f in os.listdir(LOCK_DIR) if f.startswith("instance_")])

def create_lock():
    pid = os.getpid()
    lock_file = os.path.join(LOCK_DIR, f"instance_{pid}.lock")
    with open(lock_file, 'w') as f:
        f.write(str(datetime.now(eastern).isoformat()))
    return lock_file

def remove_lock(lock_file):
    if os.path.exists(lock_file):
        os.remove(lock_file)

while active_instance_count() >= 5:
    print("⚠️ Too many instances running. Waiting...")
    time.sleep(3)

lock_file = create_lock()


def load_filtered_tickers():
    if not os.path.exists(FILTERED_TICKERS_PATH):
        print("filtered_tickers.csv not found.")
        return []

    with open(FILTERED_TICKERS_PATH, "r") as f:
        lines = [line.strip().upper() for line in f if line.strip()]
        return lines[1:] if lines and lines[0].startswith("TICKER") else lines

def load_existing_prices():
    if not os.path.exists(LATEST_PRICES_PATH):
        return pd.DataFrame(columns=['Ticker', 'Price', 'Type', 'Exchange', 'Prev Close', 'Open', 'High', 'Low', 'Mkt Cap', 'Vol', '10d Avg Vol', '3m Avg Vol', 'Sh', '52w High', '52w Low', '52w Chg', '50d Avg', '200d Avg', 'Timestamp'])

    df = pd.read_csv(LATEST_PRICES_PATH)

    for col in ['Ticker', 'Price', 'Type', 'Exchange', 'Prev Close', 'Open', 'High', 'Low', 'Mkt Cap', 'Vol', '10d Avg Vol', '3m Avg Vol', 'Sh', '52w High', '52w Low', '52w Chg', '50d Avg', '200d Avg', 'Timestamp']:
        if col not in df.columns:
            df[col] = None

    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce", utc=True).dt.tz_convert(eastern)
    return df

def fetch_prices_multithreaded(tickers, max_workers=5, retries=1, max_wait=15):
    data = []
    failed = []

    def fetch(ticker):
        start_time = time.time()
        for attempt in range(1, retries + 1):
            try:
                fast = yf.Ticker(ticker).fast_info
                price = getattr(fast, 'last_price', None)
                if price is None:
                    raise Exception("No price data returned")
                previous_close = getattr(fast, 'regular_market_previous_close', None)
                open_price= getattr(fast, 'open', None)
                day_high= getattr(fast, 'day_high', None)
                day_low= getattr(fast, 'day_low', None)
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
                ts = datetime.now(eastern).isoformat()
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
                    return None, {
                        'Ticker': ticker,
                        'Reason': str(e),
                        'When': datetime.now(eastern).isoformat()
                    }
                time.sleep(random.uniform(1.5, 2.5) * attempt)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch, t): t for t in tickers}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching prices (multi)"):
            row, fail = future.result()
            if row:
                data.append(row)
            if fail:
                failed.append(fail)

    if failed:
        fail_df = pd.DataFrame(failed)
        file_exists = os.path.exists(FAILED_TICKERS_PATH)
        fail_df.to_csv(FAILED_TICKERS_PATH, mode='a', index=False, header=not file_exists)
        print(f"Logged {len(failed)} failed tickers to failed_tickers.csv")

    return pd.DataFrame(data)

def update_latest_prices():
    filtered = load_filtered_tickers()
    filtered = ['APUS'] + filtered
    if not filtered:
        print("No filtered tickers found.")
        return

    if len(filtered) > 500:
        print(f"Too many tickers ({len(filtered)}). Trimming to amount.")
        filtered = filtered[:500]

    new_data = fetch_prices_multithreaded(filtered)

    if new_data.empty:
        print("No new data fetched.")
        return

    existing = load_existing_prices()

    new_data["Timestamp"] = pd.to_datetime(new_data["Timestamp"], errors="coerce", utc=True).dt.tz_convert(eastern)
    existing["Timestamp"] = pd.to_datetime(existing["Timestamp"], errors="coerce", utc=True).dt.tz_convert(eastern)

    updated = existing.set_index("Ticker")


    # Ensure uniqueness (keep the last occurrence if duplicates exist)
    updated = updated[~updated.index.duplicated(keep='last')]


    for _, row in new_data.iterrows():
        ticker = row["Ticker"]
        new_time = row["Timestamp"]

        if (
            ticker not in updated.index
            or pd.isna(updated.loc[ticker, "Timestamp"])
            or new_time > updated.loc[ticker, "Timestamp"]
        ):
            updated.loc[ticker] = row[['Ticker', 'Price', 'Type', 'Exchange', 'Prev Close', 'Open', 'High', 'Low', 'Mkt Cap', 'Vol', '10d Avg Vol', '3m Avg Vol', 'Sh', '52w High', '52w Low', '52w Chg', '50d Avg', '200d Avg', 'Timestamp']]

    updated = updated.reset_index()
    updated.sort_values("Ticker", inplace=True)
    updated.to_csv(LATEST_PRICES_PATH, index=False)

    # Save to Excel
    EXCEL_PATH = os.path.join(BASE_DIR, "Screener.xlsm")

    try:
        app = xw.apps.active  # attach to running Excel
    except IndexError:
        app = xw.App(visible=True)  # fallback, start Excel if none open

    # Try to get already opened workbook
    wb = None
    for book in app.books:
        if book.name == "Screener.xlsm":
            wb = book
            break
    if wb is None:
        wb = app.books.open(EXCEL_PATH)

    sht = wb.sheets["data_LatestPrices"]
    sht.used_range.clear_contents()
    sht.range("A1").value = updated.columns.tolist()
    sht.range("A2").value = updated.values
    #wb.save()

    # DO NOT call app.quit() or wb.close() to keep Excel and workbook open
    print(f"✅ Updated {len(new_data)} tickers at {datetime.now(eastern).strftime('%H:%M:%S')}")

if __name__ == "__main__":
    try:
        update_latest_prices()
        flag_file = os.path.join(FLAGS_DIR, f"filtered_prices_done_{os.getpid()}.flag")
        with open(flag_file, "w") as f:
            f.write("done")

        #if os.path.exists(SOUND):
        #    winsound.PlaySound(SOUND, winsound.SND_FILENAME | winsound.SND_ASYNC)
        #    time.sleep(4)
        #else:
        #    print(f"Sound file not found: {SOUND}")

    finally:
        remove_lock(lock_file)