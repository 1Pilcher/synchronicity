import pandas as pd
from tqdm import tqdm
import numpy as np
import requests
from collections import defaultdict
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
from datetime import datetime, timedelta
import yfinance as yf
import sys
import platform
import winsound
import xlwings as xw
import os
import sys
from curl_cffi import requests

# Change working dir to the folder of the script so relative paths work
os.chdir(os.path.dirname(os.path.abspath(__file__)))
print(f"Current working dir: {os.getcwd()}", file=sys.stderr)


# Set timestamps to EST (Market Time)
def get_est_timestamp():
    return datetime.now(pytz.timezone("US/Eastern"))


# ---- PATH SETUP ----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))     # e.g. Market/scripts
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))  # Market folder
EXTRA_DIR = os.path.join(ROOT_DIR, "Extra")
SOUND_DIR = os.path.join(EXTRA_DIR, "sounds")
SOUND = os.path.join(SOUND_DIR, "success_jingle.wav")

def rel(*parts):
    return os.path.join(ROOT_DIR, *parts)

# ---- Constants with relative paths ----
SEC_JSON_PATH = rel("Source Data", "company_tickers.json")
SEC_CSV_PATH = rel("Source Data", "sec_company_list.csv")
DICT_CSV_PATH = rel("Source Data", "dictionary.csv")
FAILED_LOG_PATH = rel("Logs", "full_metrics_failed_tickers.csv")
OUTPUT_CSV_PATH = rel("Source Data", "full_metrics.csv")
SPLITS_FOLDER = rel("Source Data", "Splits")
FLAG_PATH = rel("Flags", "full_metrics.flag")
PRIVATE_PATH = rel("Source Data", "private_list.csv")
ETF_PATH = rel("Source Data", "etf_list.csv")
MUTUAL_FUND_PATH = rel("Source Data", "mutual_fund_list.csv")
CRYPTO_PATH = rel("Source Data", "crypto_list.csv")
INDICES_PATH = rel("Source Data", "indices_list.csv")
PRIVATE_OUTPUT = rel("Source Data", "private_full_metrics.csv")
MUTUAL_FUND_OUTPUT = rel("Source Data", "mutual_fund_full_metrics.csv")
ETF_OUTPUT = rel("Source Data", "etf_full_metrics.csv")
CRYPTO_OUTPUT = rel("Source Data", "crypto_full_metrics.csv")
INDICES_OUTPUT = rel("Source Data", "indices_full_metrics.csv")

# SEC JSON URL & Headers
url = "https://www.sec.gov/files/company_tickers.json"
headers = {"User-Agent": "Josh Pilcher (jpilcher887@gmail.com)"}

# Fetch and save SEC JSON file
response = requests.get(url, headers=headers)
response.raise_for_status()
with open(SEC_JSON_PATH, "w") as f:
    f.write(response.text)
print("Downloaded and saved company_tickers.json")

#NUM_SESSIONS = 5
#session_pool = [requests.Session(impersonate="chrome") for _ in range(NUM_SESSIONS)]


data = response.json()
df = pd.DataFrame.from_dict(data, orient='index')

# Clean & reorder columns
df = df.rename(columns={
    "cik_str": "CIK",
    "ticker": "Ticker",
    "title": "Name"
})
df = df.dropna(subset=['Ticker'])
df["CIK"] = df["CIK"].astype(str).str.zfill(10)

# Save SEC company list CSV
df.to_csv(SEC_CSV_PATH, index=False)
print(f"✅ Saved {len(df)} tickers from SEC to CSV")

# Read back all CSV's
csv_paths = [
#    SEC_CSV_PATH,
#    MUTUAL_FUND_PATH,
#    ETF_PATH,
#    CRYPTO_PATH,
#    PRIVATE_PATH
    INDICES_PATH

]


dfs = []
for path in csv_paths:
    if os.path.exists(path):
        tmp_df = pd.read_csv(path)
        if 'Ticker' in tmp_df.columns:
            tmp_df['Ticker'] = tmp_df['Ticker'].astype(str).str.upper()
            dfs.append(tmp_df)
        else:
            print(f"Skipping {path}: no 'Ticker' column found.")
    else:
        print(f"File not found: {path}")

combined_df = pd.concat(dfs, ignore_index=True)
combined_df.drop_duplicates(subset='Ticker', inplace=True)
combined_df.reset_index(drop=True, inplace=True)

print(f"Combined tickers from all sources: {len(combined_df)} unique tickers")

# Create final ticker list
tickers = combined_df['Ticker'].dropna().unique()




#tickers = df['Ticker'].dropna().str.upper().unique()
# Load and clean dictionary schema
#schema_df = pd.read_csv(DICT_CSV_PATH)
#schema_df["YF Code"] = schema_df["YF Code"].astype(str).str.strip().str.strip("'").str.strip('"')
#schema_df["Short Name"] = schema_df["Short Name"].astype(str).str.strip()
#fields = schema_df["YF Code"].dropna().astype(str).str.strip()

#allowed_categories = {
#    "Identity", "Liquidity & Trading", "Valuation", "Share Structure",
#    "Balance Sheet Health", "Earnings", "Margins", "Cashflow", "Growth", "Per Share Financials",
#    "Price-Based Valuation Multiples", "Enterprise Value Multiples", "Dividend",
#    "Analyst Sentiment", "Historical & Technical", "Risk & Governance", "Ownership", "Financial Reporting"
#}
#filtered_schema_df = schema_df[schema_df['Category'].isin(allowed_categories)]

# Mapping dicts
#full_name_map = dict(zip(schema_df["YF Code"], schema_df["Full Name"]))
#short_name_map = dict(zip(schema_df["YF Code"], schema_df["Short Name"]))
#category_map = dict(zip(schema_df["YF Code"], schema_df["Category"]))



# --- *** ADDED: Retry previously failed tickers first *** ---
if os.path.exists(FAILED_LOG_PATH):
    failed_df = pd.read_csv(FAILED_LOG_PATH)
    failed_tickers = failed_df['Ticker'].dropna().unique().tolist()
    # Filter to only tickers still in the SEC list
    failed_tickers = [t for t in failed_tickers if t in tickers]
    print(f"Retrying {len(failed_tickers)} previously failed tickers...")
else:
    failed_tickers = []

def retry_failed_tickers(failure_list, max_retries=3):
    recovered_rows = []
    final_failures = []

    for ticker in tqdm(failure_list, desc="🔁 Retrying failed tickers"):
        for attempt in range(1, max_retries + 1):
            try:
                info = yf.Ticker(ticker).get_info()
                if info:
                    row = {"Ticker": ticker, "Timestamp": get_est_timestamp()}
                    # Flatten dict: only keep str, int, float, bool, None
                    for k, v in info.items():
                        if isinstance(v, (str, int, float, bool)) or v is None:
                            row[k] = v
                    recovered_rows.append(row)
                    tqdm.write(f"✅ Recovered {ticker} on attempt {attempt}")
                    break
            except Exception as e:
                if attempt == max_retries:
                    tqdm.write(f"❌ {ticker} failed: {e}")
                    final_failures.append({"Ticker": ticker, "Error": str(e)})
                else:
                    time.sleep(0.5 * (2 ** (attempt - 1)))
    return recovered_rows, final_failures

recovered_rows, final_failures = retry_failed_tickers(failed_tickers, max_retries=3)

# Initialize results list starting with recovered failed tickers (so they’re not lost)
results = recovered_rows.copy()




# Exclude recovered tickers from main fetch (already got fresh data for those)
recovered_tickers = [row['Ticker'] for row in recovered_rows]
tickers_to_fetch = [t for t in tickers if t not in recovered_tickers]
print(f"Will fetch {len(tickers_to_fetch)} fresh tickers.")

# ---- BATCHED yfinance fetch ----
def fetch_ticker_info(ticker, retries=3, max_total_wait=15):
    start_time = time.time()
    for attempt in range(1, retries + 1):
        try:
            info = yf.Ticker(ticker).get_info()
            if not info:
                raise Exception("Empty response (possible rate limit)")

            # Flatten dict into row
            row = {'Ticker': ticker, 'Timestamp': get_est_timestamp()}
            for k, v in info.items():
                # Optionally filter out nested dicts/lists you don't want
                if isinstance(v, (str, int, float, bool)) or v is None:
                    row[k] = v
                # Otherwise skip nested dict/list for now
            return row, None
        except Exception as e:
            elapsed = time.time() - start_time
            if elapsed > max_total_wait or attempt == retries:
                tqdm.write(f"❌ {ticker}: {e} (after {elapsed:.1f}s)")
                row = {'Ticker': ticker, 'Timestamp': get_est_timestamp()}
                return row, {'Ticker': ticker, 'Error': str(e)}
            else:
                wait = random.uniform(1.5, 3.5) * attempt
                tqdm.write(f"⚠️ {ticker}: retry {attempt}, waiting {wait:.1f}s")
                time.sleep(wait)
def countdown_timer(seconds):
    for remaining in range(seconds, 0, -1):
        mins, secs = divmod(remaining, 60)
        time_str = f"\r⏳ Retrying in {mins:02}:{secs:02}... "
        print(time_str, end="", flush=True)
        time.sleep(1)
    print("\r✅ Retry time reached!                 ")

def beep():
    if platform.system() == "Windows":
        import winsound
        winsound.Beep(1000, 500)
    else:
        print("\a", end="")  # System bell (may or may not work depending on terminal)

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

if not preflight_test():
    print("Exiting script due to repeated pre-flight fetch failures.")
    exit(1)


MAX_WORKERS = 1
first_failures = []
success_count = 0
fail_count = 0

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(fetch_ticker_info, t): t for t in tickers_to_fetch}

    with tqdm(total=len(futures), desc="Multithreaded Fetch") as pbar:
        for future in as_completed(futures):
            row, fail = future.result()
            results.append(row)
            if fail:
                first_failures.append(fail)
                fail_count += 1
            else:
                success_count += 1
            pbar.update(1)

print(f"\n✅ Done! Fetched: {success_count} | ❌ Failed: {fail_count} | Total requested: {len(tickers_to_fetch)}")
print(f"Total results length (including skipped + recovered): {len(results)}")

# Retry logic for failed tickers from this main fetch
recovered_rows_2, final_failures = retry_failed_tickers(first_failures, max_retries=3)
results.extend(recovered_rows_2)

# Create output df, drop duplicates keeping last (favoring recovered/new data)
output_df = pd.DataFrame(results).drop_duplicates(subset="Ticker", keep="last")

# Merge with SEC info df to keep all company metadata
df_merged = pd.merge(output_df, df, on='Ticker', how='left')

# Log remaining failures if any
if final_failures:
    pd.DataFrame(final_failures).to_csv(FAILED_LOG_PATH, index=False)
    print(f"📝 Logged {len(final_failures)} failed tickers to 'full_metrics_failed_tickers.csv'")
else:
    # If no failures remain, remove old failed log so next run starts clean
    if os.path.exists(FAILED_LOG_PATH):
        os.remove(FAILED_LOG_PATH)
    print("🎉 All tickers recovered after retries!")

print("\n🔍 df columns:", df.columns.tolist())
print("🔍 output_df columns:", output_df.columns.tolist())
print("🧪 df Ticker dtype:", df['Ticker'].dtype)
print("🧪 output_df Ticker dtype:", output_df['Ticker'].dtype)

# Rename columns with short names
#df_merged.rename(columns=short_name_map, inplace=True)

# Date conversion for selected columns
for col in ['Split Date', 'FY End', 'Next FY', 'Latest Qtr']:
    if col in df_merged.columns:
        try:
            df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce')
            df_merged.loc[~np.isfinite(df_merged[col]), col] = np.nan
            df_merged[col] = pd.to_datetime(df_merged[col], unit='s', errors='coerce').dt.strftime('%Y-%m-%d')
        except Exception as e:
            print(f"⚠️ Error parsing column {col}: {e}")

# Calculated fields helper
def add_calculated_fields(df):
    def safe_div(x, y):
        mask = (y != 0) & (~y.isna()) & (~x.isna())
        result = pd.Series(np.nan, index=x.index, dtype='float64')
        result.loc[mask] = x.loc[mask] / y.loc[mask]
        return result

    if "totalDebt" in df.columns and "totalCash" in df.columns:
        df["Net Debt"] = df["totalDebt"] - df["totalCash"]
    if "operatingCashflow" in df.columns and "totalRevenue" in df.columns:
        df["Op CF M"] = df["operatingCashflow"] / df["totalRevenue"]
    if "freeCashflow" in df.columns and "totalRevenue" in df.columns:
        df["FCF M"] = df["freeCashflow"] / df["totalRevenue"]
    if "forwardPE" in df.columns and "earningsGrowth" in df.columns:
        df["PEG R"] = safe_div(df["forwardPE"], df["earningsGrowth"])
    if "freeCashflow" in df.columns and "sharesOutstanding" in df.columns:
        df["FCF/Sh"] = safe_div(df["freeCashflow"], df["sharesOutstanding"])
    if "enterpriseValue" in df.columns and "freeCashflow" in df.columns:
        df["EV/FCF"] = safe_div(df["enterpriseValue"], df["freeCashflow"])
    if "totalDebt" in df.columns and "ebitda" in df.columns:
        df["D/EBITDA"] = safe_div(df["totalDebt"], df["ebitda"])
    if "totalRevenue" in df.columns and "operatingMargins" in df.columns:
        df["Op P"] = (df["totalRevenue"] * df["operatingMargins"])
    return df




def write_to_excel(df, workbook_path, sheet_name="data_indicesFullMetrics"):
    app = xw.App(visible=False)
    wb = xw.Book(workbook_path)
    sht = wb.sheets[sheet_name]

    sht.used_range.clear_contents()

    # Start writing at A2 (headers), data below
    sht.range("A1").value = df.columns.tolist()
    sht.range("A2").value = df.values

    wb.save()

final_columns = [
'Ticker', 'IndexName', 'typeDisp', 'fullExchangeName',
    'fiftyDayAverageChange', 'fiftyDayAverageChangePercent',
    'twoHundredDayAverageChange',
    'twoHundredDayAverageChangePercent', 'firstTradeDateMilliseconds', 'Timestamp'

]

rename_map = {
'Ticker': 'Ticker', 'IndexName': 'Name',
    'typeDisp': 'Type', 'fullExchangeName': 'Exchange',
    'fiftyDayAverageChange': '50d Chg', 'fiftyDayAverageChangePercent': '50d % Chg',
    'twoHundredDayAverageChange': '200d Chg',
    'twoHundredDayAverageChangePercent': '200d % Chg',
    'firstTradeDateMilliseconds': 'First Trade',
    'Timestamp': 'Timestamp'

}

# Merge yfinance results with SEC data (CIK, etc.)
df_merged = pd.merge(output_df, df, on="Ticker", how="left")

# 🔑 Merge back the Name from indices_list.csv
df_merged = pd.merge(
    df_merged,
    combined_df[["Ticker", "Name"]].rename(columns={"Name": "IndexName"}),
    on="Ticker",
    how="left"
)

df_merged = add_calculated_fields(df_merged)

# Keep only the columns you care about, in order
existing_cols = [c for c in final_columns if c in df_merged.columns]
df_final = df_merged[existing_cols].copy()

# Apply renaming
df_final.rename(columns=rename_map, inplace=True)


# Order columns based on schema
#ordered_columns = ["Ticker", "Name", "CIK"]
#for name in filtered_schema_df["Short Name"]:
#    if name in df_merged.columns and name not in ordered_columns:
#        ordered_columns.append(name)

#ordered_columns += [col for col in df_merged.columns if col not in ordered_columns]

#df_merged = df_merged[ordered_columns]

#print("Before rename:", df_merged.columns.tolist())

# Drop columns that are all NaN
#nan_cols = df_merged.columns[df_merged.isna().all()].tolist()
#print("NaN columns to drop:", nan_cols)
#df_merged.drop(columns=nan_cols, inplace=True)

# Make splits folder if it doesn't exist
#os.makedirs(SPLITS_FOLDER, exist_ok=True)

# Group columns by category
#columns_by_category = defaultdict(list)
#for _, row in filtered_schema_df.iterrows():
#    short_name = row["Short Name"]
#    category = row["Category"]
#    if short_name in df_merged.columns:
#        columns_by_category[category].append(short_name)

# Sort by Market Cap if available
#if "Mkt Cap" in df_merged.columns:
#    df_merged.sort_values(by="Mkt Cap", ascending=False, inplace=True)
#else:
#    print("Warning: 'Mkt Cap' column missing, skipping sort.")

EXCEL_PATH = rel("Screener.xlsm")  # use the real filename here

try:
    write_to_excel(df_final, EXCEL_PATH)
except Exception as e:
    print(f"[WARNING] Could not write to Excel: {e}")
finally:
    df_final.to_csv(INDICES_OUTPUT, index=False)

print(df_final.columns.tolist())
print(df_final.head())




# Save category splits
#for category, cols in columns_by_category.items():
#    df_split = df_merged[cols]
#    filename = os.path.join(SPLITS_FOLDER, f"full_metrics_{category.replace(' ', '_')}.csv")
#    df_split.to_csv(filename, index=False)
#    print(f"✅ Saved {filename} with {len(cols)} columns")

# Write completion flag
with open(FLAG_PATH, "w") as f:
    f.write("done")
print(f"🚩 Dropped completion flag at {FLAG_PATH}")

if os.path.exists(SOUND):
    winsound.PlaySound(SOUND, winsound.SND_FILENAME | winsound.SND_ASYNC)
    time.sleep(2)
else:
    print(f"Sound file not found: {SOUND}")