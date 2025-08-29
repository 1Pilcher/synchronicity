import pandas as pd
import os
import numpy as np
import xlwings as xw
import winsound
import time
import os
import sys

# Change working dir to the folder of the script so relative paths work
os.chdir(os.path.dirname(os.path.abspath(__file__)))
print(f"Current working dir: {os.getcwd()}", file=sys.stderr)


# Base folder for your Market project
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
BASE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
SRC_DIR = os.path.join(BASE_DIR, "Source Data")
FLAGS_DIR = os.path.join(BASE_DIR, "Flags")
FULL_METRICS = os.path.join(SRC_DIR, "full_metrics.csv")
SCREENER = os.path.join(BASE_DIR, "Screener.xlsm")
FILTERED_TICKERS_PATH = os.path.join(SRC_DIR, "filtered_tickers.csv")
LATEST_PRICES_PATH = os.path.join(SRC_DIR, "latest_prices.csv")
FAILED_TICKERS_PATH = os.path.join(BASE_DIR, "Logs", "latest_filtered_failed_tickers.csv")
LOCK_DIR = os.path.join(BASE_DIR, "Locks")
EXTRA_DIR = os.path.join(BASE_DIR, "Extra")
SOUND_DIR = os.path.join(EXTRA_DIR, "sounds")
SOUND = os.path.join(SOUND_DIR, "success_jingle.wav")


# Load your full_metrics dataset
df = pd.read_csv(FULL_METRICS)

# Ensure necessary columns are present and numeric
numeric_cols = [
    "Mkt Cap", "EV", "Rev", "Gr P", "EBITDA", "Op P", "Net Inc",
    "Op CF", "FCF", "Avg Rating", "Analysts"
]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Fill blanks in Sector with "none"
df["Industry"] = df["Industry"].fillna("none")

# Group by Industry and calculate aggregates
grouped = df.groupby("Industry")

summary = grouped.agg({
    "Ticker": "count",
    "Mkt Cap": "sum",
    "EV": "sum",
    "Rev": "sum",
    "Gr P": "sum",
    "EBITDA": "sum",
    "Op P": "sum",
    "Net Inc": "sum",
    "Op CF": "sum",
    "FCF": "sum",
    "Avg Rating": "mean",
    "Analysts": "sum"
}).rename(columns={
    "Ticker": "# of Companies",
    "Mkt Cap": "T Mkt Cap",
    "EV": "T EV",
    "Rev": "T Rev",
    "Gr P": "T Gr P",
    "EBITDA": "T EBITDA",
    "Op P": "T Op P",
    "Net Inc": "T Net Inc",
    "Op CF": "T Op CF",
    "FCF": "T FCF",
    "Avg Rating": "Avg Rating",
    "Analysts": "T Analysts"
})

# Add Weighted Averages / Ratios
summary["W/A Gr M"] = summary["T Gr P"] / summary["T Rev"]
summary["W/A EBITDA M"] = summary["T EBITDA"] / summary["T Rev"]
summary["W/A Op M"] = summary["T Op P"] / summary["T Rev"]
summary["W/A Net M"] = summary["T Net Inc"] / summary["T Rev"]
summary["W/A Op CF M"] = summary["T Op CF"] / summary["T Rev"]
summary["W/A FCF M"] = summary["T FCF"] / summary["T Rev"]
summary["W/A P/E"] = summary["T Mkt Cap"] / summary["T Net Inc"]
summary["W/A EV/EBITDA"] = summary["T EV"] / summary["T EBITDA"]

# Add HHI & Concentration Ratios
hhi_data = []
for Industry, group in grouped:
    caps = group["Mkt Cap"].dropna()
    total = caps.sum()
    shares = (caps / total).sort_values(ascending=False)
    hhi = (shares**2).sum() * 10_000
    cr1 = shares.iloc[0] if len(shares) >= 1 else np.nan
    cr2 = shares.iloc[:2].sum() if len(shares) >= 2 else np.nan
    cr4 = shares.iloc[:4].sum() if len(shares) >= 4 else np.nan
    cr10 = shares.iloc[:10].sum() if len(shares) >= 10 else shares.sum()

    hhi_data.append({
        "Industry": Industry,
        "HHI Score": hhi,
        "CR1": cr1,
        "CR2": cr2,
        "CR4": cr4,
        "CR10": cr10
    })

hhi_df = pd.DataFrame(hhi_data).set_index("Industry")

# Merge with summary
summary = summary.merge(hhi_df, left_index=True, right_index=True)

# Bring Industry index back as column without triggering index export
summary = summary.reset_index(drop=False)

# Preferred column order
column_order = [
    "Industry", "# of Companies", "W/A P/E", "W/A EV/EBITDA", "T Mkt Cap", "T EV", "T Rev",
    "T Gr P", "T EBITDA", "T Op P", "T Net Inc", "T Op CF", "T FCF",
    "W/A Gr M", "W/A EBITDA M", "W/A Op M", "W/A Net M", "W/A Op CF M", "W/A FCF M",
    "Avg Rating", "T Analysts", "HHI Score", "CR1", "CR2", "CR4", "CR10"
]

# Apply column order
summary = summary[column_order]

# Sort by Total Market Capitalization
summary = summary.sort_values("T Mkt Cap", ascending=False)

# === Push to Excel with xlwings ===
wb_path = SCREENER  # change this
sheet_name = "data_Industries"    # or wherever you want it

with xw.App(visible=False) as app:
    wb = xw.Book(wb_path)
    sht = wb.sheets[sheet_name]
    sht.clear_contents()  # optional
    sht.range("A1").options(index=False).value = summary

winsound.PlaySound(SOUND, winsound.SND_FILENAME | winsound.SND_ASYNC)
time.sleep(2)