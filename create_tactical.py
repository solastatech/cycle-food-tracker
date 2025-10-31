import os
import sys
import json
import numpy as np
from dotenv import load_dotenv 
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from helpers.debug_util import debug, debug_df
from helpers.debug_config import verbose

verbose_safety = False # Set to True for production sheet check, then to False once confident
# Auth + config
# Only load .env if running outside GitHub Actions
if os.path.exists(".env"):
    load_dotenv(dotenv_path=".env")
    debug("✅ Loaded .env from local file")
else:
    debug("📡 Skipping .env load (GitHub Action mode)")

env = os.environ.get("ENV_NAME", "Unknown")
if env == "Unknown":
    debug("❌ ENV_NAME not found in environment. Aborting script.")
    sys.exit(1)

else:
    debug(f"🌿 Running in {env.upper()} mode 🚀")

debug("🧪 Sheet loaded:", os.environ.get("FOOD_LOG_URL_SHEET"))
if verbose_safety == True and os.environ.get("FOOD_LOG_URL_SHEET") == "Worksheet":
    raise RuntimeError("🛑 Refusing to write: You are about to overwrite the production sheet.")

key_input = os.environ["KEY_FILE_NAME"]

# 🧠 Determine whether this is a raw JSON string or a file path
if key_input.strip().startswith("{"):
    debug("🔐 Detected inlined JSON credentials from GitHub Secrets.")
    credentials_dict = json.loads(key_input)
else:
    debug(f"📄 Loading credentials from file: {key_input}")
    with open(key_input) as f:
        credentials_dict = json.load(f)

# Auth + config
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
client = gspread.authorize(creds)

# Other variables
master_table_url = os.environ["MASTER_TABLE_URL"]
master_table_url_sheet = os.environ["MASTER_TABLE_URL_SHEET"]
activity_log_url = os.environ["ACTIVITY_LOG_URL"]
activity_log_url_sheet = os.environ["ACTIVITY_LOG_URL_SHEET"]
tactical_db_url = os.environ["TACTICAL_DB_URL"]
tactical_db_url_sheet = os.environ["TACTICAL_DB_URL_SHEET"]

master_table_ws = client.open_by_url(master_table_url).worksheet(master_table_url_sheet)
activity_log_ws = client.open_by_url(activity_log_url).worksheet(activity_log_url_sheet)
tactical_db_ws = client.open_by_url(tactical_db_url).worksheet(tactical_db_url_sheet)

# Load into DataFrames
master_table = pd.DataFrame(master_table_ws.get_all_records())
activity_log = pd.DataFrame(activity_log_ws.get_all_records())

# Drop rows that are entirely NA/blank
master_table = master_table.dropna(axis=0, how="all")
activity_log = activity_log.dropna(axis=0, how="all")

# Trim whitespace in column names
master_table.columns = [str(c).strip() for c in master_table.columns]
activity_log.columns = [str(c).strip() for c in activity_log.columns]

debug_df(master_table)
debug_df(activity_log)

merged = master_table.merge(activity_log, on='Date', how='left').drop(['ID'], axis=1)
debug(merged)
debug_df(merged)  

# Menstrual phase
merged.loc[(merged["Menstruation"] == "Y"), "Phase"] = "Menstrual"

# Identify first menstrual day (today is Menstrual, yesterday is not)
merged["First mens day"] = False
merged.loc[(merged["Phase"] == "Menstrual") & ~(merged["Phase"].shift(1) == "Menstrual"), "First mens day"] = True
merged["Cycle No."] = merged["First mens day"].cumsum() + 3 # because my tracker starts at Cycle 4

# Identify the start of follicular day (today is not Menstrual, yesterday is the last day Menstrual)
merged.loc[~(merged["Menstruation"] == "Y") & (merged["Menstruation"].shift(1) == "Y"), "Phase"] = "Follicular"

# Phase fills as above
merged["Phase"] = merged["Phase"].replace("", np.nan).ffill()

# Phase ID and days
phase_order = {
    "Menstrual": 1,
    "Follicular": 2,
    "Ovulatory": 3,
    "Luteal": 4
}
merged["Phase_ID"] = merged["Phase"].map(phase_order)
merged["Cycle_Day"] = merged.groupby("Cycle No.").cumcount() + 1

# Steps measure the numbers
merged["Steps"] = merged["Steps"].astype(str).str.strip()
merged["Steps"] = merged["Steps"].apply(
    lambda x: float(str(x).replace("k", "")) * 1000 if str(x).endswith("k") else x
)

# Load-bearing change ✅ to Y
merged.loc[(merged["Load-bearing"] == "✅") | (merged["Load-bearing"] == "y") | (merged["Load-bearing"] == "hip mobility"), "Load-bearing"] = "Y"

# Bedtime as before or after midnight
merged["Bedtime"] = merged["Bedtime"].replace("midnight", "00:00")
merged["Bedtime"] = pd.to_datetime(merged["Bedtime"], format="%H:%M", errors="coerce")
mask = merged["Bedtime"].dt.hour.between(8,11)
merged.loc[mask, "Bedtime"] += pd.Timedelta(hours=12)
merged["Bedtime_clean"] = "Not OK"
merged.loc[(merged["Bedtime"].dt.hour >=20) | ((merged["Bedtime"].dt.hour == 0) & (merged["Bedtime"].dt.minute ==0)), "Bedtime_clean"] = "OK"
merged.loc[merged["Bedtime"].isna(), "Bedtime_clean"] = "No Data"
merged["Bedtime"] = merged["Bedtime"].dt.strftime("%H:%M")

debug(merged.tail(10))

## Wake-up time to calculate the total sleeping time
# Transform Date to be datelike column
merged["Date"] = pd.to_datetime(merged["Date"], format="%d/%m/%Y", errors="coerce").dt.date

# Transform Wake-up time to be time column
merged["Wake-up time"] = pd.to_datetime(merged["Wake-up time"], format="%H:%M", errors="coerce")
merged["Wake-up time"] = merged["Wake-up time"].dt.strftime("%H:%M")

# Transform bedtime to be full datetime
bed_dt = pd.to_datetime(merged["Date"].astype(str) + " " + merged["Bedtime"], errors="coerce")

# Transform midnight bedtime to be the next day's date
mask = bed_dt.dt.hour < 6
bed_dt.loc[mask] += pd.Timedelta(days=1)
debug(bed_dt)

# Shift Wake-up time column upwards
wakeup_next_dt = pd.to_datetime(merged["Date"].shift(-1).astype(str) + " " + merged["Wake-up time"].shift(-1), errors="coerce")
merged["Sleep_duration"] = (wakeup_next_dt - bed_dt)
merged["Sleep_duration"] = (merged["Sleep_duration"].dt.total_seconds()/3600).round(1)
debug(merged.tail(10))

# #Data clean-up on poop-time
merged = merged.replace("-", "")

# Filter for LookerStudio
cycle_to_display = 4
max_cycle = merged["Cycle No."].max()
# merged["Include_Last4"] = False
# merged.loc[(merged["Cycle No."] > (max_cycle - cycle_to_display)), "Include_Last4"] = True

merged["Include_Last4"] = merged["Cycle No."] > (max_cycle - cycle_to_display)

# Print to GSheet
set_with_dataframe(tactical_db_ws, merged, row=1,col=1, include_column_header=True)
print("💕 Tactical DB updated 💕")


