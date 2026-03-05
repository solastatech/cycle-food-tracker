import os
import sys
from dotenv import load_dotenv
import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from helpers.debug_util import debug, debug_df
from helpers.debug_config import verbose

verbose_safety = False # Set to True for production sheet check, then to False once confident

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
food_log_url = os.environ["FOOD_LOG_URL"]
food_log_url_sheet = os.environ["FOOD_LOG_URL_SHEET"]
master_table_url = os.environ["MASTER_TABLE_URL"]
master_table_url_sheet = os.environ["MASTER_TABLE_URL_SHEET"]

#Debugger
debug("⚠️ Sheet name resolved from ENV:", food_log_url_sheet)

food_log_ws = client.open_by_url(food_log_url).worksheet(food_log_url_sheet)
master_table_ws = client.open_by_url(master_table_url).worksheet(master_table_url_sheet)

# Load into DataFrames
food_log = pd.DataFrame(food_log_ws.get_all_records())
master_table = pd.DataFrame(master_table_ws.get_all_records()) 

# Forced to numeric
food_log["Saturated Fat g"] = (pd.to_numeric(food_log["Saturated Fat g"], errors='coerce').fillna(0.0))
food_log["Fibre g"] = (pd.to_numeric(food_log["Fibre g"], errors='coerce').fillna(0.0))
food_log["Sugar g"] = (pd.to_numeric(food_log["Sugar g"], errors='coerce').fillna(0.0))

# Group by date
food_log["Date"] = food_log["Date"].astype(str).str.strip()
food_log["Date"] = pd.to_datetime(food_log["Date"], format="%d/%m/%Y")
debug(food_log["Date"])

master_table["Date"] = pd.to_datetime(master_table["Date"], format="%d/%m/%Y", errors='coerce')

sat_map = food_log.groupby("Date")["Saturated Fat g"].sum()
fib_map = food_log.groupby("Date")["Fibre g"].sum()
sug_map = food_log.groupby("Date")["Sugar g"].sum()

# Inject value
master_table["Sat Fat (g)"] = master_table["Date"].map(sat_map).fillna(0)
master_table["Fibre (g)"] = master_table["Date"].map(fib_map).fillna(0)
master_table["Sugar (g)"] = master_table["Date"].map(sug_map).fillna(0) 
debug(master_table)

# Update the master table with nutrition values
start_row = 2
end_row = start_row + len(master_table["Sat Fat (g)"]) - 1
update_range = f"H{start_row}:J{end_row}"

cols_to_update = ["Sat Fat (g)", "Fibre (g)", "Sugar (g)"]

master_table_ws.update(range_name=update_range, values=master_table[cols_to_update].values.tolist())

