import os
import sys
from dotenv import load_dotenv
import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from .debug_util import debug, debug_df
from .debug_config import verbose

verbose_safety = True # Set to True for production sheet check, then to False once confident

def open_gsheet():
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
    food_data_url = os.environ["FOOD_DATA_URL"]
    food_data_url_sheet = os.environ["FOOD_DATA_URL_SHEET"]
    food_log_url = os.environ["FOOD_LOG_URL"]
    food_log_url_sheet = os.environ["FOOD_LOG_URL_SHEET"]
    master_table_url = os.environ["MASTER_TABLE_URL"]
    master_table_url_sheet = os.environ["MASTER_TABLE_URL_SHEET"]

    # Open the sheets
    debug("🧪 FOOD_DATA_URL from .env:", food_data_url)
    food_data_spreadsheet = client.open_by_url(food_data_url)

    #Debugger
    sheet_titles = [ws.title for ws in food_data_spreadsheet.worksheets()]
    debug("📋 Sheet titles found:", sheet_titles)
    debug("🧪 Target sheet from env var:", repr(food_data_url_sheet))
    debug("🔍 Matching sheet?", food_data_url_sheet in sheet_titles)
    debug("⚠️ Sheet name resolved from ENV:", food_log_url_sheet)

    food_data_ws = food_data_spreadsheet.worksheet(food_data_url_sheet)
    food_log_ws = client.open_by_url(food_log_url).worksheet(food_log_url_sheet)
    master_table_ws = client.open_by_url(master_table_url).worksheet(master_table_url_sheet)

    # Load into DataFrames
    food_data = pd.DataFrame(food_data_ws.get_all_records())
    food_log = pd.DataFrame(food_log_ws.get_all_records())
    master_table = pd.DataFrame(master_table_ws.get_all_records())

    debug_df(food_data)
    debug_df(food_log)
    debug_df(master_table)    

    # Cleanse food names
    food_data.columns = food_data.columns.str.strip()
    food_log.columns = food_log.columns.str.strip()
    master_table.columns = master_table.columns.str.strip()
    food_data['Food'] = food_data['Food'].str.strip().str.lower()
    food_data['Alias'] = food_data['Alias'].str.strip().str.lower()
    food_log['Food'] = food_log['Food'].str.strip().str.lower()

    return food_data, food_log, master_table