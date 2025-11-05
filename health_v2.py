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

# debug_df(food_data)
# debug_df(food_log)
# debug_df(master_table)    

# Cleanse food names
food_data.columns = food_data.columns.str.strip()
food_log.columns = food_log.columns.str.strip()
master_table.columns = master_table.columns.str.strip()
food_data['Food'] = food_data['Food'].str.strip().str.lower()
food_data['Alias'] = food_data['Alias'].str.strip().str.lower()
food_log['Food'] = food_log['Food'].str.strip().str.lower()

# Add the validation for empty Values
records = []
nutrition_to_append = []

for i, row in food_log.iterrows():
    if row['Manual Input'] == 'Y' or (row['Value']=='' and row['Manual Input']==''):
        records.append({
            "Date": row["Date"],
            "Kcal": row["Kcal"],
            "Protein (g)": row["P"],
            "Carb (g)": row["C"],
            "Fat (g)": row["F"]
        })
        nutrition_to_append.append([None, None, None, None])
    else:
        food_name = row['Food']
        value_str = str(row.get('Value', '')).strip()
        
        if not value_str:
            print(f"⚠️ Skipping: No value for '{food_name}' on {row['Date']}")
            nutrition_to_append.append([None, None, None, None])
            continue  # ensure no shifted rows
    
        try:
            value = float(value_str)
        except ValueError:
            print(f"⚠️ Skipping: Invalid number '{value_str}' for '{food_name}' on {row['Date']}")
            continue

        # Apply conversion if available
        conversion_str = str(row.get('Conversion', '')).strip()
        if conversion_str:
            try:
                value *= float(conversion_str)
            except ValueError:
                print(f"⚠️ Invalid conversion factor '{conversion_str}' for {food_name}, ignoring.")

        match = food_data[(food_data['Food'] == food_name) | (food_data['Alias'] == food_name)]
        if not match.empty:
            ref = match.iloc[0]
            factor = value / float(ref["Per Unit"])

            kcal = round(factor * float(ref["Kcal"]),1)
            protein = round(factor * float(ref["Protein g"]),1)
            carb = round(factor * float(ref["Carb g"]),1)
            fat = round(factor * float(ref["Fat g"]),1)
            
            records.append({
                "Date": row["Date"],
                "Kcal": kcal,
                "Protein (g)": protein,
                "Carb (g)": carb,
                "Fat (g)": fat
            })

            nutrition_to_append.append([kcal, protein, carb, fat])
        
        else:
            print(f"⚠️ No match found for '{food_name}' — check name or alias.")

# Match the length of the final append with the initial one to ensure no shifting rows
debug(f"Length of nutrition list to append:",len(nutrition_to_append))
debug(f"Length of food log:",len(food_log))
if not len(nutrition_to_append) == len(food_log):
    debug("❌ Different lengths between arrays. Potential shifted rows. Aborting mission.")
    sys.exit(1)

# Update the Food Log with nutrition values
start_row = 2
end_row = start_row + len(nutrition_to_append) - 1
update_range = f"G{start_row}:J{end_row}"
food_log_ws.update(update_range, nutrition_to_append)

# Group by date
debug(records)

current_df = pd.DataFrame(master_table)
all_records = pd.DataFrame(records)
debug_df(current_df)
debug_df(all_records)

debug(all_records["Date"])
all_records["Date"] = all_records["Date"].astype(str).str.strip()
all_records["Date"] = pd.to_datetime(all_records["Date"], format="%d/%m/%Y")
debug(all_records["Date"])

new_df = all_records.groupby("Date").sum().reset_index()

debug_df(new_df)
debug(new_df)

current_df['Date'] = pd.to_datetime(current_df['Date'], format="%d/%m/%Y", errors='coerce')
current_df['Kcal'] = pd.to_numeric(current_df['Kcal'], errors='coerce').round(0)

new_df['Kcal'] = pd.to_numeric(new_df['Kcal'], errors='coerce').round(0)

merged = new_df.merge(current_df[['Date','Kcal']].rename(columns={'Kcal':'Kcal_cur'}),
                      on='Date', how='left')
debug(merged)                                                    

k_new = merged['Kcal']
k_cur = merged['Kcal_cur']

mask_new    = k_cur.isna()                     # Date not present => INSERT
mask_same   = (~mask_new) & (k_new == k_cur)   # Date present & same Kcal => SKIP
mask_update = (~mask_new) & (k_new != k_cur)   # Date present & different Kcal => UPDATE

debug(f"Mask_new:", mask_new)
debug(f"Mask_update:", mask_update)
cols = ['Date', 'Kcal', 'Protein (g)', 'Carb (g)', 'Fat (g)']

to_insert = new_df.loc[mask_new, cols].copy()
to_update = new_df.loc[mask_update, cols].copy()
debug(f"To_insert:", to_insert)
debug(f"To_update:", to_update)

# --- read header from the sheet so we can place values in the correct columns
header = master_table_ws.row_values(1)  # row 1 is header
# map header name -> 1-based column index
col_idx = {name: i+1 for i, name in enumerate(header)}

# --- build Date -> sheet row map for UPDATEs
# use your existing current_df (already coerced)
cur_reset = current_df.reset_index(drop=True)
date_to_row = {}
for i, r in cur_reset.iterrows():
    if pd.notna(r.get('Date')):
        date_to_row[r['Date']] = i + 2   # +2 => header row + 1-based
debug(date_to_row)
# ==============================
# UPDATE: write ONLY the columns in `cols`
# ==============================
updates = []  # collect all cell updates here
for _, r in to_update.iterrows():
    dt = r['Date']
    if dt not in date_to_row:
        continue
    ws_row = date_to_row[dt]
    debug(ws_row)
    # write each selected column (skip ones missing from the sheet header)
    for c in cols:
        if c not in col_idx:
            continue
        cell_a1 = gspread.utils.rowcol_to_a1(ws_row, col_idx[c])
        debug(cell_a1)
        value = r[c]
        # Use '' if value is NaN so we don't write the string "nan"
        if pd.isna(value):
            value = ''
        elif type(value) == pd.Timestamp:
            value = value.strftime('%d/%m/%Y') 
        else:
            value 
        debug(value)
        updates.append({
            "range": cell_a1,
            "values": [[value]],
        })

debug(updates)
debug(len(updates))

# helper to chunk the updates so we don't send 3000 ranges in one call
def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

for chunk in chunked(updates, 60):  
    master_table_ws.batch_update(
        chunk,
        value_input_option='USER_ENTERED'
    )

# ==============================
# INSERT: append rows with your `cols` only,
# fill non-selected columns with '' (blank) so Notes etc are untouched later
# ==============================
if not to_insert.empty:
    full_rows = []
    for _, r in to_insert.iterrows():
        # start with blanks for the whole header
        row_vals = [''] * len(header)
        # put values ONLY for the columns you care about
        for c in cols:
            if c in col_idx:
                v = r[c]
                if pd.isna(v):
                    row_vals[col_idx[c] - 1] = '' 
                elif type(v) == pd.Timestamp:
                    row_vals[col_idx[c] - 1] = v.strftime('%d/%m/%Y')
                else:
                    row_vals[col_idx[c] - 1] = v
        full_rows.append(row_vals)

    # append in one shot (no NaNs, only blanks where you didn't provide data)
    # USER_ENTERED will let numbers be numbers; change if you need RAW
    master_table_ws.append_rows(full_rows, value_input_option='USER_ENTERED')



print(f"Inserts: {len(to_insert)} | Updates: {len(to_update)} | Skips: {mask_same.sum()}")