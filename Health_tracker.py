#!/usr/bin/env python
# coding: utf-8

# In[1]:

import os
import sys
from dotenv import load_dotenv
import json
import numpy as np
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from debug_util import debug, debug_df
from debug_config import verbose

verbose_safety = True # it checks the Worksheet we're writing to

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

# Auth + config
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

keypath = os.environ["KEY_FILE_NAME"]
with open(keypath) as f:
    credentials_dict = json.load(f)

food_data_url = os.environ["FOOD_DATA_URL"]
food_data_url_sheet = os.environ["FOOD_DATA_URL_SHEET"]
food_log_url = os.environ["FOOD_LOG_URL"]
food_log_url_sheet = os.environ["FOOD_LOG_URL_SHEET"]
cycle_tracker_url = os.environ["CYCLE_TRACKER_URL"]
current_cycle = os.environ["CURRENT_CYCLE"]

creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
client = gspread.authorize(creds)

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
cycle_ws = client.open_by_url(cycle_tracker_url).worksheet(current_cycle)

# Load into DataFrames
food_data = pd.DataFrame(food_data_ws.get_all_records())
food_log = pd.DataFrame(food_log_ws.get_all_records())
cycle_df = pd.DataFrame(cycle_ws.get_all_records())

debug_df(food_data)
debug_df(food_log)
debug_df(cycle_df)

# Cleanse food names
food_data.columns = food_data.columns.str.strip()
food_log.columns = food_log.columns.str.strip()
cycle_df.columns = cycle_df.columns.str.strip()
food_data['Food'] = food_data['Food'].str.strip().str.lower()
food_data['Alias'] = food_data['Alias'].str.strip().str.lower()
food_log['Food'] = food_log['Food'].str.strip().str.lower()


# In[3]:

# Add the validation for empty Values
records = []
nutrition_to_append = []

for i, row in food_log.iterrows():
    if row['Manual Input'] == 'Y':
        records.append({
            "Date": row["Date"],
            "Calories": row["Kcal"],
            "Protein (g)": row["P"],
            "Carbs (g)": row["C"],
            "Fat (g)": row["F"]
        })
        nutrition_to_append.append([None, None, None, None])
    else:
        food_name = row['Food']
        value_str = str(row.get('Value', '')).strip()
        
        if not value_str:
            print(f"⚠️ Skipping: No value for '{food_name}' on {row['Date']}")
            continue  # skip this row if no value entered
    
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
                "Calories": kcal,
                "Protein (g)": protein,
                "Carbs (g)": carb,
                "Fat (g)": fat
            })

            nutrition_to_append.append([kcal, protein, carb, fat])
            # # Update the Food Log sheet inline
            # update_range = f"G{i+2}:J{i+2}"  # Adjust column letters if your sheet differs
            # food_log_ws.update(update_range, [[kcal, protein, carb, fat]])
        else:
            print(f"⚠️ No match found for '{food_name}' — check name or alias.")

# Update the Food Log with nutrition values
start_row = 2
end_row = start_row + len(nutrition_to_append) - 1
update_range = f"G{start_row}:J{end_row}"

food_log_ws.update(update_range, nutrition_to_append)

# In[4]:


# Group by Date
df = pd.DataFrame(records)
df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")
daily_totals = df.groupby("Date").sum().reset_index()
# Merge into cycle tracker
cycle_df["Date"] = pd.to_datetime(cycle_df["Date"], format="%d/%m/%Y", 
                                  errors="coerce")  # keeps blank rows as NaT



# In[5]:


debug(cycle_df)
merged = pd.merge(cycle_df, daily_totals, on="Date", how="left", suffixes=('', '_new'))
debug(merged)


# In[6]:


# Update only if new data is available
for col in ["Calories", "Protein (g)", "Carbs (g)", "Fat (g)"]:
    new_col = col + "_new"
    if new_col in merged:
        merged[col] = pd.to_numeric(merged[new_col], errors='coerce').combine_first(pd.to_numeric(merged[col], errors='coerce'))
        merged[col] = merged[col].round(0)
        merged.drop(columns=[new_col], inplace=True)
        merged
# Convert only the 'Date' column to string (with your desired format)
merged["Date"] = merged["Date"].dt.strftime("%d/%m/%Y")
merged = merged.replace([pd.NA, np.nan], '')
debug(merged)


# In[7]:


values = [merged.columns.tolist()] + merged.values.tolist()
cycle_ws.update('A1', values)
print("💕 cycle tracker updated 💕")


# In[ ]:




