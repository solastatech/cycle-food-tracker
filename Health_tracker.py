#!/usr/bin/env python
# coding: utf-8

# In[1]:

import numpy as np
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from config import (food_data_url,food_data_url_sheet, food_log_url,food_log_url_sheet, 
cycle_tracker_url, current_cycle, keyfilename)

# Authenticate
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(keyfilename, scope)
client = gspread.authorize(creds)

# Open the sheets
food_data_ws = client.open_by_url(food_data_url).worksheet(food_data_url_sheet)
food_log_ws = client.open_by_url(food_log_url).worksheet(food_log_url_sheet)
cycle_ws = client.open_by_url(cycle_tracker_url).worksheet(current_cycle)

# Load into DataFrames
food_data = pd.DataFrame(food_data_ws.get_all_records())
food_log = pd.DataFrame(food_log_ws.get_all_records())
cycle_df = pd.DataFrame(cycle_ws.get_all_records())

food_data.info()
food_log.info()
cycle_df.info()

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
for _, row in food_log.iterrows():
    if row['Manual Input'] == 'Y':
        records.append({
            "Date": row["Date"],
            "Calories": row["Kcal"],
            "Protein (g)": row["P"],
            "Carbs (g)": row["C"],
            "Fat (g)": row["F"]
        })
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
            records.append({
                "Date": row["Date"],
                "Calories": factor * float(ref["Kcal"]),
                "Protein (g)": factor * float(ref["Protein g"]),
                "Carbs (g)": factor * float(ref["Carb g"]),
                "Fat (g)": factor * float(ref["Fat g"])
            })
        else:
            print(f"⚠️ No match found for '{food_name}' — check name or alias.")


# In[4]:


# Group by Date
df = pd.DataFrame(records)
df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")
daily_totals = df.groupby("Date").sum().reset_index()

# display(daily_totals)
# Merge into cycle tracker
cycle_df["Date"] = pd.to_datetime(cycle_df["Date"], format="%d/%m/%Y", 
                                  errors="coerce")  # keeps blank rows as NaT
# display(cycle_df)


# In[5]:


# display(cycle_df)
merged = pd.merge(cycle_df, daily_totals, on="Date", how="left", suffixes=('', '_new'))
# display(merged)


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
# print(merged)


# In[7]:


values = [merged.columns.tolist()] + merged.values.tolist()
cycle_ws.update('A1', values)
print("💕 cycle tracker updated.")


# In[ ]:




