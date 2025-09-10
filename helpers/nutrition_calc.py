#!/usr/bin/env python
# coding: utf-8

import sys
import numpy as np
import pandas as pd
from datetime import datetime
from .debug_util import debug, debug_df
from .debug_config import verbose
from .gsheet_util import init_env, gsheet_client, open_gsheet

def main():
    init_env()
    client = gsheet_client()
    food_data, food_log = open_gsheet(client)
    debug_df(food_data)
    debug_df(food_log)

    # Cleanse food names
    food_data.columns = food_data.columns.str.strip()
    food_log.columns = food_log.columns.str.strip()
    food_data['Food'] = food_data['Food'].str.strip().str.lower()
    food_data['Alias'] = food_data['Alias'].str.strip().str.lower()
    food_log['Food'] = food_log['Food'].str.strip().str.lower()

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
            
            else:
                print(f"⚠️ No match found for '{food_name}' — check name or alias.")

    # Match the length of the final append with the initial one to ensure no shifting rows
    debug(f"Length of nutrition list to append:",len(nutrition_to_append))
    debug(f"Length of food log:",len(food_log))
    if not len(nutrition_to_append) == len(food_log):
        debug("❌ Different lengths between arrays. Potential shifted rows. Aborting mission.")
        sys.exit(1)
    return nutrition_to_append, records

if __name__ == "__main__":
    nutrition_to_append, records = main()
    debug(len(nutrition_to_append))
    debug(nutrition_to_append)
