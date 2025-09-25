#!/usr/bin/env python
# coding: utf-8

import sys
import numpy as np
import pandas as pd
from datetime import datetime
from .debug_util import debug, debug_df
from .debug_config import verbose
from .gsheet_util import open_gsheet

def nutrition_calc():
    """
    Running the nutrition calculation.
    Returning 3 objects: a list of nutrition_to_append, records, and the master_table, to avoid running open_gsheet twice in the
    idempotency check.
    nutrition_to_append: has no Date column. Purpose is to update the nutrition values.
    records: has a Date column. Purpose is to merge with the read Gsheet of master_table.
    master_table: the master_table combining nutrition and activities
    """
    food_data, food_log, master_table = open_gsheet()
    # debug_df(food_data)
    # debug_df(food_log)

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
    
    # Group by date
    records = pd.DataFrame(records)
    return nutrition_to_append, records, master_table

if __name__ == "__main__":
    nutrition_to_append, records = nutrition_calc()
    debug(len(nutrition_to_append))
    debug_df(records)
    debug(len(records))
    