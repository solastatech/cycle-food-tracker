import os
import sys
import re
import json
from pathlib import Path
from dotenv import load_dotenv 
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from debug_util import debug, debug_df

# Auth + config
def make_client_from_env():
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
    
    # Include modern Sheets scope as well
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    key_input = os.environ["KEY_FILE_NAME"]
    if not key_input:
        raise RuntimeError(
            "Missing KEY_FILE_NAME. Set it to either a path to your service-account JSON "
            "or the full JSON string itself."
        )

    key_input = key_input.strip()

    # 🧠 Determine whether this is a raw JSON string or a file path
    if key_input.startswith("{"):
        debug("🔐 Detected inlined JSON credentials from GitHub Secrets.")
        credentials_dict = json.loads(key_input)
    else:
        debug(f"📄 Loading credentials from file: {key_input}")
        with open(key_input) as f:
            credentials_dict = json.load(f)

    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    client = gspread.authorize(creds)
    return client

# ========= CONFIG =========

# Name of the combined tab to (re)create. Set to None if you do NOT want to write back.
COMBINED_TAB_NAME    = "DB"

# Also save locally?
SAVE_LOCAL_PARQUET   = True
LOCAL_PARQUET_PATH   = Path("cycle_db.parquet")
SAVE_LOCAL_CSV       = False
LOCAL_CSV_PATH       = Path("cycle_db.csv")

# Match tabs called "Cycle 4", "cycle5", "CYCLE 07", etc.
CYCLE_SHEET_REGEX = re.compile(r"^cycle\s*0*\d+\s*$", re.IGNORECASE)
# ==========================


def list_cycle_worksheets(sh):
    """Return worksheets whose titles look like 'Cycle N' (robust to spaces/case/leading zeros)."""
    return [ws for ws in sh.worksheets() if CYCLE_SHEET_REGEX.match(ws.title.strip())]

def read_worksheet_to_df(ws: gspread.Worksheet) -> pd.DataFrame:
    """
    Read a single worksheet into a DataFrame.
    - Keeps all columns as strings where possible to avoid type weirdness, then lets pandas infer.
    - Drops fully empty rows and fully empty columns.
    - Adds `cycle_tab` column recording the worksheet title.
    """
    df = get_as_dataframe(
        ws,
        evaluate_formulas=True,
        header=0,           # first row is header
        dtype=None,         # allow inference; Sheets often mixes types
        numerize=True,      # convert numbers where possible
        include_index=False
    )

    # Remove all-empty rows/columns
    if df is None:
        df = pd.DataFrame()

    # Drop columns that are entirely NA/blank
    df = df.dropna(axis=1, how="all")
    # Drop rows that are entirely NA/blank
    df = df.dropna(axis=0, how="all")

    # Trim whitespace in column names
    df.columns = [str(c).strip() for c in df.columns]

    # Add provenance
    df["cycle_tab"] = ws.title.strip()

    return df

def categorise_poop_time(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip().lower()
    if s in {"", "-"}:
        return None
    
    # Word-only categories
    if "morning" in s:
        return "morning"
    if "afternoon" in s:
        return "afternoon"
    if any(w in s for w in ["evening", "night", "midnight"]):
        return "evening"
    
    # # If time
    # t = pd.to_datetime(s, errors="coerce", format="HH:MM")
    # if pd.isna(t):
    #     return None
    # t = t.time()
    
    # hour = t.hour
    # if 6 <= hour < 12:       # 6am–11:59am
    #     return "morning"
    # elif 12 <= hour < 18:    # 12pm–5:59pm
    #     return "afternoon"
    # else:                    # 6pm–5:59am
    #     return "evening"


def normalise_bedtime(x):
    """
    Normalize bedtime to HH:MM (24h).
    - Interprets plain numbers as PM (night hours).
      e.g. 11 -> 23:00, 1 -> 01:00 (after midnight), 0 -> 00:00
    - Accepts '11pm', '23', '23:15', etc.
    - Returns string 'HH:MM' or None if invalid.
    """
    s = str(x).strip().lower()

    # midnight shortcut
    if s in ["0", "00", "0000", "midnight", "12am"]:
        return "00:00"

    # If contains ":" or am/pm, let pandas handle
    if ":" in s or "am" in s or "pm" in s:
        try:
            t = pd.to_datetime(s, errors="coerce").time()
            if t:
                return f"{t.hour:02d}:{t.minute:02d}"
        except Exception:
            return None

    # If plain integer like '11'
    if s.isdigit():
        h = int(s)
        if h == 0:
            return "00:00"
        elif 1 <= h <= 5:   # early morning
            return f"{h:02d}:00"
        elif 6 <= h <= 11:  # assume night/PM
            return f"{h+12:02d}:00"
        elif 12 <= h <= 23:
            return f"{h:02d}:00"
        else:
            return None

    return None

def clean_steps(x):
    """
    Convert messy strings like '7.6k steps', '~5k', '(smartcoin 5.4k)', '6.2k steps'
    into integer step counts.
    
    Rules:
    - '7.6k' -> 7600
    - '5k'   -> 5000
    - '~5k'  -> 5000
    - '(smartcoin 5.4k)' -> 5400
    - '6.2k steps' -> 6200
    - plain integer '5000' -> 5000
    - blank/invalid -> None
    """
    if pd.isna(x):
        return None
    s = str(x).lower().strip()

    if not s or s in {"-", ""}:
        return None

    # Grab first number (with optional decimal + k suffix)
    m = re.search(r'(\d+(?:\.\d+)?)(k)?', s)
    if not m:
        return None

    num = float(m.group(1))
    return int(round(num * 1000))

def combine_cycles(sh) -> pd.DataFrame:
    cycle_sheets = list_cycle_worksheets(sh)

    if not cycle_sheets:
        raise RuntimeError("No tabs matching 'Cycle N' found. Check your sheet names or regex.")

    frames = []
    for ws in cycle_sheets:
        df = read_worksheet_to_df(ws)
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    # Union of columns via concat; pandas will align by column name
    combined = pd.concat(frames, ignore_index=True, sort=False)

    # Optional: common niceties if these columns exist
    # Standardise a 'Date' column to pandas datetime if present
    for candidate in ["Date", "date", "DATE"]:
        if candidate in combined.columns:
            combined["Date"] = pd.to_datetime(combined[candidate], errors="coerce", format="%d/%m/%Y")
            break

    # If you keep a Day column, make sure it's numeric
    for candidate in ["Cycle Day", "Day"]:
        if candidate in combined.columns:
            combined["Day_clean"] = pd.to_numeric(combined[candidate], errors="coerce")
            combined["Day_clean"] = combined["Day_clean"].astype("Int64")
            break
    # NEAT_clean Combined
    combined["NEAT_clean"] = None
    if any(c in combined.columns for c in ["NEAT / Walk", "NEAT_Walk", "NEAT"]):
        col = next(c for c in ["NEAT / Walk", "NEAT_Walk", "NEAT"] if c in combined.columns)
        combined["NEAT_clean"] = combined[col].apply(clean_steps)

    # Sort if we have helpful keys
    sort_cols = []
    if "Date" in combined.columns:
        sort_cols.append("Date")
    if "Day_clean" in combined.columns:
        sort_cols.append("Day_clean")
    sort_cols.append("cycle_tab")

    # 💩 Time and Bedtime
    if any(c in combined.columns for c in ["💩"]):
        col = next(c for c in ["💩"] if c in combined.columns)
        combined["Poop_time"] = combined[col].apply(categorise_poop_time)

    if any(c in combined.columns for c in ["Bedtime"]):
        col = next(c for c in ["Bedtime"] if c in combined.columns)
        combined["Bedtime_fmt"] = combined[col].apply(normalise_bedtime)

    # unique + keep order stable: ensure we don’t duplicate sort keys unnecessarily
    seen = set()
    sort_cols = [c for c in sort_cols if not (c in seen or seen.add(c))]
    try:
        combined = combined.sort_values(sort_cols).reset_index(drop=True)
    except Exception:
        combined = combined.reset_index(drop=True)

    combined = combined.rename(columns={"cycle_tab" : "Cycle No.", "Bedtime_fmt" : "Bedtime_clean",
                                        "NEAT / Walk" : "NEAT_Walk", "Mood / Notes" : "Mood_Notes"})

    combined = combined.reindex(["Date", "Cycle No.", "Day_clean", "Phase", "Calories",
                                 "Protein (g)", "Carbs (g)", "Fat (g)", "NEAT_clean",
                                 "Load-bearing", "Bedtime_clean", "Poop_time", "Mood_Notes"], axis=1)
    return combined

def ensure_or_replace_worksheet(dest_sh, title: str, rows: int = 1000, cols: int = 26) -> gspread.Worksheet:
    """
    Ensure the DEST sheet has a worksheet named `title`.
    If it exists, clear it; if not, create it. Then make sure it’s sized for the DataFrame.
    """
    try:
        ws = dest_sh.worksheet(title)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = dest_sh.add_worksheet(title=title, rows=max(rows, 1000), cols=max(cols, 26))

    # Resize to fit data (extra padding helps avoid 'out of bounds' set errors)
    ws.resize(rows=max(rows, 1000), cols=max(cols, 26))
    return ws

def write_to_dest(dest_sh, df: pd.DataFrame, title: str):
    if title is None:
        return
    rows_needed = len(df) + 10
    cols_needed = len(df.columns) + 2
    ws = ensure_or_replace_worksheet(dest_sh, title, rows=rows_needed, cols=cols_needed)
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)

def main():
    client = make_client_from_env()

    # SOURCE (your cycle tabs live here)
    source_url = os.environ.get("CYCLE_SOURCE_URL") or os.environ.get("CYCLE_TRACKER_URL")
    if not source_url:
        raise RuntimeError("Set CYCLE_SOURCE_URL (or legacy CYCLE_TRACKER_URL) to the source Google Sheet URL.")
    source_sh = client.open_by_url(source_url)

    # DEST (your tactical DB lives here)
    dest_url = os.environ.get("CYCLE_DEST_URL")
    if not dest_url:
        raise RuntimeError("Set CYCLE_DEST_URL to the destination Google Sheet URL for the tactical DB.")
    dest_sh = client.open_by_url(dest_url)

    # Build combined DF from all 'Cycle N' tabs in SOURCE
    combined = combine_cycles(source_sh)

    # Optional local snapshots
    if SAVE_LOCAL_PARQUET:
        combined.to_parquet(LOCAL_PARQUET_PATH, index=False)
        print(f"Saved Parquet → {LOCAL_PARQUET_PATH.resolve()}")
    if SAVE_LOCAL_CSV:
        combined.to_csv(LOCAL_CSV_PATH, index=False)
        print(f"Saved CSV     → {LOCAL_CSV_PATH.resolve()}")

    # Write to DEST tab (default 'DB' or override via env)
    dest_tab = os.environ.get("CYCLE_DEST_TAB", "DB")
    write_to_dest(dest_sh, combined, dest_tab)
    print(f"Wrote combined table to DEST: tab {dest_tab!r} (rows={len(combined)})")

if __name__ == "__main__":
    main()