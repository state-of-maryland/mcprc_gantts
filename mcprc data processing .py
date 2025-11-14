import smartsheet
import os
import csv
import pandas as pd
import numpy as np
import pytz
from datetime import date, timedelta, datetime
import holidays as pyholidays
from socrata_replace.socrata_py_replace import ReplaceBot
from secret_data import userpass
from datetime import date 

# =================================================================
# === CONFIGURATION & CREDENTIALS ===
# =================================================================

SMARTSHEET_ACCESS_TOKEN = "" #left blank, please use your access token from smartsheets
TARGET_TZ = 'US/Eastern'
eastern_tz = pytz.timezone(TARGET_TZ)

today_tz_aware = pd.to_datetime(date.today()).tz_localize(eastern_tz).normalize()
RAW_OUTPUT_FILENAME = 'compiled_smartsheet_data_raw.csv'
CALCULATED_OUTPUT_FILENAME = 'calculated_smartsheet_data.csv'

PORTAL = "data.maryland.gov"
DATASET_UID = '' #leaving blank to prevent accidental upserts

try:
    smartsheet_client = smartsheet.Smartsheet(SMARTSHEET_ACCESS_TOKEN)
    print("Smartsheet client initialized.")
except Exception as e:
    print(f"Error initializing Smartsheet client. Details: {e}")
    exit()

# Base columns for all date processing
DATE_COLUMNS = [
    'Submitted Date',
    'Communication Start Date',
    'Initial Assessment Date',
    'Review Start Date',
    'Final Resolution Date',
    'Expiration Date'
]


ELAPSED_DATE_COLUMNS = DATE_COLUMNS[:-1]

EXPIRATION_COLUMN = DATE_COLUMNS[-1] 

MILESTONE_DATE_COLUMNS = DATE_COLUMNS[:-1]
# =================================================================
# === FUNCTION DEFINITIONS ===
# =================================================================

def get_sheet_id_by_name(sheet_name):
    """ Finds the ID of a sheet given its name. """
    if not sheet_name: return None
    try:
        response = smartsheet_client.Sheets.list_sheets(include_all=True)
        for sheet in response.data:
            if sheet.name.strip() == sheet_name.strip():
                return sheet.id
    except smartsheet.exceptions.ApiError as e:
        print(f"API Error finding sheet ID for '{sheet_name}': {e.error.message}")
    return None

def get_sheet_data(sheet_id):
    """ Retrieves the full row and cell data for a specific sheet ID and returns it as a pandas DataFrame. """
    try:
        sheet_object=smartsheet_client.Sheets.get_sheet(sheet_id)
    except smartsheet.exceptions.ApiError as e:
        print(e.error.message)
        return pd.DataFrame()
    
    print(f"\n--- Processing Sheet: {sheet_object.name} (ID: {sheet_id}) ---")
    column_map = {col.id: col.title for col in sheet_object.columns}
    column_titles = list(column_map.values())
    all_rows_data = []
    
    for row in sheet_object.rows:
        row_data = {}
        has_data = False
        for cell in row.cells:
            column_title = column_map.get(cell.column_id, "Unknown Column")
            value = cell.display_value if cell.display_value is not None else cell.value
            row_data[column_title] = value
            if value is not None and value != '':
                has_data = True
                
        if has_data:
            all_rows_data.append(row_data)
            
    print(f"Total NON-BLANK rows added to DataFrame: {len(all_rows_data)}")
    df = pd.DataFrame(all_rows_data, columns=column_titles)
    df['Sheet Name'] = sheet_object.name
    df['Sheet ID'] = sheet_id
    return df

def count_maryland_business_days(start_date, end_date):
    """
    Calculates the total number of business days (Mon-Fri, excluding 
    US federal and Maryland state holidays) between a start_date and an end_date.
    
    Returns the difference in business days (End Date - Start Date).
    """
    if not (isinstance(start_date, date) and isinstance(end_date, date)):
        return np.nan
        
    if pd.isna(start_date) or pd.isna(end_date):
        return np.nan

    if start_date > end_date:
        return np.nan 

    md_holidays = pyholidays.US(state='MD', years=range(start_date.year, end_date.year + 2))

    business_day_count = 0
    current_date = start_date + timedelta(days=1)

    while current_date <= end_date:
        is_weekend = current_date.weekday() >= 5 
        is_holiday = current_date in md_holidays
        
        if not is_weekend and not is_holiday:
            business_day_count += 1
            
        current_date += timedelta(days=1)

    return business_day_count

# =================================================================
# === DATA RETRIEVAL AND INITIAL CLEANUP ===
# =================================================================

all_dfs = []
csv_name = 'Sheet Names.csv'
try:
    df_names = pd.read_csv(csv_name)
except FileNotFoundError:
    print(f"❌ Error: Required file '{csv_name}' not found.")
    exit()

for names in df_names['Sheet Name']:
    sheet_id = get_sheet_id_by_name(names)
    if sheet_id:
        current_df = get_sheet_data(sheet_id)
        if not current_df.empty:
            all_dfs.append(current_df)

if all_dfs:
    final_compiled_df = pd.concat(all_dfs, ignore_index=True, sort=False)
    final_compiled_df.fillna('', inplace=True)
    
    
    final_compiled_df['Duration'] = final_compiled_df['Duration'].replace('', np.nan)
    final_compiled_df['Duration_Cleaned'] = final_compiled_df['Duration'].astype(str).str.replace('d', '', regex=False)
    final_compiled_df['Duration_Days'] = pd.to_numeric(final_compiled_df['Duration_Cleaned'], errors='coerce')
    final_compiled_df['Duration'] = final_compiled_df['Duration_Days'].astype('Int64')
    final_compiled_df = final_compiled_df.drop(['Duration_Days', 'Duration_Cleaned'], axis=1)
    
    
    final_compiled_df.to_csv(RAW_OUTPUT_FILENAME, index=False, encoding='utf-8')
    print("\n" + "="*50)
    print(f"Compilation Complete! Total Sheets Compiled: {len(all_dfs)}")
    print(f"Final DataFrame shape: {final_compiled_df.shape}")
    print("="*50)

else:
    print("\n--- No sheets were successfully compiled. Exiting. ---")
    exit()

# =================================================================
# === DATE STANDARDIZATION (CRITICAL STEP 1) ===
# =================================================================
# This converts columns to Datetime Objects, which is necessary for the next step.

print("\nStarting date standardization...")
df_processed = final_compiled_df.copy() 

for col in DATE_COLUMNS:
    
    df_processed[col] = pd.to_datetime(
        df_processed[col],
        errors='coerce',
        utc=True
    )
    
    if pd.api.types.is_datetime64_any_dtype(df_processed[col]):
        df_processed[col] = df_processed[col].dt.tz_convert(TARGET_TZ)
    else:
        df_processed[col] = pd.NaT

print("Date Standardization Complete. Columns are now Datetime Objects.")


# =================================================================
# === BUSINESS DAY CALCULATION (CRITICAL STEP 2) ===
# =================================================================

# --- CRITICAL STEP 2: CALCULATE BUSINESS DAYS AND EXPIRATION DAYS ---


today_date = pd.to_datetime(date.today()) 

print("\nStarting business day and expiration calculations...")


for start_col in ELAPSED_DATE_COLUMNS:
    
    new_col_name = start_col.replace(' Date', '').replace(' ', '_').lower() + "_business_days_to_today"
    
    df_processed[new_col_name] = df_processed.apply(
        lambda row: count_maryland_business_days(
            row[start_col].date(),
            date.today()
        )
        if pd.notna(row[start_col]) else np.nan,
        axis=1
    )
    
    print(f"Calculated ELAPSED business days for '{start_col}'.")

# --- 2B. REMAINING/OVERDUE Days (Today -> Expiration Date) ---


expiration_col_name = EXPIRATION_COLUMN.replace(' Date', '').replace(' ', '_').lower() + "_days_remaining"

df_processed[expiration_col_name] = (
    df_processed[EXPIRATION_COLUMN].dt.normalize() - today_tz_aware
).dt.days

df_processed[expiration_col_name] = df_processed[expiration_col_name].astype('Int64')

print(f"Calculated REMAINING days for '{EXPIRATION_COLUMN}'.")

# =================================================================
# === NEW SECTION: 2C. DAYS BETWEEN MILESTONES (BUSINESS DAYS) ===
# =================================================================

print("\nStarting calculations for business days between milestones...")


for i in range(len(MILESTONE_DATE_COLUMNS)):
    for j in range(i + 1, len(MILESTONE_DATE_COLUMNS)):
        start_col = MILESTONE_DATE_COLUMNS[i]
        end_col = MILESTONE_DATE_COLUMNS[j]

        start_name = start_col.replace(' Date', '').replace(' ', '_').lower()
        end_name = end_col.replace(' Date', '').replace(' ', '_').lower()
        new_col_name = f"{start_name}_to_{end_name}_business_days"

        df_processed[new_col_name] = df_processed.apply(
            lambda row: count_maryland_business_days(
                row[start_col].date(),
                row[end_col].date()
            )
            if pd.notna(row[start_col]) and pd.notna(row[end_col]) else np.nan,
            axis=1
        )
        print(f"Calculated business days for '{start_col}' to '{end_col}'.")

all_calculated_cols_new = [col for col in df_processed.columns if col.endswith("_business_days")]
print("All new day-difference calculations complete.")

all_calculated_cols = [
    col for col in df_processed.columns
    if col.endswith("_business_days_to_today") or col.endswith("_days_remaining") or col.endswith("_business_days")
]

for col in all_calculated_cols:
    df_processed[col] = df_processed[col].astype('Int64', errors='ignore')

print("Conversion of calculated columns to whole numbers complete.")

print("\n--- Converting Date Objects back to ISO strings for export. ---")
for col in DATE_COLUMNS:
    if pd.api.types.is_datetime64_any_dtype(df_processed[col]):
        df_processed[col] = df_processed[col].apply(
            lambda x: x.isoformat() if pd.notna(x) else np.nan
        )
    
df_processed.to_csv(CALCULATED_OUTPUT_FILENAME, index=False, encoding='utf-8')
print(f"Final Calculated DataFrame saved to '{CALCULATED_OUTPUT_FILENAME}'.")