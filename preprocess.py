import pandas as pd
import os
from tqdm import tqdm

def preprocess_vehicle_data(base_path, output_filename):
    """
    Finds, combines, cleans, and sorts all daily EV data files for a single vehicle.

    Args:
        base_path (str): The root directory for the vehicle's data (e.g., './Data/Vehicle 2').
        output_filename (str): The name for the final processed file (e.g., 'processed_vehicle_2.parquet').
    """
    # 1. Find all .xlsx data files recursively
    print(f"Searching for .xlsx files in: {base_path}")
    all_files = []
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith('.xlsx'):
                all_files.append(os.path.join(root, file))

    if not all_files:
        print("Error: No .xlsx files found. Please check the VEHICLE_BASE_PATH.")
        return

    print(f"Found {len(all_files)} files. Reading and combining...")

    # 2. Read each file into a DataFrame and append to a list
    df_list = []
    for file_path in tqdm(all_files, desc="Reading files"):
        try:
            # openpyxl engine is needed for .xlsx files
            df = pd.read_excel(file_path, engine='openpyxl')
            df_list.append(df)
        except Exception as e:
            print(f"Warning: Could not read file {file_path}. Error: {e}. Skipping.")

    if not df_list:
        print("Error: No dataframes were created. Halting process.")
        return
        
    # 3. Combine all DataFrames into a single one
    print("Concatenating all data into a single DataFrame...")
    full_df = pd.concat(df_list, ignore_index=True)
    print(f"Initial combined data has {len(full_df)} records.")

    # 4. Clean column names (remove units and extra spaces)
    cleaned_columns = {
        'record_time': 'record_time',
        'vehicle_state': 'vehicle_state',
        'charge_state': 'charge_state',
        'pack_voltage(V)': 'pack_voltage',
        'pack_current(A)': 'pack_current',
        'SOC(%)': 'soc',
        'max_cell_voltage (V)': 'max_cell_voltage',
        'min_cell_voltage (V)': 'min_cell_voltage',
        'max_probe_temperature (℃)': 'max_temp',
        'min_probe_temperature (℃)': 'min_temp'
    }
    full_df.rename(columns=cleaned_columns, inplace=True)

    # 5. Translate categorical columns from Chinese to English
    print("Translating state columns...")
    vehicle_state_map = {
        '车辆启动': 'Vehicle Start',
        '熄火': 'Engine Off'
    }
    charge_state_map = {
        '未充电': 'Not Charging',
        '停车充电': 'Parked Charging',
        '充电完成': 'Charging Complete'
    }
    full_df['vehicle_state'] = full_df['vehicle_state'].map(vehicle_state_map)
    full_df['charge_state'] = full_df['charge_state'].map(charge_state_map)
    
    # 6. Convert columns to appropriate data types
    print("Converting data types...")
    # Convert time column to datetime objects
    full_df['record_time'] = pd.to_datetime(full_df['record_time'], errors='coerce')

    # Identify numeric columns and convert them, coercing errors to NaN
    numeric_cols = [
        'pack_voltage', 'pack_current', 'soc', 'max_cell_voltage',
        'min_cell_voltage', 'max_temp', 'min_temp'
    ]
    for col in numeric_cols:
        full_df[col] = pd.to_numeric(full_df[col], errors='coerce')
        
    # 7. Drop rows with parsing errors (e.g., in datetime or numeric conversion)
    initial_rows = len(full_df)
    full_df.dropna(subset=['record_time'] + numeric_cols, inplace=True)
    rows_dropped = initial_rows - len(full_df)
    if rows_dropped > 0:
        print(f"Dropped {rows_dropped} rows due to missing/invalid values after conversion.")

    # 8. THE MOST IMPORTANT STEP: Sort data chronologically
    print("Sorting data by record_time...")
    full_df.sort_values(by='record_time', ascending=True, inplace=True)
    
    # Reset index after sorting
    full_df.reset_index(drop=True, inplace=True)

    # 9. Save the processed DataFrame to a highly efficient Parquet file
    print(f"Saving processed data to '{output_filename}'...")
    full_df.to_parquet(output_filename, index=False)
    
    print("\n--- Pre-processing Complete! ---")
    print(f"Final dataset has {len(full_df)} records.")
    print("Data saved successfully.")
    print("\nFirst 5 rows of processed data:")
    print(full_df.head())

# --- HOW TO USE ---
if __name__ == '__main__':
    # ** IMPORTANT: UPDATE THIS PATH **
    # Set the path to the root folder of the vehicle you want to process.
    # Use a forward slash '/' even on Windows.
    VEHICLE_BASE_PATH = './Data/Vehicle 2' # Example path

    # Define the output file name
    OUTPUT_FILENAME = 'processed_vehicle_2.parquet'

    preprocess_vehicle_data(VEHICLE_BASE_PATH, OUTPUT_FILENAME)