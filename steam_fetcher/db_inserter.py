import pyodbc
import pandas as pd
import time
import numpy as np

# Define the default date
DEFAULT_DATE = pd.Timestamp('1900-01-01')

def insert_csv_to_db(csv_path, status_callback, progress_callback):
    """
    Connects to Azure SQL DB, validates CSV columns, handles missing/default values,
    and processes each row individually: inserting new titles, updating if the CSV
    row is newer, or skipping if the database row is newer or the same.

    Args:
        csv_path (str): Path to the input CSV file.
        status_callback (function): Function to update the main status label.
        progress_callback (function): Function to update progress (e.g., rows processed).

    Returns:
        tuple: (bool, str) indicating success/failure and a status message.
    """
    server = 'certdatavalidation.database.windows.net'
    database = 'DataValidation'
    target_table_name = '[dbo].[SteamOSHandheldInfo]'
    key_column = 'Title'
    date_column = 'LastChange'

    rows_processed_total = 0
    rows_inserted = 0
    rows_updated = 0
    rows_skipped_older = 0
    rows_skipped_empty_title = 0

    conn_str = (
        f'Driver={{ODBC Driver 18 for SQL Server}};'
        f'Server=tcp:{server},1433;'
        f'Database={database};'
        f'Authentication=ActiveDirectoryInteractive;'
        f'Encrypt=yes;'
        f'TrustServerCertificate=no;'
        f'Connection Timeout=60;'
    )

    conn = None
    cursor = None

    try:
        status_callback(f"Connecting to Azure SQL: {server}/{database}...")
        conn = pyodbc.connect(conn_str)
        # Set autocommit to False to manage transactions for delete+insert
        conn.autocommit = False
        cursor = conn.cursor()
        status_callback("Connected to database.")

        # --- Get Target Table Schema ---
        status_callback(f"Fetching schema for table: {target_table_name}...")
        sql_get_columns = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION;
        """
        table_name_only = target_table_name.split('.')[-1].strip('[]')
        cursor.execute(sql_get_columns, table_name_only)
        db_schema_info = list(cursor.fetchall())
        if not db_schema_info:
            return False, f"Could not retrieve schema for table {target_table_name}"

        db_columns_raw = [row.COLUMN_NAME for row in db_schema_info]
        db_columns_lower = [col.lower() for col in db_columns_raw]
        status_callback(f"Target table columns: {', '.join(db_columns_raw)}")

        # --- Read CSV File ---
        status_callback(f"Reading CSV file: {csv_path}...")
        try:
            df = pd.read_csv(csv_path, encoding='utf-8', keep_default_na=False, na_values=[''])
            original_csv_rows = len(df)
            status_callback(f"Read {original_csv_rows} rows from CSV.")
        except FileNotFoundError:
             return False, f"Error: CSV file not found at {csv_path}"
        except UnicodeDecodeError:
             return False, f"Error: Could not decode the file using UTF-8. Please ensure the file is saved in UTF-8 format."
        except Exception as e:
             return False, f"Error reading CSV file: {e}"

        csv_columns_raw = df.columns.tolist()
        csv_columns_lower = [col.lower() for col in csv_columns_raw]
        status_callback(f"CSV columns: {', '.join(csv_columns_raw)}")

        # --- Validate Columns ---
        status_callback("Validating CSV columns against target table...")
        if key_column.lower() not in csv_columns_lower:
             return False, f"Error: CSV is missing required key column '{key_column}'."
        if date_column.lower() not in csv_columns_lower:
             return False, f"Error: CSV is missing required date column '{date_column}'."

        if csv_columns_lower != db_columns_lower:
            return False, "Error: CSV columns do not match target table columns (name or order)."
        status_callback("Column validation successful.")

        # --- Data Type Conversion and Cleaning ---
        status_callback("Cleaning and converting data types...")
        numeric_sql_types = ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney', 'bit']
        date_sql_types = ['date']
        datetime_sql_types = ['datetime', 'datetime2', 'smalldatetime']
        time_sql_types = ['time']

        df_clean = pd.DataFrame()

        for db_col_name in db_columns_raw:
            csv_col_name = df.columns[db_columns_lower.index(db_col_name.lower())]
            db_type_info = next((info for info in db_schema_info if info.COLUMN_NAME == db_col_name), None)
            db_type = db_type_info.DATA_TYPE.lower() if db_type_info else ''
            series = df[csv_col_name].copy()
            series.replace(r'^\s*$', None, regex=True, inplace=True)

            if any(num_type in db_type for num_type in numeric_sql_types):
                if db_type == 'bit':
                    bool_map = {'true': 1, 'false': 0, '1': 1, '0': 0, 'yes': 1, 'no': 0, '': None}
                    series = series.astype(str).str.lower().map(bool_map).astype(float)
                series = pd.to_numeric(series, errors='coerce')
            elif any(d_type in db_type for d_type in date_sql_types + datetime_sql_types + time_sql_types):
                series_dt = pd.to_datetime(series, errors='coerce', dayfirst=True)
                series = series_dt.fillna(DEFAULT_DATE) # Fill NaT/None with default Timestamp
            # else: # String types
                pass

            df_clean[db_col_name] = series

        df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)
        status_callback("Data cleaning finished.")

        # --- Filter out rows with empty Title ---
        initial_clean_rows = len(df_clean)
        title_is_missing = df_clean[key_column].isnull() | (df_clean[key_column].astype(str).str.strip() == '')
        df_clean = df_clean[~title_is_missing]
        rows_skipped_empty_title = initial_clean_rows - len(df_clean)
        if rows_skipped_empty_title > 0:
            status_callback(f"Skipped {rows_skipped_empty_title} rows due to missing '{key_column}'.")

        if df_clean.empty:
             return True, f"No valid rows remaining after filtering for missing '{key_column}'. Processed {original_csv_rows} CSV rows."

        rows_to_process = len(df_clean)
        status_callback(f"Processing {rows_to_process} valid rows individually...")
        progress_callback(0, rows_to_process)

        # --- Define SQL Statements ---
        # *** ADD CAST to VARCHAR(MAX) for LOWER() compatibility with TEXT type ***
        sql_select = f"SELECT [{date_column}] FROM {target_table_name} WHERE LOWER(CAST([{key_column}] AS VARCHAR(MAX))) = LOWER(?)"
        sql_delete = f"DELETE FROM {target_table_name} WHERE LOWER(CAST([{key_column}] AS VARCHAR(MAX))) = LOWER(?)"
        sql_insert = f"INSERT INTO {target_table_name} ({', '.join([f'[{c}]' for c in db_columns_raw])}) VALUES ({', '.join(['?'] * len(db_columns_raw))})"

        # --- Process Rows Individually ---
        for index, row in df_clean.iterrows():
            rows_processed_total += 1
            current_title = row[key_column]
            current_date = row[date_column] # This is a Timestamp object
            row_values = tuple(row[db_columns_raw]) # Ensure order matches db_columns_raw

            try:
                # Check if title exists
                cursor.execute(sql_select, current_title)
                existing_row = cursor.fetchone()
                existing_date = existing_row[0] if existing_row else None

                if existing_date is None:
                    # Title not found, insert new row
                    status_callback(f"Row {rows_processed_total}/{rows_to_process}: Inserting new title '{current_title[:30]}...'")
                    cursor.execute(sql_insert, row_values)
                    conn.commit() # Commit insert
                    rows_inserted += 1
                else:
                    # Title found, compare dates
                    # Ensure existing_date is comparable (handle potential non-datetime types from DB)
                    try:
                        if not isinstance(existing_date, pd.Timestamp):
                             existing_date = pd.Timestamp(existing_date)
                    except Exception:
                         # If DB date is invalid, treat CSV as newer (or log warning)
                         existing_date = DEFAULT_DATE # Or some very old date

                    if current_date > existing_date:
                        # CSV row is newer, delete existing and insert new
                        status_callback(f"Row {rows_processed_total}/{rows_to_process}: Updating title '{current_title[:30]}...' (newer date)")
                        cursor.execute(sql_delete, current_title)
                        cursor.execute(sql_insert, row_values)
                        conn.commit() # Commit delete+insert transaction
                        rows_updated += 1
                    else:
                        # DB row is newer or same, skip
                        status_callback(f"Row {rows_processed_total}/{rows_to_process}: Skipping title '{current_title[:30]}...' (DB newer or same)")
                        rows_skipped_older += 1

            except pyodbc.Error as ex:
                conn.rollback() # Rollback transaction on error
                # *** ADDED DETAILED LOGGING ***
                sqlstate = ex.args[0]
                error_message_detail = f"Database error (SQLSTATE: {sqlstate}) processing row {rows_processed_total} (Title: {current_title}): {ex}"
                print(f"ERROR: {error_message_detail}") # Print detailed error to console
                # Log the problematic row data
                print(f"--- Failing Row Data ---")
                try:
                    # Attempt to print as dictionary, handle potential issues
                    print(row.to_dict())
                except Exception as print_ex:
                    print(f"(Error printing row data: {print_ex})")
                    print(row) # Fallback to printing the Series object
                print(f"-----------------------")
                # Return the detailed message to the GUI
                return False, error_message_detail
            except Exception as e:
                conn.rollback()
                error_message = f"Unexpected error processing row {rows_processed_total} (Title: {current_title}): {e}"
                print(f"ERROR: {error_message}") # Print general error too
                print(f"--- Failing Row Data ---")
                try:
                    print(row.to_dict())
                except Exception as print_ex:
                    print(f"(Error printing row data: {print_ex})")
                    print(row)
                print(f"-----------------------")
                return False, error_message

            # Update progress bar
            progress_callback(rows_processed_total, rows_to_process)
            # Optional: Add a small sleep to prevent overwhelming the DB/GUI?
            # time.sleep(0.01)


        final_message = (f"Successfully processed {original_csv_rows} CSV rows.\n"
                         f"Skipped {rows_skipped_empty_title} rows due to missing '{key_column}'.\n"
                         f"Processed {rows_processed_total} valid rows:\n"
                         f"- Inserted: {rows_inserted}\n"
                         f"- Updated: {rows_updated}\n"
                         f"- Skipped (DB newer/same): {rows_skipped_older}")
        status_callback("Individual row processing completed.")
        return True, final_message

    except pyodbc.Error as ex:
         # ... (connection error handling) ...
         return False, f"Database connection or initial query error: {ex}" # Simplified
    except Exception as e:
        return False, f"An unexpected error occurred: {e}"
    finally:
        if conn:
            # Ensure autocommit is reset if necessary, though closing handles it
            # conn.autocommit = True
            conn.close()
            status_callback("Database connection closed.")
        # Cursor is closed automatically when connection is closed

# Example usage (for testing purposes, not called by GUI directly)
import os  # Ensure os is imported if using __main__
if __name__ == '__main__':
    def dummy_status(msg):
        print(f"STATUS: {msg}")

    def dummy_progress(current, total):
        if total > 0:
            print(f"PROGRESS: {current}/{total} ({current/total*100:.1f}%)")
        else:
            print(f"PROGRESS: {current}/{total}")

    # Replace with a valid path to your test CSV
    test_csv = 'path/to/your/problematic_data.csv'
    if os.path.exists(test_csv):
        success, message = insert_csv_to_db(test_csv, dummy_status, dummy_progress)
        print(f"\nResult: Success={success}, Message={message}")
    else:
        print(f"Test CSV not found: {test_csv}")

