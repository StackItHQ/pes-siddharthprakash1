import os
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector
from mysql.connector import Error

# Google Sheets setup
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)

# MySQL setup
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Harshil#1711',
    'database': 'superjoin_sync'
}

SHEET_ID = '1tv4ZIIvFcDGhtrLliZyT2ZvZG0F2pyRhqCS65aT-YXs'
SHEET_NAME = 'Sheet1'

def get_sheet_data(sheet):
    data = sheet.get_all_values()
    # Remove empty rows and duplicates, keeping only the first occurrence
    cleaned_data = []
    seen = set()
    for row in data:
        if any(cell.strip() for cell in row):  # Check if row is not empty
            row_tuple = tuple(row)
            if row_tuple not in seen:
                cleaned_data.append(row)
                seen.add(row_tuple)
    return cleaned_data

def get_db_data(cursor):
    cursor.execute("SELECT column1, column2, column3 FROM data_table")
    # Convert None to empty string for comparison with sheet data
    return [('column1', 'column2', 'column3')] + [tuple('' if x is None else x for x in row) for row in cursor.fetchall()]

def update_sheet(sheet, data):
    if data:
        # Update only the necessary rows in Google Sheets
        # Assuming column1 is unique and you need to match it
        for i, row in enumerate(data[1:], start=2):  # Skip header row
            sheet.update(f'A{i}', [row])
    else:
        sheet.update('A1', [['column1', 'column2', 'column3']])  # Keep header even if data is empty
 
def get_updated_db_data(cursor, last_sync_time):
    query = "SELECT column1, column2, column3, last_updated FROM data_table WHERE last_updated > %s"
    cursor.execute(query, (last_sync_time,))
    return [('column1', 'column2', 'column3')] + [tuple('' if x is None else x for x in row[:3]) for row in cursor.fetchall()]

def update_db(cursor, data, connection, max_retries=5):
    retry_count = 0
    while retry_count < max_retries:
        try:
            cursor.execute("DELETE FROM data_table")  # Clear existing data
            for row in data[1:]:  # Skip header row
                # Replace empty strings with None for SQL NULL
                row = [None if cell.strip() == '' else cell for cell in row]
                cursor.execute("""
                    INSERT INTO data_table (column1, column2, column3)
                    VALUES (%s, %s, %s)
                """, row)
            connection.commit()  # Commit transaction after successful execution
            break  # If successful, exit the loop
        except mysql.connector.Error as e:
            if e.errno == 1213:  # Deadlock error
                print(f"Deadlock detected. Retrying... ({retry_count+1}/{max_retries})")
                retry_count += 1
                time.sleep(1)  # Small delay before retrying
            else:
                raise e  # Raise any other errors

from datetime import datetime

def main():
    connection = None
    cursor = None
    last_sync_time = datetime.min  # Initialize with an old date

    try:
        # Connect to MySQL
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        print("Successfully connected to MySQL database")

        # Open the specific sheet by ID and name
        spreadsheet = client.open_by_key(SHEET_ID)
        print(f"Successfully opened spreadsheet: {spreadsheet.title}")
        
        sheet = spreadsheet.worksheet(SHEET_NAME)
        print(f"Successfully opened worksheet: {sheet.title}")

        while True:
            # Get current data from Google Sheets and updated data from Database
            sheet_data = get_sheet_data(sheet)
            updated_db_data = get_updated_db_data(cursor, last_sync_time)

            # Sync from Google Sheets to Database if needed
            db_data = get_db_data(cursor)
            if sheet_data != db_data:
                print("Updating database from Google Sheets...")
                update_db(cursor, sheet_data, connection)
                connection.commit()
                print("Database updated from Google Sheets. Current contents:")
                print(get_db_data(cursor))

            # Sync from Database to Google Sheets
            if updated_db_data:  # Only update Sheets if there are new changes in DB
                print("Updating Google Sheets from Database...")
                update_sheet(sheet, updated_db_data)
                print("Google Sheets updated from Database.")

            # Update last_sync_time after sync is done
            last_sync_time = datetime.now()

            # Wait for 10 seconds before next check
            time.sleep(10)

    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Error: Could not find a spreadsheet with ID {SHEET_ID}")
    except gspread.exceptions.WorksheetNotFound:
        print(f"Error: Could not find a worksheet named '{SHEET_NAME}' in the spreadsheet")
    except gspread.exceptions.APIError as e:
        print(f"Google Sheets API error: {e}")
    except mysql.connector.Error as e:
        print(f"MySQL error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print("MySQL connection closed")

if __name__ == "__main__":
    main()