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
        sheet.update('A1', data)
    else:
        sheet.update('A1', [['column1', 'column2', 'column3']])  # Keep header even if data is empty

def update_db(cursor, data):
    cursor.execute("DELETE FROM data_table")  # Clear existing data
    for row in data[1:]:  # Skip header row
        # Replace empty strings with None for SQL NULL
        row = [None if cell.strip() == '' else cell for cell in row]
        cursor.execute("""
            INSERT INTO data_table (column1, column2, column3)
            VALUES (%s, %s, %s)
        """, row)
def main():
    connection = None
    cursor = None
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
            # Check for changes in Google Sheet
            sheet_data = get_sheet_data(sheet)
            db_data = get_db_data(cursor)

            if sheet_data != db_data:
                print("Updating database...")
                update_db(cursor, sheet_data)
                connection.commit()
                print("Database updated. Current contents:")
                print(get_db_data(cursor))

            # Fetch updated db_data
            db_data = get_db_data(cursor)
            if sheet_data != db_data:
                print("Updating Google Sheet...")
                update_sheet(sheet, db_data)
                print("Google Sheet updated.")

            time.sleep(10)  # Wait for 10 seconds before next check

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