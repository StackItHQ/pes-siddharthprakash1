import os
import time
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector
from mysql.connector import Error
from concurrent.futures import ThreadPoolExecutor

# Google Sheets setup
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)
sheet = client.open_by_key('1tv4ZIIvFcDGhtrLliZyT2ZvZG0F2pyRhqCS65aT-YXs').worksheet('Sheet1')

# MySQL setup
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Harshil#1711',
    'database': 'superjoin_sync'
}

# Track last sync timestamps
last_sync_google = None
last_sync_mysql = None

# Polling interval (in seconds)
POLL_INTERVAL = 5


def get_mysql_connection():
    """Establishes a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("MySQL: Connection established")
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    return None


def fetch_google_sheet_data():
    """Fetch all data from Google Sheets."""
    print("Fetching data from Google Sheets...")
    data = sheet.get_all_records()
    print(f"Fetched {len(data)} rows from Google Sheets")
    return data


def fetch_mysql_data():
    """Fetch all data from MySQL."""
    print("Fetching data from MySQL...")
    connection = get_mysql_connection()
    if connection:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM sync_table")  # Assuming your table is named 'sync_table'
        data = cursor.fetchall()
        connection.close()
        print(f"Fetched {len(data)} rows from MySQL")
        return data
    return []


def sync_google_to_mysql():
    """Sync changes from Google Sheets to MySQL."""
    global last_sync_google
    print("Syncing Google Sheets data to MySQL...")
    google_data = fetch_google_sheet_data()

    connection = get_mysql_connection()
    if connection:
        cursor = connection.cursor()
        for row in google_data:
            # Assuming each row has a unique 'id' field
            id_ = row['id']
            cursor.execute("SELECT * FROM sync_table WHERE id = %s", (id_,))
            result = cursor.fetchone()

            if result:  # Update if the row exists
                print(f"Updating row in MySQL: ID = {id_}")
                cursor.execute("""
                    UPDATE sync_table SET name = %s, value = %s WHERE id = %s
                """, (row['name'], row['value'], id_))
            else:  # Insert new row
                print(f"Inserting new row in MySQL: ID = {id_}")
                cursor.execute("""
                    INSERT INTO sync_table (id, name, value) VALUES (%s, %s, %s)
                """, (id_, row['name'], row['value']))
        connection.commit()
        connection.close()

        print("Google Sheets to MySQL sync completed.")
        last_sync_google = datetime.now()


def sync_mysql_to_google():
    """Sync changes from MySQL to Google Sheets."""
    global last_sync_mysql
    print("Syncing MySQL data to Google Sheets...")
    mysql_data = fetch_mysql_data()

    google_data = fetch_google_sheet_data()
    google_ids = [row['id'] for row in google_data]

    for row in mysql_data:
        id_ = row['id']
        cell = sheet.find(str(id_))

        if cell:  # If the id is found in Google Sheets
            print(f"Updating row in Google Sheets: ID = {id_}")
            sheet.update_cell(cell.row, 2, row['name'])  # Assuming name is in column 2
            sheet.update_cell(cell.row, 3, row['value'])  # Assuming value is in column 3
        else:  # If id is not found, insert the row into Google Sheets
            print(f"Inserting new row in Google Sheets: ID = {id_}")
            new_row = [row['id'], row['name'], row['value']]
            sheet.append_row(new_row)

    print("MySQL to Google Sheets sync completed.")
    last_sync_mysql = datetime.now()


def main_sync_loop():
    """Main loop that runs synchronization tasks continuously."""
    print("Starting main sync loop...")
    with ThreadPoolExecutor(max_workers=2) as executor:
        while True:
            print("Starting sync cycle...")

            # Run Google-to-MySQL sync
            google_to_mysql = executor.submit(sync_google_to_mysql)

            # Run MySQL-to-Google sync
            mysql_to_google = executor.submit(sync_mysql_to_google)

            # Wait for both tasks to complete
            google_to_mysql.result()
            mysql_to_google.result()

            print("Sync cycle completed.")
            print(f"Sleeping for {POLL_INTERVAL} seconds before next cycle...\n")
            # Sleep before polling again
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main_sync_loop()
