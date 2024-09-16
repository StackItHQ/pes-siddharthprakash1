import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)

SHEET_ID = '1tv4ZIIvFcDGhtrLliZyT2ZvZG0F2pyRhqCS65aT-YXs'

# Open the spreadsheet
spreadsheet = client.open_by_key(SHEET_ID)

# List all worksheets
worksheets = spreadsheet.worksheets()

print("Available worksheets:")
for worksheet in worksheets:
    print(f"- {worksheet.title}")