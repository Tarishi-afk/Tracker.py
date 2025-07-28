from flask import Flask, request, send_file
from datetime import datetime
import base64
import json
import os
import io
import pytz
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# === CONFIG ===
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
IST                   = pytz.timezone("Asia/Kolkata")
MAILTRACKING_WORKBOOK = "MailTracking"

# Transparent 1×1 GIF payload
PIXEL_BYTES = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00"
    b"\xFF\xFF\xFF!\xF9\x04\x01\x00\x00\x00\x00,"
    b"\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02"
    b"L\x01\x00;"
)

# === Google Sheets client ===
creds_info = json.loads(os.environ["GOOGLE_CREDS_JSON"])
creds      = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
gc         = gspread.authorize(creds)


def update_sheet(
    sheet: gspread.Worksheet,
    email: str,
    sender: str,
    timestamp: str,
    sheet_name: str = None,
    subject: str = None,
    timezone: str = None,
    start_date: str = None,
    template: str = None
):
    """
    Update existing row for `email` or append new.
    Ensures header row includes all columns, then updates/appends.
    """
    # 1) Ensure header row exists
    headers = sheet.row_values(1)
    if not headers:
        headers = [
            "Timestamp", "Status", "Email", "Open_count", "Last_Open",
            "From", "Subject", "Sheet_Name", "Timezone",
            "Start_Date", "Template"
        ]
        sheet.append_row(headers)

    # 2) Build header→index map
    col_map = {h: i for i, h in enumerate(headers)}

    # 3) Ensure all needed columns present
    required = [
        "Status", "Open_count", "Last_Open", "From", "Subject",
        "Sheet_Name", "Timezone", "Start_Date", "Template"
    ]
    for col in required:
        if col not in col_map:
            headers.append(col)
            col_map[col] = len(headers) - 1
            sheet.update_cell(1, len(headers), col)

    # 4) Read existing rows
    body = sheet.get_all_values()[1:]  # skip header

    # 5) Try update existing email row
    for ridx, row in enumerate(body, start=2):
        if row[col_map["Email"]].strip().lower() == email.lower():
            # increment open count
            count = int(row[col_map["Open_count"]] or "0") + 1
            sheet.update_cell(ridx, col_map["Open_count"] + 1, str(count))
            # update metadata
            sheet.update_cell(ridx, col_map["Last_Open"] + 1, timestamp)
            sheet.update_cell(ridx, col_map["Status"] + 1, "OPENED")
            sheet.update_cell(ridx, col_map["From"] + 1, sender)
            if subject:
                sheet.update_cell(ridx, col_map["Subject"] + 1, subject)
            if sheet_name:
                sheet.update_cell(ridx, col_map["Sheet_Name"] + 1, sheet_name)
            if timezone:
                sheet.update_cell(ridx, col_map["Timezone"] + 1, timezone)
            if start_date:
                sheet.update_cell(ridx, col_map["Start_Date"] + 1, start_date)
            if template:
                sheet.update_cell(ridx, col_map["Template"] + 1, template)
            return

    # 6) Append new row
    new_row = [""] * len(headers)
    new_row[col_map["Timestamp"]] = timestamp
    new_row[col_map["Status"]]    = "OPENED"
    new_row[col_map["Email"]]     = email
    new_row[col_map["Open_count"]]= "1"
    new_row[col_map["Last_Open"]]= timestamp
    new_row[col_map["From"]]     = sender
    if subject:
        new_row[col_map["Subject"]] = subject
    if sheet_name:
        new_row[col_map["Sheet_Name"]] = sheet_name
    if timezone:
        new_row[col_map["Timezone"]] = timezone
    if start_date:
        new_row[col_map["Start_Date"]] = start_date
    if template:
        new_row[col_map["Template"]] = template

    sheet.append_row(new_row)


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    """
    Tracking pixel endpoint.
    Expects base64-encoded JSON metadata in the URL path.
    """
    now       = datetime.now(IST)
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

    # Decode metadata token
    try:
        token   = path.split('.')[0]
        padded  = token + "=" * (-len(token) % 4)
        payload = base64.urlsafe_b64decode(padded.encode())
        info    = json.loads(payload).get("metadata", {})
    except Exception as e:
        app.logger.error("Invalid metadata: %s", e)
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    # Extract fields
    email       = info.get("email")
    sender      = info.get("sender")
    sheet_tab   = info.get("sheet")       # e.g. "USA", "Israel", etc.
    sheet_name  = info.get("sheet_name")  # original EMAIL-status sheet
    subject     = info.get("subject")
    timezone    = info.get("timezone")
    start_date  = info.get("date")        # "YYYY-MM-DD"
    template    = info.get("template")

    # Open MailTracking workbook & selected tab
    try:
        wb   = gc.open(MAILTRACKING_WORKBOOK)
        tabs = [ws.title for ws in wb.worksheets()]
        if not sheet_tab:
            sheet_tab = tabs[0] if tabs else "USA"
        if sheet_tab not in tabs:
            wb.add_worksheet(title=sheet_tab, rows="1000", cols="20")
        sheet = wb.worksheet(sheet_tab)
    except Exception as e:
        app.logger.error("Cannot open workbook/tab: %s", e)
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    # Record the open
    if email and sender:
        update_sheet(
            sheet,
            email=email,
            sender=sender,
            timestamp=timestamp,
            sheet_name=sheet_name,
            subject=subject,
            timezone=timezone,
            start_date=start_date,
            template=template
        )
        app.logger.info("Tracked open: %s → %s at %s", email, sheet_tab, timestamp)

    return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")


@app.route('/health')
def health():
    return "Tracker is live."

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
