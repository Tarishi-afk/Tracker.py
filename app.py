from flask import Flask, send_file
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
IST = pytz.timezone("Asia/Kolkata")

# Transparent 1×1 GIF
PIXEL_BYTES = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00"
    b"\xFF\xFF\xFF!\xF9\x04\x01\x00\x00\x00\x00,"
    b"\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02"
    b"L\x01\x00;"
)

# === GSPREAD CLIENT SETUP ===
creds_info = json.loads(os.environ["GOOGLE_CREDS_JSON"])
creds      = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
gc         = gspread.authorize(creds)


def update_open_tracking(
    ws: gspread.Worksheet,
    email: str,
    sender: str,
    open_ts: str,
    sheet_name: str = None,
    subject: str = None,
    timezone: str = None,
    start_date: str = None,
    template: str = None
):
    """
    On a worksheet whose header row starts with:
      NAME | Email_ID | STATUS | SENDER | TIMESTAMP
    — ensure columns for Sheet_Name, Subject, Timezone,
      Start_Date, Template, and Open_timestamp exist,
    then find the row where Email_ID matches and update those.
    """
    # 1) Read or create header row
    headers = ws.row_values(1)
    if not headers:
        headers = [
            "NAME", "Email_ID", "STATUS", "SENDER", "TIMESTAMP",
            "Sheet_Name", "Subject", "Timezone", "Start_Date",
            "Template", "Open_timestamp"
        ]
        ws.append_row(headers)

    headers_lower = [h.strip().lower() for h in headers]
    required = [
        "NAME", "Email_ID", "STATUS", "SENDER", "TIMESTAMP",
        "Sheet_Name", "Subject", "Timezone", "Start_Date",
        "Template", "Open_timestamp"
    ]

    # 2) Ensure all required columns exist
    for col in required:
        if col.lower() not in headers_lower:
            col_idx = len(headers) + 1
            ws.update_cell(1, col_idx, col)
            headers.append(col)
            headers_lower.append(col.lower())

    # 3) Build header→column-index map (zero-based)
    col_map = {h.lower(): i for i, h in enumerate(headers)}

    # 4) Scan rows for matching Email_ID
    rows = ws.get_all_values()[1:]  # skip header
    for ridx, row in enumerate(rows, start=2):
        if row[col_map["email_id"]].strip().lower() == email.lower():
            # Update STATUS
            ws.update_cell(ridx, col_map["status"]+1, "OPENED")
            # Update open timestamp
            ws.update_cell(ridx, col_map["open_timestamp"]+1, open_ts)
            # Update metadata columns
            if sheet_name:
                ws.update_cell(ridx, col_map["sheet_name"]+1, sheet_name)
            if subject:
                ws.update_cell(ridx, col_map["subject"]+1, subject)
            if timezone:
                ws.update_cell(ridx, col_map["timezone"]+1, timezone)
            if start_date:
                ws.update_cell(ridx, col_map["start_date"]+1, start_date)
            if template:
                ws.update_cell(ridx, col_map["template"]+1, template)
            return

    # 5) If not found, append a new row
    new_row = [""] * len(headers)
    new_row[col_map["email_id"]]       = email
    new_row[col_map["status"]]         = "OPENED"
    new_row[col_map["sender"]]         = sender
    new_row[col_map["timestamp"]]      = open_ts
    if sheet_name:
        new_row[col_map["sheet_name"]] = sheet_name
    if subject:
        new_row[col_map["subject"]]    = subject
    if timezone:
        new_row[col_map["timezone"]]   = timezone
    if start_date:
        new_row[col_map["start_date"]] = start_date
    if template:
        new_row[col_map["template"]]   = template

    ws.append_row(new_row)


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    """
    Tracking pixel endpoint.
    Expects base64‐encoded JSON metadata at URL path.
    """
    now     = datetime.now(IST)
    open_ts = now.strftime("%Y-%m-%d %H:%M:%S")

    # Decode metadata
    try:
        token   = path.split('.')[0]
        padded  = token + "=" * (-len(token) % 4)
        payload = base64.urlsafe_b64decode(padded.encode())
        info    = json.loads(payload).get("metadata", {})
    except Exception as e:
        app.logger.error("Invalid metadata: %s", e)
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    email      = info.get("email")
    sender     = info.get("sender")
    workbook   = info.get("sheet")         # the workbook name (sheet2)
    sent_time  = info.get("sent_time")     # IST ISO timestamp
    sheet_name = info.get("sheet_name")
    subject    = info.get("subject")
    timezone   = info.get("timezone")
    start_date = info.get("date")
    template   = info.get("template")

    # Skip proxy hits < 7s
    if sent_time:
        try:
            sent_dt = datetime.fromisoformat(sent_time)
            if (now - sent_dt).total_seconds() < 7:
                app.logger.info("Ignoring early hit for %s", email)
                return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")
        except Exception:
            pass

    # Open the specified workbook & its first sheet
    try:
        wb = gc.open(workbook)
        ws = wb.sheet1
    except Exception as e:
        app.logger.error("Cannot open '%s': %s", workbook, e)
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    # Record the open, updating columns F+ (metadata & Open_timestamp)
    if email and sender:
        update_open_tracking(
            ws,
            email=email,
            sender=sender,
            open_ts=open_ts,
            sheet_name=sheet_name,
            subject=subject,
            timezone=timezone,
            start_date=start_date,
            template=template
        )
        app.logger.info("Logged open for %s in %s at %s", email, workbook, open_ts)

    return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")


@app.route('/health')
def health():
    return "Tracker is live."

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
