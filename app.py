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
IST                   = pytz.timezone("Asia/Kolkata")
MAILTRACKING_WORKBOOK = "MailTracking"   # << fixed workbook name

PIXEL_BYTES = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00"
    b"\xFF\xFF\xFF!\xF9\x04\x01\x00\x00\x00\x00,"
    b"\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02"
    b"L\x01\x00;"
)

# === GOOGLE CREDS ===
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
    - Assumes row1 has: NAME | Email_ID | STATUS | SENDER | TIMESTAMP
    - Ensures columns F–K exist:
        Sheet_Name | Subject | Timezone | Start_Date | Template | Open_timestamp
    - Finds row where Email_ID matches, updates those cells.
    """
    # 1) read existing header row (or create it if missing)
    headers = ws.row_values(1)
    if not headers:
        headers = [
            "NAME","Email_ID","STATUS","SENDER","TIMESTAMP",
            "Sheet_Name","Subject","Timezone","Start_Date","Template","Open_timestamp"
        ]
        ws.append_row(headers)

    lower = [h.lower() for h in headers]
    required = [
        "sheet_name","subject","timezone","start_date","template","open_timestamp"
    ]
    # 2) append any missing metadata columns
    for col in required:
        if col not in lower:
            ws.update_cell(1, len(headers)+1, col.replace("_"," ").title())
            headers.append(col)
            lower.append(col)

    # 3) build map header→zero‐based index
    idx = {h.lower(): i for i,h in enumerate(headers)}

    # 4) scan rows for matching Email_ID
    rows = ws.get_all_values()[1:]
    for r, row in enumerate(rows, start=2):
        if row[idx["email_id"]].strip().lower() == email.lower():
            # update OPEN_TIMESTAMP
            ws.update_cell(r, idx["open_timestamp"]+1, open_ts)
            # update other metadata
            if sheet_name:
                ws.update_cell(r, idx["sheet_name"]+1, sheet_name)
            if subject:
                ws.update_cell(r, idx["subject"]+1, subject)
            if timezone:
                ws.update_cell(r, idx["timezone"]+1, timezone)
            if start_date:
                ws.update_cell(r, idx["start_date"]+1, start_date)
            if template:
                ws.update_cell(r, idx["template"]+1, template)
            return

    # 5) if not found, append a new row (optional)
    new_row = [""] * len(headers)
    new_row[ idx["email_id"]        ] = email
    new_row[ idx["status"]          ] = "OPENED"
    new_row[ idx["sender"]          ] = sender
    new_row[ idx["timestamp"]       ] = open_ts
    if sheet_name:
        new_row[ idx["sheet_name"] ] = sheet_name
    if subject:
        new_row[ idx["subject"]    ] = subject
    if timezone:
        new_row[ idx["timezone"]   ] = timezone
    if start_date:
        new_row[ idx["start_date"] ] = start_date
    if template:
        new_row[ idx["template"]   ] = template

    ws.append_row(new_row)


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    now    = datetime.now(IST)
    open_ts = now.strftime("%Y-%m-%d %H:%M:%S")

    # 1) decode metadata payload
    try:
        token   = path.split('.')[0]
        padded  = token + "=" * (-len(token) % 4)
        payload = base64.urlsafe_b64decode(padded.encode())
        info    = json.loads(payload).get("metadata", {})
    except Exception as e:
        app.logger.error("Bad metadata: %s", e)
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    email       = info.get("email")
    sender      = info.get("sender")
    sheet_tab   = info.get("sheet")      # user‐chosen tab under MailTracking
    sent_time   = info.get("sent_time")  # IST ISO timestamp
    sheet_name  = info.get("sheet_name")
    subject     = info.get("subject")
    timezone    = info.get("timezone")
    start_date  = info.get("date")
    template    = info.get("template")

    # 2) skip early hits under 7s
    if sent_time:
        try:
            sent_dt = datetime.fromisoformat(sent_time)
            if (now - sent_dt).total_seconds() < 7:
                app.logger.info("Ignoring early hit for %s", email)
                return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")
        except Exception:
            pass

    # 3) open MailTracking and the specific worksheet
    try:
        wb = gc.open(MAILTRACKING_WORKBOOK)
        if sheet_tab not in [ws.title for ws in wb.worksheets()]:
            wb.add_worksheet(title=sheet_tab, rows="1000", cols="20")
        ws = wb.worksheet(sheet_tab)
    except Exception as e:
        app.logger.error("Cannot open workbook/tab: %s", e)
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    # 4) record the open
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
        app.logger.info("Logged open for %s in %s at %s", email, sheet_tab, open_ts)

    return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")


@app.route('/health')
def health():
    return "Tracker is live."

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
