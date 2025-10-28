import gspread
from google.oauth2.service_account import Credentials
import requests
import time

# === CONFIG ===
SPREADSHEET_NAME = "Attendes-Eventbrite"
TOKEN = "ZF2TIWHOCHJCRNY6REGD"  # Replace with ENV var or secret in production

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
CREDS = Credentials.from_service_account_file("/etc/secrets/service_account.json", scopes=SCOPES)
gc = gspread.authorize(CREDS)
sheet = gc.open(SPREADSHEET_NAME)

# === EVENTS TO SYNC ===
EVENTS = {
    "Cardiff": {
        "Register Your Interest": "1328273324269",
        "Visitor": "690193285697",
        "Exhibitor": "690013116807",
        "Speaker": "690194037947",
        "Investor's Pitch Pest": "1328277165759"
    },
    "Isle Of Man": {
        "Register Your Interest": "1010366081147",
        "Visitor": "690175141427",
        "Exhibitor": "690009164987",
        "Speaker": "690176034097",
        "Investor's Pitch Pest": "1323531029939"
    },
    "Birmingham": {
        "Register Your Interest": "1312765309379",
        "Visitor": "1113497564189",
        "Exhibitor": "1059486987099",
        "Speaker": "1306666949019",
        "Investor's Pitch Pest": "1306668714299"
    },
    "Doncaster": {
        "Visitor": "1257850939019",
        "Exhibitor": "1257133934439",
        "Speaker": "1257134586389",
        "Investor's Pitch Pest": "1446973971099"
    },
    "London": {
        "Visitor": "690169945887",
        "Exhibitor": "689999997567",
        "Speaker": "690170708167",
        "Investor's Pitch Pest": "1321066117319"
    },
    "Corporate Wellbeing": {
        "Visitor": "1320080178349"
    },
    "Dubai": {
        "Register Your Interest": "1034764788337",
        "Visitor": "908228454757",
        "Exhibitor": "1434437082929",
        "Speaker": "1042895688077"
    }
}

# === Desired Headers ===
DESIRED_HEADERS = [
    "Order no.", "Order Date", "First Name", "Surname", "Email", "Quantity", "Price Tier", "Ticket Type",
    "Attendee no.", "Group", "Order Type", "Currency", "Total Paid", "Fees Paid", "Eventbrite Fees",
    "Eventbrite Payment Processing", "Attendee Status", "Home Address 1", "Home Address 2",
    "Home City", "County of Residence", "Home Postcode", "Home Country", "Mobile Phone",
    "Where did you hear about the show?", "Do you want to be an Exhibitor?", "Do you want to be a Speaker?",
    "I accept Exhibition Terms & Conditions and User Privacy Policy", "Please Specify Channel",
    "Please select your event preference", "Please select your event preference_2", "Job Title",
    "Company", "Website", "Campaign"
]

def get_attendees(event_id):
    attendees = []
    page = 1
    headers = {"Authorization": f"Bearer {TOKEN}"}
    while True:
        url = f"https://www.eventbriteapi.com/v3/events/{event_id}/attendees/"
        resp = requests.get(url, headers=headers, params={"page": page})
        data = resp.json()
        if "attendees" not in data:
            print("❌ Error fetching:", data)
            break
        attendees.extend(data["attendees"])
        if not data["pagination"]["has_more_items"]:
            break
        page += 1
    return attendees

def parse_attendee(att, campaign):
    profile = att.get("profile", {})
    answers = {ans["question"].strip(): ans.get("answer", "") for ans in att.get("answers", [])}
    return [
        att.get("order_id", ""),
        att.get("created", ""),
        profile.get("first_name", ""),
        profile.get("last_name", ""),
        profile.get("email", ""),
        att.get("quantity", ""),
        att.get("costs", {}).get("base_price", {}).get("display", ""),
        att.get("ticket_class_name", ""),
        att.get("id", ""),
        att.get("team", {}).get("name", ""),
        att.get("barcodes", [{}])[0].get("status", ""),
        att.get("costs", {}).get("gross", {}).get("currency", ""),
        att.get("costs", {}).get("gross", {}).get("value", ""),
        att.get("costs", {}).get("eventbrite_fee", {}).get("value", ""),
        att.get("costs", {}).get("eventbrite_fee", {}).get("display", ""),
        att.get("costs", {}).get("payment_fee", {}).get("display", ""),
        att.get("status", ""),
        profile.get("address_1", ""),
        profile.get("address_2", ""),
        profile.get("city", ""),
        profile.get("region", ""),
        profile.get("postal_code", ""),
        profile.get("country", ""),
        profile.get("phone", ""),
        answers.get("Where did you hear about the show?", ""),
        answers.get("Do you want to be an Exhibitor?", ""),
        answers.get("Do you want to be a Speaker?", ""),
        answers.get("I accept Exhibition Terms & Conditions and User Privacy Policy", ""),
        answers.get("Please Specify Channel", ""),
        answers.get("Please select your event preference", ""),
        answers.get("Please select your event preference", ""),  # duplicated question label
        answers.get("Job Title", ""),
        answers.get("Company", ""),
        answers.get("Website", ""),
        campaign
    ]

def ensure_tab(tab_name):
    try:
        return sheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"🆕 Creating sheet for {tab_name}")
        return sheet.add_worksheet(title=tab_name, rows="1000", cols="50")

def append_data(tab_name, rows):
    if not rows:
        return

    ws = ensure_tab(tab_name)
    existing = ws.get_all_values()

    first_row = existing[0] if existing else []

    # Add headers if missing
    if not any(col in first_row for col in DESIRED_HEADERS):
        print(f"📝 Adding headers to sheet: {tab_name}")
        ws.clear()
        ws.append_row(DESIRED_HEADERS)

    # Build set of (first_name.lower(), surname.lower()) for deduplication
    existing_names = set()
    for row in existing[1:]:  # skip header
        if len(row) >= 4:
            fname = row[2].strip().lower()
            lname = row[3].strip().lower()
            existing_names.add((fname, lname))

    # Filter rows: skip if Info Requested or duplicate name
    filtered_rows = []
    for row in rows:
        fname = row[2].strip()
        lname = row[3].strip()

        if fname.lower() == "info requested" or lname.lower() == "info requested":
            continue

        name_key = (fname.lower(), lname.lower())
        if name_key in existing_names:
            continue

        filtered_rows.append(row)
        existing_names.add(name_key)

    if filtered_rows:
        ws.append_rows(filtered_rows, value_input_option="RAW")
        print(f"✅ {len(filtered_rows)} rows appended to {tab_name}")
    else:
        print(f"ℹ️ No new rows to append for {tab_name}")


def main():
    for location, campaigns in EVENTS.items():
        all_rows = []
        for campaign, event_id in campaigns.items():
            if event_id == "NA":
                continue
            print(f"📥 Fetching {campaign} - {location}")
            attendees = get_attendees(event_id)
            for att in attendees:
                row = parse_attendee(att, campaign)
                all_rows.append(row)
            time.sleep(1)
        append_data(location, all_rows)
#Email sending function
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import imaplib

def send_attendee_email(sheet_id):
    # === CONFIG ===
    SMTP_SERVER = 'mail.b2bgrowthexpo.com'
    SMTP_PORT = 587
    SMTP_USER = 'nagendra@b2bgrowthexpo.com'
    SMTP_PASSWORD = 'D@shwood0404'
    IMAP_SERVER = 'mail.b2bgrowthexpo.com'  # e.g., imap.gmail.com
    TO_EMAILS = ['Nagendra@b2bgrowthhub.com']

    # === EXPORT SHEET AS CSV ===
    export_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
    response = requests.get(export_url)

    if response.status_code != 200:
        print("❌ Failed to download spreadsheet:", response.status_code)
        return

    # === CREATE EMAIL ===
    msg = MIMEMultipart()
    msg['From'] = SMTP_USER
    msg['To'] = ", ".join(TO_EMAILS)
    msg['Subject'] = "📊 Eventbrite Attendee Sheet Updated"

    body = f"""
Hi Sir,

Hope you're doing well.

The attendee data for all expo locations has been successfully updated and compiled in the shared Google Sheet.  
Each location (e.g., Cardiff, Isle of Man, Birmingham, etc.) has its own dedicated tab within the sheet.

🕒 Last Updated: {time.strftime('%d-%m-%Y %I:%M %p')}  
🔗 Sheet Link: https://docs.google.com/spreadsheets/d/{sheet_id}

Let us know if you need anything else or have questions.

Best regards,  
Automation Bot  
B2B Growth Expo Team
"""

    msg.attach(MIMEText(body, 'plain'))

    # Attach the file
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(response.content)
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename="Attendees.csv"')
    msg.attach(part)

    # === SEND EMAIL ===
    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)

        # === SAVE TO SENT FOLDER USING IMAP ===
        try:
            imap = imaplib.IMAP4_SSL(IMAP_SERVER)
            imap.login(SMTP_USER, SMTP_PASSWORD)
            imap.append("INBOX.Sent", '', imaplib.Time2Internaldate(time.time()), msg.as_bytes())
            imap.logout()
            print("📤 Email saved to Sent folder via IMAP")
        except Exception as e:
            print(f"❌ IMAP Error while saving to Sent folder: {e}")

        server.quit()
        print("📧 Email sent successfully")
    except Exception as e:
        print("❌ Failed to send email:", str(e))


if __name__ == "__main__":
    while True:
        print("🚀 Starting sync job")
        main()
        send_attendee_email(sheet.id)
        print("✅ Job complete. Sleeping for 24 hours...")
        time.sleep(86400)  # Sleep for 24 hours (in seconds)
