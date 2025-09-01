from flask import Flask, jsonify
import os
import requests
import pandas as pd
import time
import json
import datetime
from datetime import timedelta, timezone
from dotenv import load_dotenv
import smtplib
from email.message import EmailMessage
from email.utils import formataddr
from io import StringIO
import re
import threading

app = Flask(__name__)

# === Load ENV ===
load_dotenv()
HUBSPOT_KEY = os.getenv("HUBSPOT_TOKEN")   
SMTP_USER = os.getenv("EMAIL_USERNAME")       
SMTP_PASS = os.getenv("EMAIL_PASSWORD")       
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

HEADERS = {"Authorization": f"Bearer {HUBSPOT_KEY}"}

# --- Safe GET ---
def safe_get(url, headers, retries=3, timeout=10):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 200:
                return response
            else:
                print(f"⚠️ API error {response.status_code} | {url}")
        except Exception as e:
            print(f"⚠️ Request failed: {e}")
        time.sleep(1)
    return None

# --- Fetch deals ---
def fetch_all_deals(since_ts, limit=100):  
    all_deals, total_deals = [], 0
    url = "https://api.hubapi.com/crm/v3/objects/deals/search"
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "hs_lastmodifieddate",
                        "operator": "GTE",
                        "value": since_ts
                    }
                ]
            }
        ],
        "properties": ["dealname", "hubspot_owner_id", "pipeline", "amount"],
        "limit": limit
    }

    while True:
        response = requests.post(url, headers=HEADERS, json=payload)
        if not response:
            break
        data = response.json()
        all_deals.extend(data.get("results", []))
        total_deals += len(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if after:
            payload["after"] = after
        else:
            break
        time.sleep(0.3)

    return all_deals, total_deals

# --- Fetch property history ---
def fetch_property_history(deal_id, property_name):
    url = f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}?propertiesWithHistory={property_name}"
    response = safe_get(url, HEADERS)
    if not response:
        return []
    return [
        {"value": item.get("value"), "timestamp": item.get("timestamp")}
        for item in response.json()
        .get("propertiesWithHistory", {})
        .get(property_name, [])
    ]

# --- Helpers ---
def parse_hs_timestamp(ts_str):
    try:
        if ts_str.endswith("Z"):
            ts_str = ts_str.replace("Z", "+00:00")
        return datetime.datetime.fromisoformat(ts_str)
    except Exception:
        return datetime.datetime.fromtimestamp(int(ts_str) / 1000)

def format_date(value):
    if not value:
        return ""
    try:
        return datetime.datetime.fromtimestamp(int(value)/1000).strftime("%d-%m-%Y")
    except:
        return value

def map_value(value):
    if str(value).lower() == "true":
        return "Hot"
    elif str(value).lower() == "false":
        return "Warm"
    return value

PIPELINE_MAP = {
    "678921109": "Warehousing Pipeline",
    "679793780": "D2C Freight Pipeline",
    "679336879": "Tech Pipeline",
    "678993838": "Packaging Pipeline",
    "681388447": "PTL Freight Pipeline",
    "705978438": "FTL Pipeline"
}

# OWNER_MAP = {
#     "81151298": "Divij Wadhwa",
#     "26693784": "Shagun Tyagi",
#     "45964886": "Tushar Mittal",
#     "51443636": "Yavanika Sharma",
#     "75406284": "Ankit Rakhecha",
#     "76286666": "Gourav Rathi",
#     "76420249": "Vivek Mishra",
#     "76420252": "Praneet Vajpayee",
#     "76420254": "Manisha Mehani",
#     "76420257": "Satish Valla",
#     "76420259": "Dr. Ashvini Jakhar",
#     "76420261": "Siddhartha Agarwal",
#     "76420267": "Narinder Kakkar",
#     "76420274": "Richa Malhan",
#     "76420298": "Dheeraj Karki",
#     "76427044": "Pulkit Garg",
#     "76747606": "Sai Anusha",
#     "77791508": "Riya Mandhan",
#     "78633331": "Kuldeep Thakran",
#     "78633390": "Kshitij Magre",
#     "78633391": "Saurabh Jain",
#     "79601223": "Piyush Kukreja",
#     "79807296": "Lokesh Marwah",
#     "80078439": "Nazrul Islam",
#     "80102798": "Durgesh Kumawat",
#     "80521827": "Shaikh Faraz Quamar",
#     "80646448": "Vishal Labh",
#     "80940969": "Nikhil Patle",
#     "80978396": "Noyal Saharan",
#     "81098157": "Sushma Chauhan",
#     "81481361": "Siddharth Sharma",
#     "81481362": "Prabhmeet Kaur",
#     "81481363": "Nikhil Sharma",
#     "81481364": "Kumar Anshuman",
#     "81481365": "Rishikesh Tiwari",
#     "81481366": "Ankur Malviya",
#     "81481367": "Tarun Ramesh",
#     "81481368": "Rudra Tamrakar",
#     "81481369": "Rahul Pant",
#     "81481370": "Rohan Baisoya",
#     "81513348": "Bhupinder Singh",
#     "82052050": "Falguni Ghosh"
# }
with open("owner_map.json", "r") as f:
    OWNER_MAP = json.load(f)

with open("deal_stage_map.json", "r") as f:
    DEAL_STAGE_MAP = json.load(f)
# DEAL_STAGE_MAP = {
#     "678921109": {
#         "995964754": "RFI Sent",
#         "995964755": "As is Study Done",
#         "996085338": "Solution Design",
#         "996085339": "Commercial Shared",
#         "996085340": "Commercial Negotiation",
#         "996085341": "Prozo Warehouse Visit",
#         "996085342": "Commercial Closed / Agreement Initiated",
#         "996085343": "Agreement Signed",
#         "996085344": "Onboarding Completed",
#         "996089867": "Closed Lost"
#     },
#     "679793780": {
#         "996081821": "RFI Sent",
#         "996081822": "Tech Demo",
#         "996081823": "Solution Design",
#         "995921564": "Commercial Shared",
#         "995921565": "Commercial Negotiation",
#         "995921566": "Commercial Closed / Agreement Initiated",
#         "995921567": "Agreement Signed",
#         "995921568": "Onboarding Completed",
#         "996140196": "Closed Lost"
#     },
#     "679336879": {
#         "996133679": "RFI Sent",
#         "996133680": "Tech Demo",
#         "996133681": "Solution Design",
#         "995964759": "Commercial Shared",
#         "995964760": "Commercial Negotiation",
#         "995964761": "Commercial Closed / Agreement Initiated",
#         "995964762": "Agreement Signed",
#         "995964763": "Onboarding Completed",
#         "995964768": "Closed Lost"
#     },
#     "678993838": {
#         "996089773": "RFI Sent",
#         "996089774": "Solution Design",
#         "996089775": "Commercial Shared",
#         "996085345": "Commercial Negotiation",
#         "996085346": "Commercial Closed / Agreement Initiated",
#         "996085347": "Agreement Signed",
#         "996085348": "Onboarding Completed",
#         "995821842": "Closed Lost"
#     },
#     "681388447": {
#         "998351470": "RFI Sent",
#         "998351471": "Tech Demo",
#         "998351472": "Solution Design",
#         "998351473": "Commercial Shared",
#         "998351474": "Commercial Negotiation",
#         "998351475": "Commercial Closed / Agreement Initiated",
#         "998316459": "Agreement Signed",
#         "998316458": "Onboarding Completed",
#         "998351476": "Closed Lost"
#     },
#     "705978438": {
#         "1032050961": "RFI Sent",
#         "1032050962": "Commercial Shared",
#         "1032050963": "Commercial Negotiation",
#         "1032050964": "Commercial Closed / Agreement Initiated",
#         "1032050965": "Agreement Signed",
#         "1032050966": "Onboarding Completed",
#         "1032050967": "Closed Lost"
#     }
# }


# --- Email ---
def send_email(dfs_dict, total_deals, report_label):
    recipients = ["ankit.rakhecha@prozo.com","rishi.singh@prozo.com","farul.1@prozo.com"]

    for recipient in recipients:
        msg = EmailMessage()
        msg["Subject"] = f"Prozo | {report_label} Deal Property Change Report ({total_deals} deals monitored)"
        msg["From"] = formataddr(("Prozo Performance Manager", SMTP_USER.strip()))
        msg["To"] = recipient

        first_name = re.split(r"[._]", recipient.split("@")[0])[0].capitalize()

        body = f"""
<html>
  <body>
    <p>Hi {first_name},</p>
    <p>Here’s your {report_label.lower()} summary of deal property changes from HubSpot 👇</p>
    <ul>
      <li>Deal Type changes: {len(dfs_dict['deal_type'])}</li>
      <li>Deal Stage changes: {len(dfs_dict['dealstage'])}</li>
      <li>Expected Close Date changes: {len(dfs_dict['expected_close_date'])}</li>
    </ul>
    <p>📌 Details are attached in CSV files.</p>
    <p>Best regards,<br>Prozo</p>
  </body>
</html>
"""
        msg.add_alternative(body, subtype="html")

        for key, df in dfs_dict.items():
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            msg.add_attachment(
                csv_buffer.getvalue().encode("utf-8"),
                maintype="text",
                subtype="csv",
                filename=f"{key}_changes.csv"
            )

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)

        print(f"✅ Email sent to {recipient} ({report_label})")

# --- Report generator ---
def generate_and_send_report(hours_back, report_label):
    since_ts = int((datetime.datetime.now(datetime.UTC) - timedelta(hours=hours_back)).timestamp() * 1000)
    cutoff = datetime.datetime.now(datetime.UTC) - timedelta(hours=hours_back)
    deals, no_of_deals = fetch_all_deals(since_ts)

    rows_type, rows_stage, rows_close = [], [], []

    for deal in deals:
        deal_id = deal.get("id")
        print(deal_id)
        props = deal.get("properties", {})
        deal_name = props.get("dealname", "")
        owner_id = props.get("hubspot_owner_id", "")
        pipeline_id = props.get("pipeline", "")
        amount = props.get("amount", "")

        owner_name = OWNER_MAP.get(owner_id, owner_id)
        pipeline_name = PIPELINE_MAP.get(pipeline_id, pipeline_id)

        # Deal Type
        type_history = fetch_property_history(deal_id, "deal_type__hot__warm___cold_")
        if type_history:
            type_history.sort(key=lambda x: parse_hs_timestamp(x["timestamp"]))
            for i in range(1, len(type_history)):
                prev, curr = type_history[i-1], type_history[i]
                ts = parse_hs_timestamp(curr["timestamp"])
                if ts >= cutoff and prev["value"] != curr["value"]:
                    rows_type.append({
                        "Deal ID": deal_id,
                        "Deal Name": deal_name,
                        "Before Status": map_value(prev["value"]),
                        "After Status": map_value(curr["value"]),
                        "Timestamp": ts.strftime("%d-%m-%Y %H:%M"),
                        "Owner": owner_name,
                        "Pipeline": pipeline_name,
                        "Amount": amount
                    })

        # Deal Stage
        stage_history = fetch_property_history(deal_id, "dealstage")
        if stage_history:
            stage_history.sort(key=lambda x: parse_hs_timestamp(x["timestamp"]))
            for i in range(1, len(stage_history)):
                prev, curr = stage_history[i-1], stage_history[i]
                ts = parse_hs_timestamp(curr["timestamp"])
                if ts >= cutoff and prev["value"] != curr["value"]:
                    rows_stage.append({
                        "Deal ID": deal_id,
                        "Deal Name": deal_name,
                        "Before Stage": DEAL_STAGE_MAP.get(pipeline_id, {}).get(prev["value"], prev["value"]),
                        "After Stage": DEAL_STAGE_MAP.get(pipeline_id, {}).get(curr["value"], curr["value"]),
                        "Timestamp": ts.strftime("%d-%m-%Y %H:%M"),
                        "Owner": owner_name,
                        "Pipeline": pipeline_name,
                        "Amount": amount
                    })

        # Close Date
        close_history = fetch_property_history(deal_id, "expected_closure_date")
        if close_history:
            close_history.sort(key=lambda x: parse_hs_timestamp(x["timestamp"]))
            for i in range(1, len(close_history)):
                prev, curr = close_history[i-1], close_history[i]
                ts = parse_hs_timestamp(curr["timestamp"])
                if ts >= cutoff and prev["value"] != curr["value"]:
                    rows_close.append({
                        "Deal ID": deal_id,
                        "Deal Name": deal_name,
                        "Before Close Date": format_date(prev["value"]),
                        "After Close Date": format_date(curr["value"]),
                        "Timestamp": ts.strftime("%d-%m-%Y %H:%M"),
                        "Owner": owner_name,
                        "Pipeline": pipeline_name,
                        "Amount": amount
                    })
        time.sleep(0.3)

    dfs_dict = {
        "deal_type": pd.DataFrame(rows_type),
        "dealstage": pd.DataFrame(rows_stage),
        "expected_close_date": pd.DataFrame(rows_close)
    }

    send_email(dfs_dict, no_of_deals, report_label)
    return {"message": f"{report_label} report generated & sent", "total_deals": no_of_deals}

# --- Routes ---
@app.route('/')
def index():
    return "✅ Deal Property change Emailer is Live."

@app.route("/run-daily-report", methods=["GET"])
def run_daily_report():
    thread = threading.Thread(target=generate_and_send_report, args=(24, "Daily"))
    thread.start()
    return "🚀 Daily deal Property change job triggered."

@app.route("/run-weekly-report", methods=["GET"])
def run_weekly_report():
    thread = threading.Thread(target=generate_and_send_report, args=(24*7, "Weekly"))
    thread.start()
    return "🚀 Weekly deal Property change job triggered."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5005, debug=True)
