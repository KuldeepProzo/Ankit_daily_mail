import os
import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import smtplib
from email.message import EmailMessage
from io import StringIO
import time
import re

# === Load ENV ===
load_dotenv()
HUBSPOT_KEY = os.getenv("HUBSPOT_KEY")   
SMTP_USER = os.getenv("SMTP_USER")       
SMTP_PASS = os.getenv("SMTP_PASS")       
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

HEADERS = {"Authorization": f"Bearer {HUBSPOT_KEY}"}

# --- Safe GET with retry ---
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


def fetch_all_deals(limit=100, test_count=30):  
    all_deals = []
    total_deals = 0

   
    last_24h = int((datetime.utcnow() - timedelta(hours=24)).timestamp() * 1000)

    url = "https://api.hubapi.com/crm/v3/objects/deals/search"
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "hs_lastmodifieddate",
                        "operator": "GTE",
                        "value": last_24h
                    }
                ]
            }
        ],
        "properties": [
            "dealname",
            "hubspot_owner_id",
            "pipeline",
            "amount"
        ],
        "limit": limit
    }

    while True:
        response = requests.post(url, headers=HEADERS, json=payload)
        if not response:
            break
        
        data = response.json()
        all_deals.extend(data.get("results", []))
        total_deals += len(data.get("results", []))

        #if len(all_deals) >= test_count:
         #    return all_deals[:test_count], len(all_deals)

        after = data.get("paging", {}).get("next", {}).get("after")
        if after:
            payload["after"] = after
        else:
            break
        time.sleep(0.3)

    return all_deals, len(all_deals)

# --- Fetch deal_type history ---
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

# --- Parse HubSpot timestamps ---
def parse_hs_timestamp(ts_str):
    try:
        if ts_str.endswith("Z"):
            ts_str = ts_str.replace("Z", "+00:00")
        return datetime.fromisoformat(ts_str)
    except Exception:
        return datetime.fromtimestamp(int(ts_str) / 1000)

def format_date(value):
    """Convert HS timestamp string to DD-MM-YYYY, else return as is"""
    if not value:
        return ""
    try:
        return datetime.fromtimestamp(int(value)/1000).strftime("%d-%m-%Y")
    except:
        return value


# --- Map values ---
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

OWNER_MAP = {
    "Divij Wadhwa": "81151298",
    "divij wadhwa": "81151298",
    "Shagun Tyagi": "26693784",
    "Tushar Mittal": "45964886",
    "Yavanika Sharma": "51443636",
    "Ankit Rakhecha": "75406284",
    "Gourav Rathi": "76286666",
    "Vivek Mishra": "76420249",
    "Praneet Vajpayee": "76420252",
    "Manisha Mehani": "76420254",
    "Satish Valla": "76420257",
    "Dr. Ashvini Jakhar": "76420259",
    "Siddhartha Agarwal": "76420261",
    "Narinder Kakkar": "76420267",
    "Richa Malhan": "76420274",
    "Dheeraj Karki": "76420298",
    "Pulkit Garg": "76427044",
    "Sai Anusha": "76747606",
    "Riya Mandhan": "77791508",
    "Kuldeep Thakran": "78633331",
    "Kshitij Magre": "78633390",
    "Saurabh Jain": "78633391",
    "Piyush Kukreja": "79601223",
    "Lokesh Marwah": "79807296",
    "Nazrul Islam": "80078439",
    "Durgesh Kumawat": "80102798",
    "Shaikh Faraz Quamar": "80521827",
    "Vishal Labh": "80646448",
    "Nikhil Patle": "80940969",
    "Noyal Saharan": "80978396",
    "Sushma Chauhan": "81098157",
    "Siddharth Sharma": "81481361",
    "Prabhmeet Kaur": "81481362",
    "Nikhil Sharma": "81481363",
    "Kumar Anshuman": "81481364",
    "Rishikesh Tiwari": "81481365",
    "Ankur Malviya": "81481366",
    "Tarun Ramesh": "81481367",
    "Rudra Tamrakar": "81481368",
    "Rahul Pant": "81481369",
    "Rohan Baisoya": "81481370",
    "Bhupinder Singh": "81513348",
    "Falguni Ghosh": "82052050"
}


# --- Send Email ---
def send_email(dfs_dict, total_deals):
    recipient_emails = ["divijwadhwa44@gmail.com", "kuldeep.thakran@prozo.com"]

    for recipient_email in recipient_emails:
        # create a fresh message object for each recipient
        msg = EmailMessage()
        msg["Subject"] = f"Prozo | Daily Deal Property Change Report ({total_deals} deals monitored)"
        msg["From"] = SMTP_USER
        msg["To"] = recipient_email

        # first name from email
        first_name = re.split(r"[._]", recipient_email.split("@")[0])[0].capitalize()

        body = f"""
<html>
  <body>
    <p>Hi {first_name},</p>
    <p>Here’s your latest summary of deal property changes from HubSpot 👇</p>
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

        # Attach CSVs
        for key, df in dfs_dict.items():
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            msg.add_attachment(
                csv_buffer.getvalue().encode("utf-8"),
                maintype="text",
                subtype="csv",
                filename=f"{key}_changes.csv"
            )

        # Send
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)

        print(f"✅ Email sent to {recipient_email}")



def safe_send_email(df, count ,no_of_deals):
    while True:
        try:
            send_email(df, count ,no_of_deals)
            
            break  # exit loop on success
        except Exception as e:
            print(f"❌ Failed to send email : {e}")
            print("🔁 Retrying in 5 seconds...")
            time.sleep(5)
            try:
                 send_email(df, count ,no_of_deals)
                 print(f"✅ Email sent on retry")
            except Exception as e2:
                 print(f"❌ Retry also failed : {e2}")
             


# --- Main ---
rows_type = []
rows_stage = []
rows_close = []

deals, no_of_deals = fetch_all_deals()  
print(f"Total deals fetched: {len(deals)} | HubSpot count: {no_of_deals}")

cutoff = datetime.now(timezone.utc) - timedelta(days=1)

for deal in deals:
    deal_id = deal.get("id")
    props = deal.get("properties", {})

    deal_name = props.get("dealname", "")
    owner_id = props.get("hubspot_owner_id", "")
    pipeline_id = props.get("pipeline", "")
    amount = props.get("amount", "")

    owner_name = OWNER_MAP.get(owner_id, owner_id)
    pipeline_name = PIPELINE_MAP.get(pipeline_id, pipeline_id)

    print(f"\n🔎 Processing Deal: {deal_id} | {deal_name} | Owner: {owner_name} | Pipeline: {pipeline_name}")

    # 1) Deal Type history
    print("   📜 Fetching property history: deal_type__hot__warm___cold_")
    type_history = fetch_property_history(deal_id, "deal_type__hot__warm___cold_")
    print(f"   ➡️ History fetched: {type_history}")

    if type_history:
        type_history.sort(key=lambda x: parse_hs_timestamp(x["timestamp"]))
        for i in range(1, len(type_history)):
            prev, curr = type_history[i-1], type_history[i]
            ts = parse_hs_timestamp(curr["timestamp"])
            if ts >= cutoff and prev["value"] != curr["value"]:
                row = {
                    "Deal ID": deal_id,
                    "Deal Name": deal_name,
                    "Before Status": map_value(prev["value"]),
                    "After Status": map_value(curr["value"]),
                    "Timestamp": ts.strftime("%d-%m-%Y %H:%M"),
                    "Owner": owner_name,
                    "Pipeline": pipeline_name,
                    "Amount": amount
                }
                print(f"   ✅ Added row (Deal Type change): {row}")
                rows_type.append(row)

    # 2) Deal Stage history
    print("   📜 Fetching property history: dealstage")
    stage_history = fetch_property_history(deal_id, "dealstage")
    print(f"   ➡️ History fetched: {stage_history}")

    if stage_history:
        stage_history.sort(key=lambda x: parse_hs_timestamp(x["timestamp"]))
        for i in range(1, len(stage_history)):
            prev, curr = stage_history[i-1], stage_history[i]
            ts = parse_hs_timestamp(curr["timestamp"])
            if ts >= cutoff and prev["value"] != curr["value"]:
                row = {
                    "Deal ID": deal_id,
                    "Deal Name": deal_name,
                    "Before Stage": prev["value"],
                    "After Stage": curr["value"],
                    "Timestamp": ts.strftime("%d-%m-%Y %H:%M"),
                    "Owner": owner_name,
                    "Pipeline": pipeline_name,
                    "Amount": amount
                }
                print(f"   ✅ Added row (Deal Stage change): {row}")
                rows_stage.append(row)

    # 3) Expected Close Date history
    print("   📜 Fetching property history: expected_closure_date")
    close_history = fetch_property_history(deal_id, "expected_closure_date")
    print(f"   ➡️ History fetched: {close_history}")

    if close_history:
        close_history.sort(key=lambda x: parse_hs_timestamp(x["timestamp"]))
        for i in range(1, len(close_history)):
            prev, curr = close_history[i-1], close_history[i]
            ts = parse_hs_timestamp(curr["timestamp"])
            if ts >= cutoff and prev["value"] != curr["value"]:
                row = {
                    "Deal ID": deal_id,
                    "Deal Name": deal_name,
                    "Before Close Date": format_date(prev["value"]),
                    "After Close Date": format_date(curr["value"]),
                    "Timestamp": ts.strftime("%d-%m-%Y %H:%M"),
                    "Owner": owner_name,
                    "Pipeline": pipeline_name,
                    "Amount": amount
                }
                print(f"   ✅ Added row (Close Date change): {row}")
                rows_close.append(row)

    time.sleep(0.3)



# Convert to DataFrames
df_type = pd.DataFrame(rows_type)
df_stage = pd.DataFrame(rows_stage)
df_close = pd.DataFrame(rows_close)



dfs_dict = {
    "deal_type": df_type,
    "dealstage": df_stage,
    "expected_close_date": df_close
}

send_email(dfs_dict, no_of_deals)


