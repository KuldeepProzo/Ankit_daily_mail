import requests
import os
import json
from dotenv import load_dotenv

load_dotenv()
HUBSPOT_KEY = os.getenv("HUBSPOT_TOKEN")
HEADERS = {"Authorization": f"Bearer {HUBSPOT_KEY}"}

url = "https://api.hubapi.com/crm/v3/owners"
response = requests.get(url, headers=HEADERS)
data = response.json()

if "status" in data and data["status"] == "error":
    raise Exception(f"HubSpot API error: {data['message']}")
print(data)
# Build mapping: owner ID -> owner name
OWNER_MAP = {str(owner["id"]): f"{owner.get('firstName','')} {owner.get('lastName','')}".strip() for owner in data.get("results", [])}

# Save to JSON
with open("owner_map.json", "w") as f:
    json.dump(OWNER_MAP, f, indent=4)

print("✅ OWNER_MAP saved as owner_map.json")