import requests
import os
from dotenv import load_dotenv
import json

load_dotenv()
HUBSPOT_KEY = os.getenv("HUBSPOT_TOKEN")
HEADERS = {"Authorization": f"Bearer {HUBSPOT_KEY}"}

# Fetch all deal pipelines
url = "https://api.hubapi.com/crm/v3/pipelines/deals"
response = requests.get(url, headers=HEADERS)
pipelines = response.json().get("results", [])

# Initialize the mapping dictionary
DEAL_STAGE_MAP = {}

# Iterate through each pipeline and its stages
for pipeline in pipelines:
    pipeline_id = pipeline["id"]
    DEAL_STAGE_MAP[pipeline_id] = {
        stage["id"]: stage["label"]
        for stage in pipeline["stages"]
    }

# Print the mapping for verification
print(DEAL_STAGE_MAP)
with open("deal_stage_map.json", "w") as f:
    json.dump(DEAL_STAGE_MAP, f, indent=4)