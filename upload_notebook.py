import base64
import json
import requests
import time
from auth_cli import access_token  # Your credential function or token provider

WORKSPACE_ID = "c7d1ecc1-d8fa-477c-baf1-47528abf9fe5"
NOTEBOOK_PATH = "notebooks/test_notebook.ipynb"
NOTEBOOK_NAME = "test_notebook"
API_BASE = "https://api.fabric.microsoft.com/v1"

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# 1. Check if notebook with the same display name already exists
list_url = f"{API_BASE}/workspaces/{WORKSPACE_ID}/items?filter=type eq 'Notebook'"
list_resp = requests.get(list_url, headers=headers)
list_resp.raise_for_status()
notebooks = list_resp.json().get("value", [])

notebook_id = None
for nb in notebooks:
    if nb.get("displayName") == NOTEBOOK_NAME:
        notebook_id = nb["id"]
        print(f"Notebook with name '{NOTEBOOK_NAME}' exists. Will update notebook with ID: {notebook_id}")
        break

# 2. Load notebook file and base64 encode content
with open(NOTEBOOK_PATH, "rb") as f:
    raw = f.read()
b64_notebook = base64.b64encode(raw).decode("utf-8")

# Prepare parts list initially with current notebook content
parts = [{
    "path": "notebook-content.ipynb",
    "payload": b64_notebook,
    "payloadType": "InlineBase64"
}]



# 3. Updating existing notebook
if notebook_id is not None:
    defn_url = f"{API_BASE}/workspaces/{WORKSPACE_ID}/notebooks/{notebook_id}/definition"
    defn_resp = requests.get(defn_url, headers=headers)
    defn_resp.raise_for_status()
    definition = defn_resp.json().get("definition", {})
    existing_parts = definition.get("parts", [])

    # Find .platform part and include it for updateMetadata
    platform_part = next((p for p in existing_parts if p.get("path") == ".platform"), None)
    if platform_part:
        parts.append(platform_part)

definition_payload = {
    "definition": {
        "format": "ipynb",
        "parts": parts
    }
}

# Creating new notebook.
if notebook_id is None:
    # 4a. Create new notebook
    payload = {
        "displayName": NOTEBOOK_NAME,
        "definition": definition_payload["definition"]
    }
    url = f"{API_BASE}/workspaces/{WORKSPACE_ID}/notebooks"
else:
    # 4b. Update existing notebook, including metadata update
    url = f"{API_BASE}/workspaces/{WORKSPACE_ID}/notebooks/{notebook_id}/updateDefinition?updateMetadata=true"
    payload = definition_payload

resp = requests.post(url, headers=headers, json=payload)

try:
    resp.raise_for_status()
except requests.exceptions.HTTPError as e:
    if resp.status_code == 400:
        print("Bad Request: possibly due to missing .platform file or payload issue.")
        print("Response:", resp.text)
        exit(1)
    else:
        raise e

# 5. Handle long running operation if present
if resp.status_code == 202:
    op_url = resp.headers["Location"]
    while True:
        poll_resp = requests.get(op_url, headers=headers)
        op = poll_resp.json()
        if op.get("status") in ("Succeeded", "Failed"):
            break
        retry_after = int(poll_resp.headers.get("Retry-After", 5))
        time.sleep(retry_after)
    if op.get("status") != "Succeeded":
        raise Exception("Notebook operation failed")

    # If created, get notebook id from listing
    if notebook_id is None:
        list_resp = requests.get(list_url, headers=headers)
        list_resp.raise_for_status()
        notebooks = list_resp.json().get("value", [])
        for nb in notebooks:
            if nb.get("displayName") == NOTEBOOK_NAME:
                notebook_id = nb["id"]
                break
        if notebook_id is None:
            raise Exception("Notebook created but ID not found in listing")
else:
    # For create, id in response, for update use existing
    if notebook_id is None:
        notebook_id = resp.json().get("id")

print(f"Notebook operation succeeded. Notebook ID: {notebook_id}")
