import requests
import time
from auth_cli import access_token  # Your auth token provider

WORKSPACE_ID = "c7d1ecc1-d8fa-477c-baf1-47528abf9fe5"
NOTEBOOK_ID = "21a2f830-05af-4711-b551-333eaa256e3b"
API_BASE = "https://api.fabric.microsoft.com/v1"

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# Correct run notebook job endpoint
run_url = f"{API_BASE}/workspaces/{WORKSPACE_ID}/items/{NOTEBOOK_ID}/jobs/instances?jobType=RunNotebook"

# You can pass parameters and configuration inside executionData if needed, else empty
body = {
  "executionData": {
    "parameters": {},
    "configuration": {
      "useStarterPool": True
    }
  }
}


resp = requests.post(run_url, headers=headers, json=body)
resp.raise_for_status()

if resp.status_code == 202:
    op_url = resp.headers["Location"]
    print(f"Execution started. Polling operation at: {op_url}")

    while True:
        poll_resp = requests.get(op_url, headers=headers)
        poll_resp.raise_for_status()
        op_data = poll_resp.json()

        status = op_data.get("status")
        print(f"Status: {status}")

        if status in ("Succeeded", "Failed"):
            break

        retry_after = int(poll_resp.headers.get("Retry-After", 5))
        time.sleep(retry_after)

    if status != "Succeeded":
        raise Exception(f"Notebook execution failed: {op_data}")
    else:
        print("Notebook executed successfully.")
else:
    print("Notebook execution completed synchronously.")
