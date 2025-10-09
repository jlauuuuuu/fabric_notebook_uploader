#!/usr/bin/env python3
"""
Direct Fabric API script to run an existing notebook using REST API calls.
This script bypasses the msfabricpysdkcore wrapper and calls the Fabric API directly.
"""

import requests
import time
import json
import subprocess
import shutil
from pathlib import Path

# Optional import for Python-based auth fallback
try:
    from azure.identity import DeviceCodeCredential
except Exception:
    DeviceCodeCredential = None

def get_azure_cli_token(resource="https://api.fabric.microsoft.com/", tenant_id=None):
    """
    Get an access token. Prefer the Azure CLI if available; otherwise fall back
    to a Python device-code login using `azure-identity`.
    """
    # 1) If `az` is available on PATH, use it.
    az_path = shutil.which("az")
    if az_path:
        try:
            result = subprocess.run([
                az_path, "account", "get-access-token", "--resource", resource, "--output", "json"
            ], capture_output=True, text=True, check=True)
            token_info = json.loads(result.stdout)
            return token_info.get("accessToken")
        except Exception as e:
            raise RuntimeError(f"Failed to get Azure CLI token: {e}")

    # 2) Fallback: use azure-identity DeviceCodeCredential if available
    if DeviceCodeCredential is not None:
        try:
            # azure-identity expects a scope like '<resource>/.default'
            scope = resource.rstrip('/') + '/.default'
            cred_kwargs = {"tenant_id": tenant_id} if tenant_id else {}
            cred = DeviceCodeCredential(**cred_kwargs)
            token = cred.get_token(scope)
            return token.token
        except Exception as e:
            raise RuntimeError(f"Failed to get token via DeviceCodeCredential: {e}")

    # 3) Neither az nor azure-identity available: instruct the user
    raise RuntimeError(
        "Azure CLI not found on PATH and 'azure-identity' is not installed. "
        "Install the Azure CLI (recommended) or install the Python package with: 'pip install azure-identity'."
    )

def list_notebooks_in_workspace(workspace_id, access_token, debug=False):
    """
    List all notebooks in a workspace using direct API calls.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # Filter for notebooks only
    params = {
        "type": "Notebook"
    }
    
    if debug:
        print(f"[DEBUG] Listing notebooks in workspace {workspace_id}")
        print(f"[DEBUG] URL: {url}")
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        raise Exception(f"Failed to list notebooks: {response.status_code} {response.text}")
    
    data = response.json()
    notebooks = data.get("value", [])
    
    if debug:
        print(f"[DEBUG] Found {len(notebooks)} notebooks")
    
    return notebooks

def get_notebook_by_name(workspace_id, notebook_name, access_token, debug=False):
    """
    Find a notebook by its display name.
    """
    notebooks = list_notebooks_in_workspace(workspace_id, access_token, debug)
    
    for notebook in notebooks:
        if notebook.get("displayName") == notebook_name:
            return notebook
    
    return None

def run_notebook_direct_api(workspace_id, notebook_id, access_token, debug=False):
    """
    Run a notebook using direct Fabric API calls.
    
    Args:
        workspace_id: The Fabric workspace ID
        notebook_id: The notebook item ID  
        access_token: Azure access token
        debug: Enable debug output
        
    Returns:
        dict: Job execution results with status and runtime info
    """
    
    # Step 1: Start the notebook execution
    run_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    params = {
        "jobType": "RunNotebook"
    }
    
    # Add execution data to enable inline installation (%pip commands)
    execution_data = {
        "_inlineInstallationEnabled": True
    }
    
    if debug:
        print(f"[DEBUG] Starting notebook execution")
        print(f"[DEBUG] URL: {run_url}")
        print(f"[DEBUG] Params: {params}")
        print(f"[DEBUG] Execution data: {execution_data}")
    
    print(f"🚀 Starting notebook execution...")
    start_time = time.time()
    
    # Add execution data to enable inline installation (%pip commands)
    execution_data = {
        "_inlineInstallationEnabled": True
    }
    
    # Make the POST request to start the job with execution data
    response = requests.post(run_url, headers=headers, params=params, json={"executionData": execution_data})
    
    if response.status_code != 202:
        raise Exception(f"Failed to start notebook execution: {response.status_code} {response.text}")
    
    # Extract job instance ID from the Location header
    location_header = response.headers.get("Location")
    if not location_header:
        raise Exception("No Location header returned from job start request")
    
    # Extract job instance ID from URL like: 
    # https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances/{jobInstanceId}
    job_instance_id = location_header.split("/")[-1]
    
    print(f"📝 Job started with ID: {job_instance_id}")
    
    if debug:
        print(f"[DEBUG] Location header: {location_header}")
        print(f"[DEBUG] Job instance ID: {job_instance_id}")
    
    # Step 2: Poll for job completion
    status_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances/{job_instance_id}"
    
    status = "NotStarted"
    job_info = None
    
    while status in ["NotStarted", "InProgress"]:
        time.sleep(10)  # Wait 10 seconds before checking again
        
        if debug:
            print(f"[DEBUG] Polling job status: {status_url}")
        
        # Get job status
        status_response = requests.get(status_url, headers=headers)
        
        if status_response.status_code != 200:
            raise Exception(f"Failed to get job status: {status_response.status_code} {status_response.text}")
        
        job_info = status_response.json()
        status = job_info.get("status")
        
        # Calculate runtime
        elapsed_time = time.time() - start_time
        elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
        
        print(f"⏳ Status: {status} - Runtime: {elapsed_str}")
        
        if debug:
            print(f"[DEBUG] Job info: {json.dumps(job_info, indent=2)}")
    
    # Step 3: Final results
    total_runtime = time.time() - start_time
    total_runtime_str = time.strftime("%H:%M:%S", time.gmtime(total_runtime))
    
    result = {
        'notebook_id': notebook_id,
        'job_instance_id': job_instance_id,
        'status': status,
        'total_runtime': total_runtime,
        'total_runtime_str': total_runtime_str,
        'success': status == "Completed",
        'job_info': job_info,
        'start_time_utc': job_info.get('startTimeUtc') if job_info else None,
        'end_time_utc': job_info.get('endTimeUtc') if job_info else None,
        'failure_reason': job_info.get('failureReason') if job_info else None
    }
    
    # Print final status
    if status == "Completed":
        print(f"✅ Notebook execution completed successfully!")
        print(f"⏱️ Total runtime: {total_runtime_str}")
        if job_info.get('startTimeUtc') and job_info.get('endTimeUtc'):
            print(f"🕐 Started: {job_info['startTimeUtc']}")
            print(f"🕑 Ended: {job_info['endTimeUtc']}")
    elif status == "Failed":
        print(f"❌ Notebook execution failed!")
        print(f"⏱️ Runtime before failure: {total_runtime_str}")
        if job_info and job_info.get('failureReason'):
            print(f"💥 Failure reason: {json.dumps(job_info['failureReason'], indent=2)}")
    else:
        print(f"⚠️ Notebook execution ended with status: {status}")
        print(f"⏱️ Total runtime: {total_runtime_str}")
    
    return result

def run_notebook_by_name_direct_api(workspace_id, notebook_name, access_token, debug=False):
    """
    Run a notebook by name using direct API calls.
    """
    print(f"🔍 Looking for notebook '{notebook_name}' in workspace...")
    
    # Find the notebook by name
    notebook = get_notebook_by_name(workspace_id, notebook_name, access_token, debug)
    
    if not notebook:
        raise Exception(f"Notebook '{notebook_name}' not found in workspace {workspace_id}")
    
    notebook_id = notebook["id"]
    print(f"📓 Found notebook: {notebook['displayName']} (ID: {notebook_id})")
    
    # Run the notebook
    return run_notebook_direct_api(workspace_id, notebook_id, access_token, debug)

# =============================================================================
# CONFIGURATION - Edit these variables
# =============================================================================

WORKSPACE_ID = "c7d1ecc1-d8fa-477c-baf1-47528abf9fe5"  # Your Fabric workspace ID
NOTEBOOK_NAME = "data_agent_creation_clean_no_pip"                    # Name of the notebook to run
DEBUG = True                                              # Enable debug output

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    try:
        print("🔐 Getting Azure access token...")
        access_token = get_azure_cli_token()
        print("✅ Successfully obtained access token")
        
        print(f"🎯 Running notebook '{NOTEBOOK_NAME}' in workspace {WORKSPACE_ID}")
        
        # Run the notebook
        result = run_notebook_by_name_direct_api(
            workspace_id=WORKSPACE_ID,
            notebook_name=NOTEBOOK_NAME,
            access_token=access_token,
            debug=DEBUG
        )
        
        print("\n" + "="*60)
        print("📊 EXECUTION SUMMARY")
        print("="*60)
        print(f"Notebook: {NOTEBOOK_NAME}")
        print(f"Status: {result['status']}")
        print(f"Success: {result['success']}")
        print(f"Total Runtime: {result['total_runtime_str']}")
        print(f"Job Instance ID: {result['job_instance_id']}")
        
        if result.get('start_time_utc'):
            print(f"Start Time: {result['start_time_utc']}")
        if result.get('end_time_utc'):
            print(f"End Time: {result['end_time_utc']}")
            
        if not result['success'] and result.get('failure_reason'):
            print(f"Failure Reason: {json.dumps(result['failure_reason'], indent=2)}")
        
        print("="*60)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        exit(1)