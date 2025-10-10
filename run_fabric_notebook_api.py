#!/usr/bin/env python3
"""
Direct REST API implementation for running Fabric notebooks.

This script provides faster execution and more detailed logging compared to the SDK version
by using direct REST API calls to the Microsoft Fabric API.
"""

import json
import time
import requests
import subprocess
import sys
from typing import Optional, Dict, Any


def get_azure_cli_token() -> str:
    """Get Azure CLI access token for Fabric API authentication."""
    try:
        # Try different ways to call az command
        az_commands = [
            ["az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com"],
            ["az.cmd", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com"],
            ["C:\\Program Files\\Microsoft SDKs\\Azure\\CLI2\\wbin\\az.cmd", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com"]
        ]
        
        last_error = None
        for cmd in az_commands:
            try:
                print(f"🔍 Trying command: {' '.join(cmd)}")
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=True,
                    shell=True  # Use shell to help find the command
                )
                token_info = json.loads(result.stdout)
                print("✅ Successfully obtained Azure CLI token")
                return token_info["accessToken"]
            except (subprocess.CalledProcessError, FileNotFoundError) as e:
                last_error = e
                print(f"⚠️ Command failed: {e}")
                continue
        
        # If all commands failed
        print(f"❌ Failed to get Azure CLI token with all methods")
        print(f"Last error: {last_error}")
        print("Make sure you're logged in with 'az login'")
        print("Try running 'az account get-access-token --resource https://api.fabric.microsoft.com' manually")
        sys.exit(1)
        
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse token response: {e}")
        print(f"Raw output: {result.stdout}")
        sys.exit(1)


def make_fabric_api_request(method: str, url: str, headers: Dict[str, str], data: Optional[Dict] = None) -> requests.Response:
    """Make a request to the Fabric API with detailed logging."""
    print(f"🔍 API Request: {method} {url}")
    if data:
        print(f"📤 Request Body: {json.dumps(data, indent=2)}")
    
    try:
        response = requests.request(method, url, headers=headers, json=data, timeout=30)
        print(f"📥 Response Status: {response.status_code}")
        print(f"📥 Response Headers: {dict(response.headers)}")
        
        if response.text.strip():
            try:
                response_json = response.json()
                print(f"📥 Response Body: {json.dumps(response_json, indent=2)}")
            except json.JSONDecodeError:
                print(f"📥 Response Body (text): {response.text}")
        else:
            print(f"📥 Response Body: (empty)")
        
        return response
    except requests.exceptions.RequestException as e:
        print(f"❌ API Request failed: {e}")
        raise


def get_workspace_info(workspace_id: str, token: str) -> Dict[str, Any]:
    """Get workspace information."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
    response = make_fabric_api_request("GET", url, headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Failed to get workspace info: {response.status_code}")
        response.raise_for_status()


def list_workspace_items(workspace_id: str, token: str) -> list:
    """List all items in a workspace."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    response = make_fabric_api_request("GET", url, headers)
    
    if response.status_code == 200:
        return response.json().get("value", [])
    else:
        print(f"❌ Failed to list workspace items: {response.status_code}")
        response.raise_for_status()


def find_notebook_by_name(workspace_id: str, notebook_name: str, token: str) -> Optional[str]:
    """Find a notebook ID by its display name."""
    print(f"🔍 Searching for notebook: {notebook_name}")
    
    items = list_workspace_items(workspace_id, token)
    
    for item in items:
        if item.get("type") == "Notebook" and item.get("displayName") == notebook_name:
            notebook_id = item.get("id")
            print(f"✅ Found notebook: {notebook_name} (ID: {notebook_id})")
            return notebook_id
    
    print(f"❌ Notebook not found: {notebook_name}")
    return None


def start_notebook_execution(workspace_id: str, notebook_id: str, token: str) -> Dict[str, Any]:
    """Start notebook execution via REST API."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Execution data with inline installation enabled
    execution_data = {
        "_inlineInstallationEnabled": True
    }
    
    # Correct API endpoint with jobType as query parameter
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"
    data = {
        "executionData": execution_data
    }
    
    print(f"🚀 Starting notebook execution...")
    response = make_fabric_api_request("POST", url, headers, data)
    
    if response.status_code == 202:
        # For 202 responses, the job info is in the headers, not the body
        location_header = response.headers.get('Location', '')
        job_id = response.headers.get('x-ms-job-id', '')
        
        # Extract job ID from location header if not in x-ms-job-id
        if not job_id and location_header:
            job_id = location_header.split('/jobs/instances/')[-1]
        
        job_info = {
            'id': job_id,
            'status': 'NotStarted',  # Initial status for 202 accepted
            'location': location_header
        }
        
        print(f"✅ Job started successfully!")
        print(f"📋 Job ID: {job_info.get('id')}")
        print(f"📋 Initial Status: {job_info.get('status')}")
        print(f"📍 Location: {location_header}")
        return job_info
    else:
        print(f"❌ Failed to start notebook execution: {response.status_code}")
        response.raise_for_status()


def get_job_status(workspace_id: str, notebook_id: str, job_id: str, token: str) -> Dict[str, Any]:
    """Get the current status of a job."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances/{job_id}"
    response = make_fabric_api_request("GET", url, headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Failed to get job status: {response.status_code}")
        response.raise_for_status()


def monitor_job_execution(workspace_id: str, notebook_id: str, job_id: str, token: str) -> Dict[str, Any]:
    """Monitor job execution until completion with detailed logging."""
    start_time = time.time()
    check_interval = 10  # seconds
    
    print(f"⏳ Monitoring job execution (checking every {check_interval}s)...")
    
    while True:
        job_info = get_job_status(workspace_id, notebook_id, job_id, token)
        status = job_info.get("status")
        
        # Calculate runtime
        runtime = time.time() - start_time
        runtime_str = time.strftime("%H:%M:%S", time.gmtime(runtime))
        
        print(f"📊 Status: {status} - Runtime: {runtime_str}")
        
        # Check if job is complete
        if status not in ["InProgress", "NotStarted"]:
            break
        
        time.sleep(check_interval)
    
    # Final status
    total_runtime = time.time() - start_time
    total_runtime_str = time.strftime("%H:%M:%S", time.gmtime(total_runtime))
    
    result = {
        'notebook_id': notebook_id,
        'job_id': job_id,
        'status': status,
        'total_runtime': total_runtime,
        'total_runtime_str': total_runtime_str,
        'success': status == "Completed",
        'job_info': job_info
    }
    
    if status == "Completed":
        print(f"✅ Notebook execution completed successfully!")
        print(f"⏱️ Total runtime: {total_runtime_str}")
    elif status == "Failed":
        print(f"❌ Notebook execution failed!")
        print(f"⏱️ Runtime before failure: {total_runtime_str}")
        if 'error' in job_info:
            print(f"💥 Error details: {json.dumps(job_info['error'], indent=2)}")
    else:
        print(f"⚠️ Notebook execution ended with status: {status}")
        print(f"⏱️ Total runtime: {total_runtime_str}")
    
    return result


def run_notebook_by_name(workspace_id: str, notebook_name: str) -> Dict[str, Any]:
    """
    Run a Fabric notebook by its display name using direct REST API.
    
    Args:
        workspace_id: The Fabric workspace ID
        notebook_name: The display name of the notebook to run
        
    Returns:
        dict: Job execution results with status and runtime info
    """
    print(f"🎯 Running notebook: {notebook_name}")
    print(f"🏢 Workspace ID: {workspace_id}")
    
    # Get authentication token
    token = get_azure_cli_token()
    
    # Get workspace info
    workspace_info = get_workspace_info(workspace_id, token)
    print(f"🏢 Workspace: {workspace_info.get('displayName', 'Unknown')}")
    
    # Find notebook by name
    notebook_id = find_notebook_by_name(workspace_id, notebook_name, token)
    if not notebook_id:
        return {
            'notebook_name': notebook_name,
            'error': f'Notebook "{notebook_name}" not found in workspace',
            'success': False
        }
    
    # Start execution
    job_info = start_notebook_execution(workspace_id, notebook_id, token)
    job_id = job_info.get('id')
    
    # Monitor execution
    result = monitor_job_execution(workspace_id, notebook_id, job_id, token)
    result['notebook_name'] = notebook_name
    
    return result


def run_notebook_by_id(workspace_id: str, notebook_id: str) -> Dict[str, Any]:
    """
    Run a Fabric notebook by its ID using direct REST API.
    
    Args:
        workspace_id: The Fabric workspace ID
        notebook_id: The ID of the notebook to run
        
    Returns:
        dict: Job execution results with status and runtime info
    """
    print(f"🎯 Running notebook ID: {notebook_id}")
    print(f"🏢 Workspace ID: {workspace_id}")
    
    # Get authentication token
    token = get_azure_cli_token()
    
    # Get workspace info
    workspace_info = get_workspace_info(workspace_id, token)
    print(f"🏢 Workspace: {workspace_info.get('displayName', 'Unknown')}")
    
    # Start execution
    job_info = start_notebook_execution(workspace_id, notebook_id, token)
    job_id = job_info.get('id')
    
    # Monitor execution
    result = monitor_job_execution(workspace_id, notebook_id, job_id, token)
    
    return result


def list_notebooks_in_workspace(workspace_id: str) -> list:
    """
    List all notebooks in a workspace using direct REST API.
    
    Args:
        workspace_id: The Fabric workspace ID
        
    Returns:
        list: List of notebook items with id and displayName
    """
    print(f"📋 Listing notebooks in workspace: {workspace_id}")
    
    # Get authentication token
    token = get_azure_cli_token()
    
    # Get workspace info
    workspace_info = get_workspace_info(workspace_id, token)
    print(f"🏢 Workspace: {workspace_info.get('displayName', 'Unknown')}")
    
    # List all items
    items = list_workspace_items(workspace_id, token)
    
    # Filter notebooks
    notebooks = []
    for item in items:
        if item.get("type") == "Notebook":
            notebooks.append({
                'id': item.get('id'),
                'displayName': item.get('displayName'),
                'description': item.get('description', '')
            })
    
    print(f"📊 Found {len(notebooks)} notebooks")
    for nb in notebooks:
        print(f"  📓 {nb['displayName']} (ID: {nb['id']})")
    
    return notebooks


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run Fabric notebooks using direct REST API")
    parser.add_argument("workspace_id", help="Fabric workspace ID")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--name", help="Notebook name to run")
    group.add_argument("--id", help="Notebook ID to run")
    group.add_argument("--list", action="store_true", help="List all notebooks in workspace")
    
    args = parser.parse_args()
    
    try:
        if args.list:
            notebooks = list_notebooks_in_workspace(args.workspace_id)
            print(f"\n📊 Summary: Found {len(notebooks)} notebooks")
        elif args.name:
            result = run_notebook_by_name(args.workspace_id, args.name)
            if result['success']:
                print(f"\n✅ Execution completed successfully in {result['total_runtime_str']}")
                sys.exit(0)
            else:
                print(f"\n❌ Execution failed")
                sys.exit(1)
        elif args.id:
            result = run_notebook_by_id(args.workspace_id, args.id)
            if result['success']:
                print(f"\n✅ Execution completed successfully in {result['total_runtime_str']}")
                sys.exit(0)
            else:
                print(f"\n❌ Execution failed")
                sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️ Execution interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)