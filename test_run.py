#!/usr/bin/env python3
"""
Test script using the msfabricpysdkcore API to run notebooks.
This script uses the same successful approach as the direct API but through the SDK.
"""

from fabric_notebook_uploader import run_notebook_by_name, list_notebooks_in_workspace

# =============================================================================
# CONFIGURATION - Edit these variables
# =============================================================================

WORKSPACE_ID = "c7d1ecc1-d8fa-477c-baf1-47528abf9fe5"
NOTEBOOK_NAME = "data_agent_creation3"  # Using the clean notebook that works

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    try:
        print("🎯 Running notebook using msfabricpysdkcore API...")
        print(f"📓 Notebook: '{NOTEBOOK_NAME}'")
        print(f"🏢 Workspace: {WORKSPACE_ID}")
        print()
        
        # Run a notebook by name
        result = run_notebook_by_name(
            workspace_id=WORKSPACE_ID,
            notebook_name=NOTEBOOK_NAME
        )
        
        print("\n" + "="*60)
        print("📊 EXECUTION SUMMARY")
        print("="*60)
        print(f"Notebook: {NOTEBOOK_NAME}")
        print(f"Status: {result['status']}")
        print(f"Success: {result['success']}")
        print(f"Total Runtime: {result['total_runtime_str']}")
        print(f"Job Instance ID: {result['job_id']}")
        
        if result.get('start_time_utc'):
            print(f"Start Time: {result['start_time_utc']}")
        if result.get('end_time_utc'):
            print(f"End Time: {result['end_time_utc']}")
        
        print("="*60)
        
        print("\n📋 Available notebooks in workspace:")
        notebooks = list_notebooks_in_workspace(WORKSPACE_ID)
        for nb in notebooks:
            print(f"- {nb['display_name']} (ID: {nb['id']})")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        exit(1)