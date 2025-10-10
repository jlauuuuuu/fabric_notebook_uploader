#!/usr/bin/env python3
"""
Get detailed information about Fabric Data Agents via REST API.

This script provides comprehensive information about data agents including:
- Basic item properties (name, ID, type, description)
- Extended metadata and properties
- Publish/deployment status
- Configuration details
- Associated resources
"""

import json
import time
import requests
import subprocess
import sys
from typing import Optional, Dict, Any, List


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
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=True,
                    shell=True
                )
                token_info = json.loads(result.stdout)
                return token_info["accessToken"]
            except (subprocess.CalledProcessError, FileNotFoundError) as e:
                last_error = e
                continue
        
        print(f"❌ Failed to get Azure CLI token with all methods")
        print(f"Last error: {last_error}")
        sys.exit(1)
        
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse token response: {e}")
        sys.exit(1)


def make_fabric_api_request(method: str, url: str, headers: Dict[str, str], data: Optional[Dict] = None) -> requests.Response:
    """Make a request to the Fabric API with detailed logging."""
    print(f"🔍 API Request: {method} {url}")
    if data:
        print(f"📤 Request Body: {json.dumps(data, indent=2)}")
    
    try:
        response = requests.request(method, url, headers=headers, json=data, timeout=30)
        print(f"📥 Response Status: {response.status_code}")
        
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


def get_workspace_items(workspace_id: str, token: str, item_type: str = None) -> List[Dict]:
    """Get all items in a workspace, optionally filtered by type."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    if item_type:
        url += f"?type={item_type}"
    
    response = make_fabric_api_request("GET", url, headers)
    
    if response.status_code == 200:
        return response.json().get("value", [])
    else:
        print(f"❌ Failed to get workspace items: {response.status_code}")
        response.raise_for_status()


def get_item_details(workspace_id: str, item_id: str, token: str) -> Dict[str, Any]:
    """Get detailed information about a specific item."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}"
    response = make_fabric_api_request("GET", url, headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Failed to get item details: {response.status_code}")
        response.raise_for_status()


def get_item_definition(workspace_id: str, item_id: str, token: str) -> Dict[str, Any]:
    """Get the definition/configuration of an item."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/GetDefinition"
    response = make_fabric_api_request("POST", url, headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"⚠️ Could not get item definition: {response.status_code}")
        return {}


def check_data_agent_specific_endpoints(workspace_id: str, item_id: str, token: str) -> Dict[str, Any]:
    """Try to get data agent specific information from various endpoints."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # List of potential data agent specific endpoints to try
    endpoints_to_try = [
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{item_id}",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{item_id}/status",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{item_id}/configuration",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/dataagent",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/properties",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/metadata"
    ]
    
    results = {}
    
    for endpoint in endpoints_to_try:
        try:
            print(f"\n🧪 Trying endpoint: {endpoint}")
            response = requests.get(endpoint, headers=headers, timeout=30)
            
            if response.status_code in [200, 202]:
                try:
                    data = response.json()
                    results[endpoint] = {
                        "status_code": response.status_code,
                        "data": data
                    }
                    print(f"✅ Success! Data found: {json.dumps(data, indent=2)}")
                except json.JSONDecodeError:
                    results[endpoint] = {
                        "status_code": response.status_code,
                        "data": response.text
                    }
                    print(f"✅ Success! Text response: {response.text}")
            else:
                print(f"❌ Failed with status: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Error: {e}")
            continue
    
    return results


def analyze_data_agents(workspace_id: str) -> None:
    """Analyze all data agents in a workspace and get detailed information."""
    print(f"🔍 Analyzing Data Agents in Workspace: {workspace_id}")
    
    # Get authentication token
    token = get_azure_cli_token()
    
    # Get all items in workspace
    print(f"\n📋 Getting all items in workspace...")
    all_items = get_workspace_items(workspace_id, token)
    
    # Filter for data agents and notebooks (data agents might be created as notebooks)
    data_agents = []
    notebooks = []
    
    for item in all_items:
        item_type = item.get('type', '')
        display_name = item.get('displayName', '')
        
        if 'agent' in display_name.lower() or 'data_agent' in display_name.lower():
            if item_type == 'Notebook':
                notebooks.append(item)
            else:
                data_agents.append(item)
        elif item_type in ['DataAgent', 'MLModel', 'AISkill']:  # Potential data agent types
            data_agents.append(item)
    
    print(f"\n📊 Found {len(data_agents)} potential data agents")
    print(f"📊 Found {len(notebooks)} agent-related notebooks")
    
    # Analyze each potential data agent
    all_agents = data_agents + notebooks
    
    for i, item in enumerate(all_agents, 1):
        print(f"\n{'='*80}")
        print(f"🤖 DATA AGENT {i}/{len(all_agents)}: {item.get('displayName', 'Unknown')}")
        print(f"{'='*80}")
        
        # Basic item information
        print(f"📋 Basic Information:")
        print(f"  🆔 ID: {item.get('id')}")
        print(f"  📝 Name: {item.get('displayName')}")
        print(f"  📄 Description: {item.get('description', 'No description')}")
        print(f"  🏷️ Type: {item.get('type')}")
        print(f"  📁 Folder ID: {item.get('folderId', 'Root')}")
        
        # Get detailed item information
        print(f"\n🔍 Getting detailed item information...")
        try:
            detailed_info = get_item_details(workspace_id, item['id'], token)
            
            # Check for any additional properties
            extra_props = {}
            for key, value in detailed_info.items():
                if key not in ['id', 'displayName', 'description', 'type', 'workspaceId', 'folderId']:
                    extra_props[key] = value
            
            if extra_props:
                print(f"  ✨ Additional Properties: {json.dumps(extra_props, indent=4)}")
            else:
                print(f"  ℹ️ No additional properties found in basic item API")
                
        except Exception as e:
            print(f"  ❌ Error getting detailed info: {e}")
        
        # Try to get item definition
        print(f"\n📖 Getting item definition...")
        try:
            definition = get_item_definition(workspace_id, item['id'], token)
            if definition:
                print(f"  📄 Definition found: {json.dumps(definition, indent=4)}")
            else:
                print(f"  ℹ️ No definition available")
        except Exception as e:
            print(f"  ❌ Error getting definition: {e}")
        
        # Try data agent specific endpoints
        print(f"\n🧪 Checking data agent specific endpoints...")
        try:
            agent_specific_data = check_data_agent_specific_endpoints(workspace_id, item['id'], token)
            
            if agent_specific_data:
                print(f"  🎯 Data agent specific data found!")
                for endpoint, result in agent_specific_data.items():
                    print(f"    📍 {endpoint}: Status {result['status_code']}")
                    if isinstance(result['data'], dict):
                        # Look for publish/status related keys
                        publish_keys = [k for k in result['data'].keys() if 'publish' in k.lower() or 'status' in k.lower() or 'deploy' in k.lower()]
                        if publish_keys:
                            print(f"      🚀 Publish/Status Keys: {publish_keys}")
                            for key in publish_keys:
                                print(f"        {key}: {result['data'][key]}")
            else:
                print(f"  ℹ️ No data agent specific endpoints responded")
                
        except Exception as e:
            print(f"  ❌ Error checking data agent endpoints: {e}")
        
        print(f"\n⏱️ Waiting 1 second before next item...")
        time.sleep(1)
    
    print(f"\n🎉 Analysis complete! Checked {len(all_agents)} potential data agents.")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Get detailed information about Fabric Data Agents")
    parser.add_argument("workspace_id", help="Fabric workspace ID")
    parser.add_argument("--item-id", help="Specific item ID to analyze (optional)")
    
    args = parser.parse_args()
    
    try:
        if args.item_id:
            # Analyze specific item
            print(f"🎯 Analyzing specific item: {args.item_id}")
            token = get_azure_cli_token()
            
            print(f"\n📋 Getting basic item info...")
            item_info = get_item_details(args.workspace_id, args.item_id, token)
            print(f"Item: {json.dumps(item_info, indent=2)}")
            
            print(f"\n📖 Getting item definition...")
            definition = get_item_definition(args.workspace_id, args.item_id, token)
            if definition:
                print(f"Definition: {json.dumps(definition, indent=2)}")
            
            print(f"\n🧪 Checking data agent endpoints...")
            agent_data = check_data_agent_specific_endpoints(args.workspace_id, args.item_id, token)
            if agent_data:
                print(f"Agent-specific data: {json.dumps(agent_data, indent=2)}")
        else:
            # Analyze all data agents in workspace
            analyze_data_agents(args.workspace_id)
            
    except KeyboardInterrupt:
        print("\n⚠️ Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)