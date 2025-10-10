#!/usr/bin/env python3
"""
Enhanced Data Agent Details Script - Get comprehensive information about Fabric Data Agents

This script discovers and analyzes Fabric Data Agents to find:
- Basic properties (name, ID, type, description)
- Publish/deployment status
- Configuration details
- Associated resources and dependencies
- Management endpoints and capabilities
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
        result = subprocess.run(
            ["az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com"],
            capture_output=True,
            text=True,
            check=True,
            shell=True
        )
        token_info = json.loads(result.stdout)
        return token_info["accessToken"]
    except Exception as e:
        print(f"❌ Failed to get Azure CLI token: {e}")
        sys.exit(1)


def api_request(method: str, url: str, token: str, data: Optional[Dict] = None, silent: bool = False) -> Optional[requests.Response]:
    """Make API request with error handling."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.request(method, url, headers=headers, json=data, timeout=30)
        
        if not silent:
            print(f"🔍 {method} {url} - Status: {response.status_code}")
        
        return response
    except requests.exceptions.RequestException as e:
        if not silent:
            print(f"❌ Request failed: {e}")
        return None


def explore_data_agent_endpoints(workspace_id: str, agent_id: str, token: str) -> Dict[str, Any]:
    """Comprehensively explore all possible data agent endpoints."""
    
    print(f"\n🕵️ Comprehensive Data Agent Endpoint Discovery")
    print(f"Agent ID: {agent_id}")
    print(f"Workspace ID: {workspace_id}")
    
    results = {}
    
    # Define all possible endpoint patterns to try
    endpoint_patterns = [
        # Direct data agent endpoints
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/status",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/configuration",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/properties",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/definition",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/metadata",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/deployment",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/publish",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/publishing",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/state",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/details",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/info",
        
        # Management endpoints
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/management",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/management/status",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/management/configuration",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/management/deployment",
        
        # Item-based data agent endpoints
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{agent_id}/dataagent",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{agent_id}/dataagent/status",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{agent_id}/dataagent/configuration",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{agent_id}/dataagent/properties",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{agent_id}/dataagent/deployment",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{agent_id}/dataagent/publish",
        
        # Alternative item endpoints
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{agent_id}/properties",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{agent_id}/metadata",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{agent_id}/status",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{agent_id}/state",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{agent_id}/deployment",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{agent_id}/publish",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{agent_id}/details",
        
        # Definition endpoints (different methods)
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{agent_id}/GetDefinition",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{agent_id}/definition",
        
        # Resource-specific endpoints
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/datasources",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/connections",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/endpoints",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/resources",
        
        # AI/ML specific endpoints
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/model",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/training",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/inference",
        
        # Versioning and lifecycle
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/versions",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/lifecycle",
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataagents/{agent_id}/operations"
    ]
    
    successful_endpoints = []
    
    print(f"\n🔍 Testing {len(endpoint_patterns)} potential endpoints...")
    
    for i, endpoint in enumerate(endpoint_patterns):
        print(f"  [{i+1:2d}/{len(endpoint_patterns)}] Testing: {endpoint.split('/')[-1] if '/' in endpoint else endpoint}")
        
        # Try GET first
        response = api_request("GET", endpoint, token, silent=True)
        if response and response.status_code in [200, 202]:
            try:
                data = response.json() if response.text.strip() else {"status": "empty_response"}
                results[endpoint] = {
                    "method": "GET",
                    "status_code": response.status_code,
                    "data": data
                }
                successful_endpoints.append(endpoint)
                print(f"    ✅ GET Success! Status: {response.status_code}")
            except json.JSONDecodeError:
                results[endpoint] = {
                    "method": "GET", 
                    "status_code": response.status_code,
                    "data": response.text
                }
                successful_endpoints.append(endpoint)
                print(f"    ✅ GET Success! Status: {response.status_code} (text response)")
            continue
        
        # Try POST for definition-like endpoints
        if any(keyword in endpoint.lower() for keyword in ['definition', 'getdefinition']):
            response = api_request("POST", endpoint, token, silent=True)
            if response and response.status_code in [200, 202]:
                try:
                    data = response.json() if response.text.strip() else {"status": "empty_response"}
                    results[endpoint] = {
                        "method": "POST",
                        "status_code": response.status_code,
                        "data": data
                    }
                    successful_endpoints.append(endpoint)
                    print(f"    ✅ POST Success! Status: {response.status_code}")
                except json.JSONDecodeError:
                    results[endpoint] = {
                        "method": "POST",
                        "status_code": response.status_code, 
                        "data": response.text
                    }
                    successful_endpoints.append(endpoint)
                    print(f"    ✅ POST Success! Status: {response.status_code} (text response)")
    
    print(f"\n🎉 Discovery Complete! Found {len(successful_endpoints)} working endpoints:")
    for endpoint in successful_endpoints:
        method = results[endpoint]['method']
        status = results[endpoint]['status_code']
        endpoint_name = endpoint.split('/')[-1]
        print(f"  ✅ {method} {endpoint_name} (Status: {status})")
    
    return results


def analyze_data_agent_responses(results: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze the responses to extract useful information."""
    
    print(f"\n📊 Analyzing Data Agent Information")
    print(f"="*80)
    
    analysis = {
        "basic_info": {},
        "publish_deployment_info": {},
        "configuration_info": {},
        "resource_info": {},
        "status_info": {},
        "metadata_info": {}
    }
    
    for endpoint, result in results.items():
        endpoint_name = endpoint.split('/')[-1]
        data = result.get('data', {})
        
        print(f"\n🔍 Analyzing: {endpoint_name}")
        print(f"   Method: {result['method']} | Status: {result['status_code']}")
        
        if isinstance(data, dict):
            # Look for specific types of information
            
            # Basic information
            basic_keys = ['id', 'displayName', 'description', 'type', 'workspaceId']
            basic_info = {k: v for k, v in data.items() if k in basic_keys}
            if basic_info:
                analysis['basic_info'].update(basic_info)
                print(f"   📋 Basic Info: {basic_info}")
            
            # Publish/Deployment related
            publish_keys = [k for k in data.keys() if any(term in k.lower() for term in ['publish', 'deploy', 'live', 'active', 'enabled', 'running', 'status'])]
            if publish_keys:
                publish_info = {k: data[k] for k in publish_keys}
                analysis['publish_deployment_info'].update(publish_info)
                print(f"   🚀 Publish/Deploy Info: {publish_info}")
            
            # Configuration related
            config_keys = [k for k in data.keys() if any(term in k.lower() for term in ['config', 'setting', 'parameter', 'option'])]
            if config_keys:
                config_info = {k: data[k] for k in config_keys}
                analysis['configuration_info'].update(config_info)
                print(f"   ⚙️ Configuration Info: {config_info}")
            
            # Resource related
            resource_keys = [k for k in data.keys() if any(term in k.lower() for term in ['datasource', 'connection', 'endpoint', 'resource', 'model'])]
            if resource_keys:
                resource_info = {k: data[k] for k in resource_keys}
                analysis['resource_info'].update(resource_info)
                print(f"   🔗 Resource Info: {resource_info}")
            
            # Status related
            status_keys = [k for k in data.keys() if any(term in k.lower() for term in ['status', 'state', 'health', 'condition'])]
            if status_keys:
                status_info = {k: data[k] for k in status_keys}
                analysis['status_info'].update(status_info)
                print(f"   📊 Status Info: {status_info}")
            
            # Metadata
            meta_keys = [k for k in data.keys() if any(term in k.lower() for term in ['meta', 'tag', 'property', 'attribute', 'created', 'modified', 'version'])]
            if meta_keys:
                meta_info = {k: data[k] for k in meta_keys}
                analysis['metadata_info'].update(meta_info)
                print(f"   🏷️ Metadata Info: {meta_info}")
            
            # Any other interesting keys
            other_keys = [k for k in data.keys() if k not in basic_keys + publish_keys + config_keys + resource_keys + status_keys + meta_keys]
            if other_keys:
                other_info = {k: data[k] for k in other_keys}
                print(f"   ❓ Other Info: {other_info}")
        
        elif isinstance(data, str) and data.strip():
            print(f"   📝 Text Response: {data[:200]}{'...' if len(data) > 200 else ''}")
    
    return analysis


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Enhanced Data Agent Analysis - Find publish status and detailed info")
    parser.add_argument("workspace_id", help="Fabric workspace ID")
    parser.add_argument("agent_id", help="Data Agent ID")
    
    args = parser.parse_args()
    
    print(f"🚀 Enhanced Data Agent Analysis")
    print(f"Workspace: {args.workspace_id}")
    print(f"Agent ID: {args.agent_id}")
    
    # Get token
    token = get_azure_cli_token()
    
    # Comprehensive endpoint exploration
    results = explore_data_agent_endpoints(args.workspace_id, args.agent_id, token)
    
    if not results:
        print(f"❌ No working endpoints found for data agent {args.agent_id}")
        return
    
    # Analyze the responses
    analysis = analyze_data_agent_responses(results)
    
    # Final summary
    print(f"\n" + "="*80)
    print(f"📋 FINAL SUMMARY FOR DATA AGENT: {args.agent_id}")
    print(f"="*80)
    
    for category, info in analysis.items():
        if info:
            print(f"\n{category.replace('_', ' ').title()}:")
            for key, value in info.items():
                print(f"  {key}: {value}")
    
    # Save detailed results to file
    output_file = f"data_agent_analysis_{args.agent_id}.json"
    with open(output_file, 'w') as f:
        json.dump({
            "agent_id": args.agent_id,
            "workspace_id": args.workspace_id,
            "endpoint_results": results,
            "analysis": analysis
        }, f, indent=2)
    
    print(f"\n💾 Detailed results saved to: {output_file}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)