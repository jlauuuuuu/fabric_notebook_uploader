#!/usr/bin/env python3
"""
Test script using the msfabricpysdkcore API to run notebooks.
Uses the global configuration system for workspace and data agent management.

Usage:
    python test_run.py [data_agent_name]
"""

import sys
from fabric_notebook_uploader import run_notebook_by_name, list_notebooks_in_workspace
from config import config

def main():
    """Main test execution logic."""
    # Get data agent name from command line or select from config
    agent_name = None
    if len(sys.argv) > 1:
        agent_name = sys.argv[1]
    else:
        # List available agents with uploaded notebooks
        workspace = config.get_active_workspace()
        if not workspace:
            print("❌ No active workspace configured")
            return
        
        agents = workspace.list_data_agents()
        uploaded_agents = [name for name in agents 
                          if workspace.get_data_agent(name).notebook_id]
        
        if not uploaded_agents:
            print("❌ No data agents with uploaded notebooks found")
            print("Run 'python main.py agent_name' first to upload a notebook")
            return
        
        print("\n📋 Data agents with uploaded notebooks:")
        for i, name in enumerate(uploaded_agents, 1):
            agent = workspace.get_data_agent(name)
            print(f"  {i}. {name} (Notebook ID: {agent.notebook_id})")
        
        try:
            choice = input(f"\nSelect agent to test (1-{len(uploaded_agents)}): ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(uploaded_agents):
                agent_name = uploaded_agents[int(choice) - 1]
            else:
                print("❌ Invalid selection")
                return
        except KeyboardInterrupt:
            print("\n❌ Cancelled")
            return
    
    # Get configuration
    workspace = config.get_active_workspace()
    agent = workspace.get_data_agent(agent_name)
    
    if not agent or not agent.notebook_id:
        print(f"❌ Agent '{agent_name}' not found or notebook not uploaded")
        return
    
    print(f"\n🎯 Running notebook using msfabricpysdkcore API...")
    print(f"📓 Agent: {agent_name}")
    print(f"🏢 Workspace: {workspace.workspace_name}")
    print(f"📝 Notebook ID: {agent.notebook_id}")
    print()
    
    try:
        # Run the notebook
        result = run_notebook_by_name(
            workspace_id=workspace.workspace_id,
            notebook_name=agent_name
        )
        
        print("\n" + "="*60)
        print("📊 EXECUTION SUMMARY")
        print("="*60)
        print(f"Agent: {agent_name}")
        print(f"Status: {result['status']}")
        print(f"Success: {result['success']}")
        print(f"Total Runtime: {result['total_runtime_str']}")
        print(f"Job Instance ID: {result['job_id']}")
        
        if result.get('start_time_utc'):
            print(f"Start Time: {result['start_time_utc']}")
        if result.get('end_time_utc'):
            print(f"End Time: {result['end_time_utc']}")
        
        print("="*60)
        
        # Update agent status based on execution result
        if result['success']:
            config.update_data_agent(agent_name, status="notebook_executed")
        
        print("\n📋 Available notebooks in workspace:")
        notebooks = list_notebooks_in_workspace(workspace.workspace_id)
        for nb in notebooks:
            print(f"- {nb['display_name']} (ID: {nb['id']})")
            
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()