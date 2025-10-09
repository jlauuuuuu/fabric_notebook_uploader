#!/usr/bin/env python3
"""
Configuration status checker for fabric_notebook_uploader.

Usage:
    python check_config.py
"""

from config import config
import os

def main():
    """Display current configuration status."""
    print("🔧 Fabric Notebook Uploader - Configuration Status")
    print("=" * 60)
    
    # Check if configuration exists
    if not os.path.exists(config.config_file):
        print("❌ No configuration found")
        print("   Run: python setup_config.py")
        return
    
    # Show active workspace
    workspace = config.get_active_workspace()
    if not workspace:
        print("❌ No active workspace configured")
        print("   Run: python setup_config.py")
        return
    
    print(f"📂 Active Workspace: {workspace.workspace_name}")
    print(f"   Workspace ID: {workspace.workspace_id}")
    print(f"   Tenant ID: {workspace.tenant_id}")
    
    if workspace.lakehouse_id:
        print(f"   🏠 Lakehouse: {workspace.lakehouse_name} ({workspace.lakehouse_id})")
    else:
        print("   🏠 Lakehouse: Not configured")
    
    # Show data agents
    agents = workspace.list_data_agents()
    print(f"\n🤖 Data Agents: {len(agents)}")
    
    if agents:
        for agent_name in agents:
            agent = workspace.get_data_agent(agent_name)
            stage = agent.get_lifecycle_stage()
            print(f"   • {agent_name} (Stage {stage}/5: {agent.status})")
            
            if agent.notebook_path:
                exists = "✅" if os.path.exists(agent.notebook_path) else "❌"
                print(f"     📓 Notebook: {exists} {agent.notebook_path}")
            
            if agent.python_path:
                exists = "✅" if os.path.exists(agent.python_path) else "❌"  
                print(f"     🐍 Python: {exists} {agent.python_path}")
            
            if agent.notebook_id:
                print(f"     ☁️ Fabric ID: {agent.notebook_id}")
            
            if agent.test_url:
                print(f"     🧪 Test URL: {agent.test_url[:50]}...")
            
            missing = agent.get_missing_config()
            if missing:
                print(f"     ⚠️ Missing: {', '.join(missing)}")
            
            print()
    else:
        print("   No data agents configured")
        print("   Add one with: config.add_data_agent('name', notebook_path='...')")
    
    print("\n📋 Next Steps:")
    if not agents:
        print("   1. Run: python setup_config.py")
        print("   2. Add data agents to your configuration")
    else:
        ready_agents = [name for name in agents 
                       if workspace.get_data_agent(name).notebook_path and 
                          os.path.exists(workspace.get_data_agent(name).notebook_path)]
        if ready_agents:
            print(f"   1. Convert: python main.py {ready_agents[0]}")
            print(f"   2. Upload to Fabric and create data agent")
            print(f"   3. Test: python example.py {ready_agents[0]}")
        else:
            print("   1. Ensure notebook files exist")
            print("   2. Update agent configurations with correct paths")

if __name__ == "__main__":
    main()