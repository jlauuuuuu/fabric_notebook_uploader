#!/usr/bin/env python3
"""
Configuration setup script for fabric_notebook_uploader.

This script helps you set up your workspace and data agent configurations.

Usage:
    python setup_config.py
"""

from config import config

def main():
    """Interactive configuration setup."""
    print("🔧 Fabric Notebook Uploader Configuration Setup")
    print("=" * 55)
    
    # Check if configuration already exists
    if config.get_active_workspace():
        print("✅ Configuration already exists!")
        workspace = config.get_active_workspace()
        print(f"📂 Active workspace: {workspace.workspace_name}")
        print(f"📊 Data agents: {len(workspace.list_data_agents())}")
        print()
        
        choice = input("Do you want to add a new data agent? (y/N): ").strip().lower()
        if choice not in ['y', 'yes']:
            print("Configuration unchanged.")
            return
    else:
        print("📋 Let's set up your first workspace configuration:")
        print()
        
        # Workspace setup
        workspace_name = input("Workspace name: ").strip()
        workspace_id = input("Workspace ID (GUID): ").strip()
        tenant_id = input("Tenant ID (GUID): ").strip()
        
        # Optional lakehouse
        has_lakehouse = input("Do you have a lakehouse? (y/N): ").strip().lower()
        lakehouse_id = None
        lakehouse_name = None
        
        if has_lakehouse in ['y', 'yes']:
            lakehouse_id = input("Lakehouse ID (GUID): ").strip()
            lakehouse_name = input("Lakehouse name: ").strip()
        
        # Create workspace
        config.add_workspace(
            workspace_name=workspace_name,
            workspace_id=workspace_id,
            tenant_id=tenant_id,
            lakehouse_id=lakehouse_id,
            lakehouse_name=lakehouse_name
        )
        
        config.set_active_workspace(workspace_name)
        print(f"✅ Workspace '{workspace_name}' configured!")
    
    # Data agent setup
    print("\n📊 Adding a data agent:")
    agent_name = input("Data agent name: ").strip()
    notebook_path = input("Notebook path (e.g., notebooks/my_agent.ipynb): ").strip()
    
    # Optional paths
    python_path = input(f"Python output path (default: output/{agent_name}.py): ").strip()
    if not python_path:
        python_path = f"output/{agent_name}.py"
    
    config.add_data_agent(
        agent_name=agent_name,
        notebook_path=notebook_path,
        python_path=python_path
    )
    
    print(f"✅ Data agent '{agent_name}' added!")
    
    # Show next steps
    print("\n🎯 Next Steps:")
    print(f"1. Run: python main.py {agent_name}")
    print("2. Upload notebook to Fabric and create data agent")
    print("3. Update config with agent_id and test_url:")
    print(f"   config.update_data_agent('{agent_name}', agent_id='...', test_url='...')")
    print(f"4. Test: python example.py {agent_name}")
    
    print(f"\n📁 Configuration saved to: {config.config_file}")

if __name__ == "__main__":
    main()