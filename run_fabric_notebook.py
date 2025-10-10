#!/usr/bin/env python3
"""
Run Fabric Notebook Script

This script runs an uploaded data agent notebook in the Microsoft Fabric workspace.
It uses the run_nb.py functions and follows the test_run.py pattern for configuration management.

Usage:
    python run_fabric_notebook.py <agent_folder_name>
    
Example:
    python run_fabric_notebook.py jeff
    python run_fabric_notebook.py sales_agent
"""

import os
import sys
import json
from pathlib import Path

# Add the fabric_notebook_uploader module to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'fabric_notebook_uploader'))

from run_nb import run_notebook_by_name, run_notebook_by_id

def run_fabric_notebook(agent_folder_name, workspace_id=None):
    """
    Run a data agent notebook in Fabric workspace.
    
    Args:
        agent_folder_name: Name of the data agent folder
        workspace_id: Fabric workspace ID (optional, can be loaded from config)
    """
    print(f"🎯 Running Data Agent Notebook: {agent_folder_name}")
    print("=" * 60)
    
    # Get the agent folder path
    agent_folder = os.path.join(os.getcwd(), agent_folder_name)
    
    if not os.path.exists(agent_folder):
        print(f"❌ Error: Agent folder '{agent_folder_name}' not found")
        print(f"📂 Looking in: {agent_folder}")
        return False
    
    # Load configuration
    config_file = os.path.join(agent_folder, "config.json")
    
    if not os.path.exists(config_file):
        print(f"❌ Error: Config file not found in '{agent_folder_name}' folder")
        print(f"📄 Looking for: {config_file}")
        return False
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        agent_name = config.get('agent_name', agent_folder_name)
        notebook_id = config.get('notebook_id')
        notebook_name = config.get('notebook_name')
        
        print(f"📋 Agent name: {agent_name}")
        print(f"📓 Notebook name: {notebook_name}")
        print(f"🆔 Notebook ID: {notebook_id}")
        
        # Get workspace ID from config if not provided
        if not workspace_id:
            workspace_id = config.get('workspace_id')
            if workspace_id:
                print(f"🏢 Using workspace ID from agent config: {workspace_id}")
        
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        return False
    
    # Try to load workspace ID from global config if still not available
    if not workspace_id:
        try:
            global_config_file = "config.json"
            if os.path.exists(global_config_file):
                with open(global_config_file, 'r', encoding='utf-8') as f:
                    global_config = json.load(f)
                
                # Look for workspace ID in the active workspace
                if global_config.get('active_workspace'):
                    active_workspace_id = global_config['active_workspace']
                    workspaces = global_config.get('workspaces', {})
                    if active_workspace_id in workspaces:
                        workspace_data = workspaces[active_workspace_id]
                        workspace_id = workspace_data.get('workspace_id')
                        if workspace_id:
                            print(f"🏢 Using workspace ID from global config: {workspace_id}")
        except Exception as e:
            print(f"⚠️  Could not load global config: {e}")
    
    # Check if we have the necessary information
    if not workspace_id:
        print("❌ Error: No workspace ID found")
        print("💡 Options:")
        print("   1. Provide as command line argument: python run_fabric_notebook.py <agent> <workspace_id>")
        print("   2. Add 'workspace_id' to the agent's config.json file")
        print("   3. Set up global configuration with workspace ID")
        return False
    
    if not notebook_id and not notebook_name:
        print("❌ Error: No notebook ID or name found in config")
        print("💡 Make sure the notebook has been uploaded first:")
        print("   python upload_agent.py {agent_folder_name}")
        return False
    
    try:
        print(f"\\n🚀 Starting notebook execution...")
        print(f"   🏢 Workspace ID: {workspace_id}")
        
        # Run the notebook (prefer by name if available, fallback to ID)
        if notebook_name:
            print(f"   📓 Running by name: {notebook_name}")
            result = run_notebook_by_name(
                workspace_id=workspace_id,
                notebook_name=notebook_name
            )
        elif notebook_id:
            print(f"   🆔 Running by ID: {notebook_id}")
            result = run_notebook_by_id(
                workspace_id=workspace_id,
                notebook_id=notebook_id
            )
        
        print("\\n" + "="*60)
        print("📊 EXECUTION SUMMARY")
        print("="*60)
        print(f"Agent: {agent_name}")
        print(f"Notebook: {notebook_name or notebook_id}")
        print(f"Status: {result['status']}")
        print(f"Success: {result['success']}")
        print(f"Total Runtime: {result['total_runtime_str']}")
        print(f"Job Instance ID: {result['job_id']}")
        print("="*60)
        
        # Update config with execution results
        try:
            config['last_execution'] = {
                'job_id': result['job_id'],
                'status': result['status'],
                'success': result['success'],
                'runtime': result['total_runtime_str'],
                'timestamp': __import__('datetime').datetime.now().isoformat()
            }
            
            # Update overall status
            if result['success']:
                config['status'] = 'executed_successfully'
            else:
                config['status'] = 'execution_failed'
            
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            print(f"\\n📄 Updated config.json with execution results")
            
        except Exception as e:
            print(f"⚠️  Could not update config: {e}")
        
        if result['success']:
            print(f"\\n🎉 Data agent '{agent_name}' executed successfully!")
            print(f"💡 Your data agent should now be created in the Fabric workspace.")
            print(f"🔗 You can now query your data agent or check its status in Fabric.")
        else:
            print(f"\\n❌ Execution failed for '{agent_name}'")
            print(f"💡 Check the notebook in Fabric workspace for error details.")
        
        return result['success']
        
    except Exception as e:
        print(f"❌ Error during execution: {e}")
        import traceback
        traceback.print_exc()
        
        print(f"\\n🔧 Troubleshooting:")
        print(f"1. Check your authentication (run 'az login' if needed)")
        print(f"2. Verify the workspace ID is correct")
        print(f"3. Ensure the notebook exists in the workspace")
        print(f"4. Check your internet connection")
        print(f"5. Make sure you have permissions to run notebooks in the workspace")
        
        return False

def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python run_fabric_notebook.py <agent_folder_name> [workspace_id]")
        print("\\nExamples:")
        print("  python run_fabric_notebook.py jeff")
        print("  python run_fabric_notebook.py sales_agent abc123-def456-789ghi")
        print("\\nAvailable data agent folders with uploaded notebooks:")
        
        # List available folders with uploaded notebooks (have notebook_id in config)
        current_dir = os.getcwd()
        folders = [f for f in os.listdir(current_dir) 
                  if os.path.isdir(f) and not f.startswith('.') and not f.startswith('__')]
        
        # Filter to folders with notebook information
        agent_folders = []
        for folder in folders:
            config_file = os.path.join(folder, "config.json")
            if os.path.exists(config_file):
                try:
                    with open(config_file, 'r', encoding='utf-8') as f:
                        config = json.load(f)
                    
                    notebook_id = config.get('notebook_id')
                    notebook_name = config.get('notebook_name')
                    
                    if notebook_id or notebook_name:
                        agent_folders.append((folder, notebook_name or notebook_id))
                except:
                    continue
        
        if agent_folders:
            for folder, notebook_info in sorted(agent_folders):
                print(f"  📂 {folder} (notebook: {notebook_info})")
        else:
            print("  No uploaded data agent notebooks found")
            print("  💡 Run 'python upload_agent.py <agent_name>' to upload first")
        
        sys.exit(1)
    
    agent_folder_name = sys.argv[1]
    workspace_id = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        success = run_fabric_notebook(agent_folder_name, workspace_id)
        if not success:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\\n❌ Cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()