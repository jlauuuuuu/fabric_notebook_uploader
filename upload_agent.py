#!/usr/bin/env python3
"""
Upload Data Agent Script

This script uploads a compiled data agent to Microsoft Fabric workspace as a notebook.
If the notebook already exists, it can update the content instead of creating a new one.
This SOLVES the name availability timing issues by updating existing notebooks!

Usage:
    python upload_agent.py <agent_folder_name> [workspace_id]           # Create new or prompt to update
    python upload_agent.py <agent_folder_name> --update [workspace_id]  # Force update existing notebook
    
Examples:
    python upload_agent.py jeff                                 # Create new or ask to update
    python upload_agent.py jeff abc123-def456-789ghi            # Create with specific workspace
    python upload_agent.py jeff --update                        # Update existing notebook (no timing issues!)
    python upload_agent.py sales_agent --update workspace-id    # Update with specific workspace

✅ UPDATE MODE BENEFITS:
- No name availability delays (uses existing notebook)
- Immediate execution possible after update
- Preserves notebook ID and workspace integration
- Processes in background (202 accepted response)
"""

import os
import sys
import json
from pathlib import Path

# Add the fabric_notebook_uploader module to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'fabric_notebook_uploader'))

from create_nb import create_notebook_from_fabric_python

def get_azure_cli_token():
    """Get access token using Azure CLI."""
    try:
        import subprocess
        cmd = ['az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com/', '--query', 'accessToken', '-o', 'tsv']
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, shell=True)
        return result.stdout.strip()
    except Exception as e:
        print(f"⚠️  Could not get Azure CLI token: {e}")
        return None

def update_existing_notebook(agent_folder_name, workspace_id, notebook_id, fabric_python_content):
    """
    Update existing notebook content using direct Fabric REST API.
    """
    try:
        import base64
        import requests
        
        print(f"🔄 Updating existing notebook content...")
        
        # Get access token
        access_token = get_azure_cli_token()
        if not access_token:
            print(f"❌ Could not get access token for direct API call")
            return False
        
        # Prepare the notebook definition for FabricGitSource format
        fabric_python_base64 = base64.b64encode(fabric_python_content.encode('utf-8')).decode('utf-8')
        
        definition = {
            "format": "fabricGitSource",
            "parts": [
                {
                    "path": "notebook-content.py",
                    "payload": fabric_python_base64,
                    "payloadType": "InlineBase64"
                }
            ]
        }
        
        # Make direct API request
        update_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/updateDefinition"
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        payload = {"definition": definition}
        response = requests.post(update_url, headers=headers, json=payload)
        
        print(f"📡 API Response: {response.status_code}")
        
        if response.status_code == 200:
            print(f"✅ Update completed immediately")
            return True
        elif response.status_code == 202:
            operation_id = response.headers.get('x-ms-operation-id', 'Unknown')
            print(f"🔄 Update accepted - Operation ID: {operation_id}")
            print(f"✅ Content update is processing in background")
            print(f"💡 Check your Fabric workspace in 1-2 minutes")
            return True
        else:
            print(f"❌ Update failed: {response.status_code} - {response.text}")
            return False
        
    except Exception as e:
        print(f"❌ Error updating notebook: {e}")
        return False

def upload_data_agent(agent_folder_name, workspace_id=None, force_update=False):
    """
    Upload a compiled data agent to Fabric workspace as a notebook.
    
    Args:
        agent_folder_name: Name of the data agent folder
        workspace_id: Fabric workspace ID (optional, can be loaded from config)
    """
    print(f"🚀 Uploading Data Agent: {agent_folder_name}")
    print("=" * 50)
    
    # Get the agent folder path
    agent_folder = os.path.join(os.getcwd(), agent_folder_name)
    
    if not os.path.exists(agent_folder):
        print(f"❌ Error: Agent folder '{agent_folder_name}' not found")
        print(f"📂 Looking in: {agent_folder}")
        return False
    
    # Find the compiled Fabric Python file
    fabric_python_file = os.path.join(agent_folder, f"{agent_folder_name}_fabric.py")
    
    if not os.path.exists(fabric_python_file):
        print(f"❌ Error: Compiled file '{agent_folder_name}_fabric.py' not found")
        print(f"📂 Looking for: {fabric_python_file}")
        print(f"💡 Tip: Run 'python compile_script.py {agent_folder_name}' first")
        return False
    
    print(f"📄 Found compiled file: {os.path.basename(fabric_python_file)}")
    
    # Load configuration if available
    config_file = os.path.join(agent_folder, "config.json")
    agent_name = agent_folder_name
    
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Get agent name from config if available
            if config.get('agent_name'):
                agent_name = config['agent_name']
                print(f"📋 Using agent name from config: {agent_name}")
            
            # Get workspace ID from config if not provided
            if not workspace_id and config.get('workspace_id'):
                workspace_id = config['workspace_id']
                print(f"🏢 Using workspace ID from agent config: {workspace_id}")
            elif not workspace_id:
                print("⚠️  No workspace ID found in agent config")
                
        except Exception as e:
            print(f"⚠️  Could not load config: {e}")
    
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
    
    # Check if workspace ID is available
    if not workspace_id:
        print("❌ Error: No workspace ID provided")
        print("💡 Options:")
        print("   1. Provide as command line argument: python upload_agent.py <agent> <workspace_id>")
        print("   2. Add 'workspace_id' to the agent's config.json file")
        print("   3. Set up global configuration with workspace ID")
        return False
    
    # Read the compiled Fabric Python content
    try:
        with open(fabric_python_file, 'r', encoding='utf-8') as f:
            fabric_python_content = f.read()
        
        file_size = len(fabric_python_content)
        print(f"📊 File size: {file_size:,} characters")
        
    except Exception as e:
        print(f"❌ Error reading compiled file: {e}")
        return False
    
    # Create notebook name following the pattern: {agent_name}_creation_notebook
    notebook_name = f"{agent_name}_creation_notebook"
    
    # Check if notebook already exists and should be updated
    existing_notebook_id = config.get('notebook_id') if os.path.exists(config_file) else None
    
    try:
        print(f"🔄 Processing Fabric workspace...")
        print(f"   📓 Notebook name: {notebook_name}")
        print(f"   🏢 Workspace ID: {workspace_id}")
        
        if existing_notebook_id and (force_update or input(f"\\n📓 Notebook already exists (ID: {existing_notebook_id})\\n🔄 Update existing notebook content? (Y/n): ").strip().lower() in ['', 'y', 'yes']):
            print(f"🔄 Updating existing notebook content...")
            success = update_existing_notebook(
                agent_folder_name=agent_folder_name,
                workspace_id=workspace_id,
                notebook_id=existing_notebook_id,
                fabric_python_content=fabric_python_content
            )
            
            if success:
                print(f"✅ Successfully updated notebook content!")
                notebook_id = existing_notebook_id
                display_name = notebook_name
            else:
                print(f"❌ Failed to update existing notebook, trying to create new one...")
                raise Exception("Update failed, falling back to create")
        else:
            print(f"📤 Creating new notebook...")
            # Upload to Fabric
            notebook = create_notebook_from_fabric_python(
                workspace_id=workspace_id,
                fabric_python_content=fabric_python_content,
                notebook_name=notebook_name
            )
            
            print(f"✅ Successfully uploaded to Fabric!")
            
            # Access notebook properties (following main.py pattern)
            notebook_id = notebook.id if hasattr(notebook, 'id') else "Unknown"
            display_name = getattr(notebook, 'display_name', notebook_name)
        
        print(f"📓 Notebook ID: {notebook_id}")
        print(f"📝 Display Name: {display_name}")
        
        # Update config with notebook information if config exists
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                
                # Update with notebook info
                config['notebook_id'] = str(notebook_id)
                config['notebook_name'] = str(display_name)
                config['workspace_id'] = workspace_id
                config['status'] = 'uploaded'
                
                with open(config_file, 'w', encoding='utf-8') as f:
                    json.dump(config, f, indent=2, ensure_ascii=False)
                
                print(f"📄 Updated config.json with notebook information")
                
            except Exception as e:
                print(f"⚠️  Could not update config: {e}")
        
        print(f"\\n🎯 Next steps:")
        print(f"1. Run the notebook: python run_fabric_notebook.py {agent_folder_name}")
        print(f"2. Your data agent will be created automatically in Fabric!")
        print(f"3. Alternatively, you can run the notebook manually in the Fabric workspace")
        print(f"4. Test your data agent with queries once created")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during upload: {e}")
        import traceback
        traceback.print_exc()
        
        print(f"\\n🔧 Troubleshooting:")
        print(f"1. Check your authentication (run 'az login' if needed)")
        print(f"2. Verify the workspace ID is correct")
        print(f"3. Ensure you have permissions to create notebooks in the workspace")
        print(f"4. Check your internet connection")
        
        return False

def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python upload_agent.py <agent_folder_name> [--update] [workspace_id]")
        print("\\nExamples:")
        print("  python upload_agent.py jeff                      # Create new or prompt to update")
        print("  python upload_agent.py jeff --update             # Update existing (no name conflicts!)")
        print("  python upload_agent.py sales_agent workspace-id  # Create with specific workspace")
        print("\\n🚀 TIP: Use --update to avoid name availability timing issues!")
        print("\\nAvailable data agent folders:")
        
        # List available folders with compiled files
        current_dir = os.getcwd()
        folders = [f for f in os.listdir(current_dir) 
                  if os.path.isdir(f) and not f.startswith('.') and not f.startswith('__')]
        
        # Filter to folders with compiled files
        agent_folders = []
        for folder in folders:
            fabric_file = os.path.join(folder, f"{folder}_fabric.py")
            if os.path.exists(fabric_file):
                agent_folders.append(folder)
        
        if agent_folders:
            for folder in sorted(agent_folders):
                print(f"  📂 {folder} (has {folder}_fabric.py)")
        else:
            print("  No compiled data agent folders found")
            print("  💡 Run 'python compile_script.py <agent_name>' to compile first")
        
        sys.exit(1)
    
    agent_folder_name = sys.argv[1]
    
    # Check for --update flag
    force_update = False
    workspace_id = None
    
    for arg in sys.argv[2:]:
        if arg == '--update':
            force_update = True
        else:
            workspace_id = arg
    
    try:
        success = upload_data_agent(agent_folder_name, workspace_id, force_update)
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