#!/usr/bin/env python3
"""
Compile Data Agent Script

This script compiles a data agent notebook to Fabric Python for        print(f"🚀 Next steps:")
        print(f"1. Upload to Fabric: python upload_agent.py {agent_folder_name} --update")
        print(f"2. Run in Fabric: python run_fabric_notebook.py {agent_folder_name}")
        print(f"3. Your data agent will be created automatically in the Fabric workspace!")using the convert_nb.py function.
It takes a data agent folder name and converts the notebook to a .py file suitable for Fabric workspace.

Usage:
    python compile_data_agent.py <agent_folder_name>
    
Example:
    python compile_data_agent.py jeff
    python compile_data_agent.py sales_agent
"""

import os
import sys
from pathlib import Path

# Add the fabric_notebook_uploader module to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'fabric_notebook_uploader'))

from convert_nb import convert_ipynb_to_fabric_python

def compile_data_agent(agent_folder_name):
    """
    Compile a data agent notebook to Fabric Python format.
    
    Args:
        agent_folder_name: Name of the data agent folder
    """
    print(f"🔧 Compiling Data Agent: {agent_folder_name}")
    print("=" * 50)
    
    # Get the agent folder path
    agent_folder = os.path.join(os.getcwd(), agent_folder_name)
    
    if not os.path.exists(agent_folder):
        print(f"❌ Error: Agent folder '{agent_folder_name}' not found")
        print(f"📂 Looking in: {agent_folder}")
        return False
    
    # Find the notebook file
    notebook_file = None
    expected_notebook = os.path.join(agent_folder, f"{agent_folder_name}.ipynb")
    
    if os.path.exists(expected_notebook):
        notebook_file = expected_notebook
    else:
        # Look for any .ipynb file in the folder
        ipynb_files = [f for f in os.listdir(agent_folder) if f.endswith('.ipynb')]
        if ipynb_files:
            notebook_file = os.path.join(agent_folder, ipynb_files[0])
        else:
            print(f"❌ Error: No notebook file found in '{agent_folder_name}' folder")
            return False
    
    print(f"📓 Found notebook: {os.path.basename(notebook_file)}")
    
    # Define output file path
    output_file = os.path.join(agent_folder, f"{agent_folder_name}_fabric.py")
    
    # Load configuration if available
    config_file = os.path.join(agent_folder, "config.json")
    workspace_id = None
    lakehouse_id = None
    lakehouse_name = None
    include_metadata = False
    
    if os.path.exists(config_file):
        try:
            import json
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Extract lakehouse info if available
            lakehouse_name = config.get('lakehouse_name')
            workspace_id = config.get('workspace_id')
            lakehouse_id = config.get('lakehouse_id')
            
            if lakehouse_name and workspace_id and lakehouse_id:
                include_metadata = True
                print(f"🏠 Using lakehouse metadata: {lakehouse_name}")
            elif lakehouse_name:
                print(f"🏠 Lakehouse name found: {lakehouse_name} (no IDs for metadata)")
            else:
                print("ℹ️  No lakehouse configuration found")
                
        except Exception as e:
            print(f"⚠️  Could not load config: {e}")
    
    try:
        # Convert the notebook to Fabric Python format
        print(f"🔄 Converting notebook to Fabric Python format...")
        
        result = convert_ipynb_to_fabric_python(
            ipynb_file_path=notebook_file,
            output_file_path=output_file,
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            lakehouse_name=lakehouse_name,
            include_lakehouse_metadata=include_metadata
        )
        
        print(f"✅ Successfully compiled to: {os.path.basename(output_file)}")
        print(f"📁 Output location: {output_file}")
        
        # Show file size
        file_size = os.path.getsize(output_file)
        print(f"📊 File size: {file_size:,} bytes")
        
        # Show first few lines as preview
        print(f"\n📋 Preview (first 10 lines):")
        print("-" * 40)
        with open(output_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for i, line in enumerate(lines[:10]):
                print(f"{i+1:2d}: {line.rstrip()}")
            if len(lines) > 10:
                print(f"... ({len(lines) - 10} more lines)")
        
        print(f"\n🚀 Next steps:")
        print(f"1. Upload to Fabric: python upload_agent.py {agent_folder_name} --update")
        print(f"2. Run in Fabric: python run_fabric_notebook.py {agent_folder_name}")
        print(f"3. Your data agent will be created automatically in the Fabric workspace!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during compilation: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main entry point."""
    if len(sys.argv) != 2:
        print("Usage: python compile_data_agent.py <agent_folder_name>")
        print("\nExamples:")
        print("  python compile_data_agent.py jeff")
        print("  python compile_data_agent.py sales_agent")
        print("\nAvailable data agent folders:")
        
        # List available folders
        current_dir = os.getcwd()
        folders = [f for f in os.listdir(current_dir) 
                  if os.path.isdir(f) and not f.startswith('.') and not f.startswith('__')]
        
        # Filter to likely data agent folders (those with .ipynb files)
        agent_folders = []
        for folder in folders:
            ipynb_files = [f for f in os.listdir(folder) if f.endswith('.ipynb')]
            if ipynb_files:
                agent_folders.append(folder)
        
        if agent_folders:
            for folder in sorted(agent_folders):
                print(f"  📂 {folder}")
        else:
            print("  No data agent folders found (folders with .ipynb files)")
        
        sys.exit(1)
    
    agent_folder_name = sys.argv[1]
    
    try:
        success = compile_data_agent(agent_folder_name)
        if not success:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n❌ Cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()