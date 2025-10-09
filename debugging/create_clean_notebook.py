#!/usr/bin/env python3
"""
Alternative approach: Create a version of the notebook without %pip commands
and pre-install the required libraries in a Fabric Environment instead.
"""

from fabric_notebook_uploader import convert_ipynb_to_fabric_python, create_notebook_from_fabric_python
import json

def create_notebook_without_pip_commands(ipynb_file_path, output_notebook_name):
    """
    Create a version of the notebook without %pip commands.
    This approach requires pre-installing libraries in a Fabric Environment.
    """
    
    # Read the original notebook
    with open(ipynb_file_path, 'r', encoding='utf-8') as f:
        notebook_data = json.load(f)
    
    # Filter out cells with %pip commands
    filtered_cells = []
    removed_pip_commands = []
    
    for cell in notebook_data.get('cells', []):
        if cell.get('cell_type') == 'code':
            source_lines = cell.get('source', [])
            if source_lines:
                first_line = source_lines[0].strip() if source_lines else ""
                
                # Check if this is a %pip command
                if first_line.startswith('%pip'):
                    removed_pip_commands.append(first_line)
                    print(f"⚠️  Removed pip command: {first_line}")
                    continue
        
        # Keep this cell
        filtered_cells.append(cell)
    
    # Update notebook data
    notebook_data['cells'] = filtered_cells
    
    # Save the modified notebook
    modified_notebook_path = ipynb_file_path.replace('.ipynb', '_no_pip.ipynb')
    with open(modified_notebook_path, 'w', encoding='utf-8') as f:
        json.dump(notebook_data, f, indent=2)
    
    print(f"📝 Created modified notebook: {modified_notebook_path}")
    print(f"🗑️  Removed {len(removed_pip_commands)} pip commands:")
    for cmd in removed_pip_commands:
        print(f"   - {cmd}")
    
    # Convert to Fabric format
    fabric_content = convert_ipynb_to_fabric_python(
        ipynb_file_path=modified_notebook_path,
        output_file_path=f"fabric_notebooks/{output_notebook_name}_no_pip.py",
        workspace_id="c7d1ecc1-d8fa-477c-baf1-47528abf9fe5",
        lakehouse_id=None,
        lakehouse_name=None,
        include_lakehouse_metadata=False
    )
    
    print(f"✅ Converted to Fabric format: fabric_notebooks/{output_notebook_name}_no_pip.py")
    
    # Upload to Fabric
    try:
        notebook = create_notebook_from_fabric_python(
            workspace_id="c7d1ecc1-d8fa-477c-baf1-47528abf9fe5",
            fabric_python_content=fabric_content,
            notebook_name=f"{output_notebook_name}_no_pip"
        )
        print(f"🚀 Uploaded to Fabric: {output_notebook_name}_no_pip (ID: {notebook.id})")
        return notebook.id
    except Exception as e:
        print(f"⚠️  Upload failed (notebook may already exist): {e}")
        return None

if __name__ == "__main__":
    print("🔧 Creating notebook version without %pip commands...")
    print("📋 Note: You'll need to pre-install 'fabric-data-agent-sdk' in a Fabric Environment")
    print("   and attach it to the notebook for this to work properly.")
    print()
    
    notebook_id = create_notebook_without_pip_commands(
        ipynb_file_path="notebooks/data_agent_creation.ipynb",
        output_notebook_name="data_agent_creation_clean"
    )
    
    if notebook_id:
        print()
        print("📋 NEXT STEPS:")
        print("1. Go to Fabric portal -> Data Engineering -> Environments")
        print("2. Create a new Environment or use an existing one")
        print("3. Add 'fabric-data-agent-sdk' to the environment libraries")
        print("4. Attach the environment to your notebook")
        print("5. Run the notebook - it should work without %pip commands")
    
    print()
    print("🔍 Alternative: Try running this clean notebook with our scripts:")
    print(f"   python direct_api_run.py  # (update NOTEBOOK_NAME to 'data_agent_creation_clean_no_pip')")
    print(f"   python test_run.py        # (update NOTEBOOK_NAME to 'data_agent_creation_clean_no_pip')")