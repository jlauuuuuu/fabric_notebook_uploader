#!/usr/bin/env python3
"""
Data Agent Scaffolding Script

Creates a new data agent folder structure with configuration files and templates.
This is the first step in the data agent creation process.

Usage:
    python create_data_agent.py
"""

import os
import sys
import json
from datetime import datetime

def create_data_agent_scaffold(agent_name):
    """Create a new data agent folder structure with configuration files."""
    print("🤖 Data Agent Scaffolding Tool")
    print("=" * 40)
    
    # Clean the name for folder usage
    folder_name = agent_name.replace(" ", "_").replace("-", "_").lower()
    print(f"Creating data agent: {agent_name}")
    print(f"Folder name: {folder_name}")
    
    # Create folder structure
    agent_folder = os.path.join(os.getcwd(), folder_name)
    
    if os.path.exists(agent_folder):
        print(f"❌ Folder '{folder_name}' already exists")
        overwrite = input("Overwrite existing folder? (y/N): ").strip().lower()
        if overwrite not in ['y', 'yes']:
            print("❌ Cancelled")
            return
    
    # Create the folder
    os.makedirs(agent_folder, exist_ok=True)
    print(f"📁 Created folder: {agent_folder}")
    
    # Create config.json with same structure as existing config system
    notebook_filename = f"{folder_name}.ipynb"
    python_filename = f"{folder_name}.py"
    
    config_data = {
        "agent_name": agent_name,
        "folder_name": folder_name,
        "created_date": datetime.now().isoformat(),
        "lakehouse_name": "",
        "table_names": [],
        "instructions": "",
        "data_source_notes": "",
        "few_shot_examples": {},
        "status": "scaffolded",
        "notebook_path": os.path.join(agent_folder, notebook_filename),
        "python_path": os.path.join(agent_folder, python_filename),
        "notebook_id": "",
        "test_url": ""
    }
    
    config_file = os.path.join(agent_folder, "config.json")
    with open(config_file, 'w', encoding='utf-8') as f:
        json.dump(config_data, f, indent=2, ensure_ascii=False)
    print(f"📄 Created config file: {config_file}")
    

    
    # Create a README for the folder
    readme_content = f"""# Data Agent: {agent_name}

Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Folder: {folder_name}

## Files

- `config.json` - Basic configuration (optional for reference)
- `{folder_name}.ipynb` - Complete data agent notebook with embedded configuration
- `README.md` - This file

## Next Steps

1. Open `{folder_name}.ipynb` in your editor or Fabric workspace
2. Update the configuration section in the notebook:
   - Set your `lakehouse_name`
   - Update `table_names` list
   - Customize the instructions section
   - Add your data source notes
   - Update few-shot examples

## Features

The generated notebook includes:
- ✅ Complete data agent creation workflow
- ✅ Embedded configuration (no external files needed)
- ✅ Template sections for instructions and examples
- ✅ Helper functions for common SDK issues
- ✅ Ready to run in Fabric workspace

## Usage

1. Upload `{folder_name}.ipynb` to your Fabric workspace
2. Edit the configuration cells as needed
3. Run all cells to create your data agent

## Integration

For use with fabric_notebook_uploader workflow:
```bash
python main.py {agent_name}      # Upload notebook to Fabric
python test_run.py {agent_name}  # Execute in Fabric
```
"""
    
    readme_file = os.path.join(agent_folder, "README.md")
    with open(readme_file, 'w', encoding='utf-8') as f:
        f.write(readme_content)
    print(f"📄 Created README file: {readme_file}")
    
    # Create the notebook file
    notebook_file = os.path.join(agent_folder, f"{folder_name}.ipynb")
    create_data_agent_notebook(agent_name, folder_name, notebook_file)
    print(f"📓 Created notebook file: {notebook_file}")
    
    # Register with global configuration system
    try:
        from config import config
        workspace = config.get_active_workspace()
        if workspace:
            config.add_data_agent(
                name=agent_name,
                notebook_path=config_data["notebook_path"]
            )
            print(f"🔧 Registered with global configuration system")
        else:
            print(f"⚠️ No active workspace - run 'python setup_config.py' to register with global config")
    except ImportError:
        print(f"⚠️ Global configuration system not available")
    except Exception as e:
        print(f"⚠️ Could not register with global config: {e}")
    
    print(f"\n✅ Data agent scaffold created successfully!")
    print(f"📂 Location: {agent_folder}")
    print(f"\n📋 Files created:")
    print(f"   📄 config.json - Basic configuration reference")
    print(f"   📓 {folder_name}.ipynb - Complete notebook with embedded config")
    print(f"   📋 README.md - Documentation")
    print(f"\n🚀 Next steps:")
    print(f"1. Open {folder_name}.ipynb and update the configuration cells")
    print(f"2. Customize lakehouse_name, table_names, instructions, and examples")
    print(f"3. Upload notebook to Fabric and run to create your data agent")
    print(f"4. Optional: Use 'python main.py {agent_name}' for automated upload")
    print(f"5. Optional: Use 'python test_run.py {agent_name}' for automated execution")


def create_data_agent_notebook(agent_name, folder_name, notebook_path):
    """Create a Jupyter notebook by copying and customizing the template."""
    
    # Get the template path
    template_path = os.path.join(os.path.dirname(__file__), "templates", "data_agent_template.ipynb")
    
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"Template file not found: {template_path}")
    
    # Read the template
    with open(template_path, 'r', encoding='utf-8') as f:
        template_content = f.read()
    
    # Replace specific values in the template for customization
    # Replace the generic data agent name with the specific one  
    customized_content = template_content.replace('test_data_agent1234', agent_name)
    
    # Write the customized notebook
    with open(notebook_path, 'w', encoding='utf-8') as f:
        f.write(customized_content)

def main():
    """Main entry point."""
    if len(sys.argv) != 2:
        print("Usage: python create_data_agent.py <agent_name>")
        print("Example: python create_data_agent.py my_sales_agent")
        sys.exit(1)
    
    agent_name = sys.argv[1]
    
    try:
        create_data_agent_scaffold(agent_name)
    except KeyboardInterrupt:
        print("\n❌ Cancelled by user")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()