#!/usr/bin/env python3
"""
Create command - Generate new data agents and templates
"""

import typer
from rich.console import Console
from rich import print as rprint
from typing import Optional
import os
import sys
import json
import shutil
from pathlib import Path
from datetime import datetime

# Add parent directories to path
current_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(current_dir))

app = typer.Typer()
console = Console()


def create_data_agent_notebook(agent_name, folder_name, notebook_path):
    """Create a Jupyter notebook by copying and customizing the template."""
    
    # Get the template path
    template_path = Path(__file__).parent.parent / "templates" / "data_agent_template.ipynb"
    
    if not template_path.exists():
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


@app.command()
def agent(
    name: str = typer.Argument(..., help="Name of the data agent to create"),
    description: str = typer.Option("", "--description", "-d", help="Description of the data agent"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing agent if it exists"),
):
    """Create a new data agent"""
    
    rprint(f"\n[bold blue]Creating Data Agent: {name}[/bold blue]")
    
    # Clean the name for folder usage
    folder_name = name.replace(" ", "_").replace("-", "_").lower()
    rprint(f"Agent name: {name}")
    rprint(f"Folder name: {folder_name}")
    
    # Create folder structure
    agent_folder = Path(folder_name)
    
    if agent_folder.exists() and not force:
        rprint(f"[red]ERROR: Folder '{folder_name}' already exists[/red]")
        if not typer.confirm("Overwrite existing folder?"):
            rprint("Cancelled")
            raise typer.Exit(1)
    
    # Create the folder
    agent_folder.mkdir(exist_ok=True)
    rprint(f"[green]Created folder: {agent_folder}[/green]")
    
    # Create config.json with same structure as existing config system
    notebook_filename = f"{folder_name}.ipynb"
    python_filename = f"{folder_name}.py"
    
    config_data = {
        "agent_name": name,
        "folder_name": folder_name,
        "created_date": datetime.now().isoformat(),
        "lakehouse_name": "",
        "table_names": [],
        "instructions": "",
        "data_source_notes": "",
        "few_shot_examples": {},
        "status": "scaffolded",
        "notebook_path": str(agent_folder / notebook_filename),
        "python_path": str(agent_folder / python_filename),
        "notebook_id": "",
        "test_url": ""
    }
    
    config_file = agent_folder / "config.json"
    with open(config_file, 'w', encoding='utf-8') as f:
        json.dump(config_data, f, indent=2, ensure_ascii=False)
    rprint(f"[green]� Created config file: {config_file}[/green]")
    
    # Create a README for the folder
    readme_content = f"""# Data Agent: {name}

Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Folder: {folder_name}

## Files

- `config.json` - Basic configuration (optional for reference)
- `{folder_name}.ipynb` - Complete data agent notebook with embedded configuration
- `{folder_name}_testing.ipynb` - Testing notebook for deployed agent
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

For use with DAD-FW workflow:
```bash
dad-fw compile {name}      # Prepare for upload
dad-fw upload {name}       # Upload notebook to Fabric
dad-fw run {name}          # Execute in Fabric
```

## Testing

After your agent is deployed:
1. Update the config in `{folder_name}_testing.ipynb` with your agent URL
2. Open the testing notebook and run the cells to test your agent
"""
    
    readme_file = agent_folder / "README.md"
    with open(readme_file, 'w', encoding='utf-8') as f:
        f.write(readme_content)
    rprint(f"[green]📄 Created README file: {readme_file}[/green]")
    
    # Create the data agent notebook file
    notebook_file = agent_folder / f"{folder_name}.ipynb"
    try:
        create_data_agent_notebook(name, folder_name, notebook_file)
        rprint(f"[green]Created data agent notebook: {notebook_file}[/green]")
    except Exception as e:
        rprint(f"[yellow]Could not create data agent notebook: {e}[/yellow]")
    
    # Copy testing notebook template
    try:
        template_dir = Path(__file__).parent.parent / "templates"
        testing_template = template_dir / "testing_template.ipynb"
        
        if testing_template.exists():
            testing_notebook = agent_folder / f"{folder_name}_testing.ipynb"
            shutil.copy2(testing_template, testing_notebook)
            rprint(f"[green]Created testing notebook: {testing_notebook}[/green]")
        else:
            rprint(f"[yellow]Testing template not found: {testing_template}[/yellow]")
            
    except Exception as e:
        rprint(f"[yellow]Could not create testing notebook: {e}[/yellow]")
    
    rprint(f"\n[green]Data agent scaffold created successfully![/green]")
    rprint(f"[dim]Location: {agent_folder}[/dim]")
    rprint(f"\n[cyan]Files created:[/cyan]")
    rprint(f"   config.json - Basic configuration reference")
    rprint(f"   {folder_name}.ipynb - Complete notebook with embedded config")
    rprint(f"   {folder_name}_testing.ipynb - Testing notebook for deployed agent")
    rprint(f"   README.md - Documentation")
    rprint(f"\n[bold]Next steps:[/bold]")
    rprint(f"1. Open {folder_name}.ipynb and update the configuration cells")
    rprint(f"2. Customize lakehouse_name, table_names, instructions, and examples")
    rprint(f"3. Upload notebook to Fabric and run to create your data agent")
    rprint(f"4. Use dad-fw compile/upload/run commands for automated workflow")
    rprint(f"5. Test with {folder_name}_testing.ipynb after deployment")


@app.command()
def template(
    name: str = typer.Argument(..., help="Name of the template to create"),
    base: str = typer.Option("basic", "--base", "-b", help="Base template to copy from"),
):
    """📄 Create a new agent template"""
    
    rprint(f"\n[bold blue]📄 Creating Template: {name}[/bold blue]")
    
    template_dir = Path("templates") / name
    
    if template_dir.exists():
        rprint(f"[red]Template '{name}' already exists[/red]")
        raise typer.Exit(1)
    
    try:
        # Create template directory structure
        template_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy from base template if it exists
        base_template = Path("templates") / base
        if base_template.exists():
            import shutil
            shutil.copytree(base_template, template_dir, dirs_exist_ok=True)
            rprint(f"[green]✅ Template created based on '{base}'[/green]")
        else:
            # Create basic template structure
            (template_dir / "config.json").write_text('{\n  "template_name": "' + name + '",\n  "description": "Custom template"\n}')
            (template_dir / f"{name}_fabric.py").write_text(f"# {name} Data Agent Template\n\n# Add your agent code here\n")
            rprint(f"[green]✅ Basic template structure created[/green]")
        
        rprint(f"[dim]Location: ./templates/{name}/[/dim]")
        
    except Exception as e:
        rprint(f"[red]Error creating template: {e}[/red]")
        raise typer.Exit(1)


@app.command("list")
def list_templates():
    """📋 List available templates"""
    
    rprint("\n[bold blue]📋 Available Templates[/bold blue]\n")
    
    templates_dir = Path("templates")
    if not templates_dir.exists():
        rprint("[yellow]No templates directory found[/yellow]")
        return
    
    templates = [d for d in templates_dir.iterdir() if d.is_dir()]
    
    if not templates:
        rprint("[yellow]No templates found[/yellow]")
        return
    
    for template in templates:
        config_file = template / "config.json"
        description = "No description"
        
        if config_file.exists():
            try:
                import json
                config = json.loads(config_file.read_text())
                description = config.get("description", "No description")
            except:
                pass
        
        rprint(f"[cyan]📄 {template.name}[/cyan] - {description}")


if __name__ == "__main__":
    app()