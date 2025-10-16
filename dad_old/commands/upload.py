#!/usr/bin/env python3
"""
Upload command - Upload data agents to Fabric workspace
"""

import typer
from rich.console import Console
from rich import print as rprint
from typing import Optional
import os
import sys
import json
import base64
import requests
import subprocess
from pathlib import Path

# Add current directory to path for imports
current_dir = Path(__file__).parent.parent
sys.path.insert(0, str(current_dir))

from ..create_nb import create_notebook_from_fabric_python

app = typer.Typer()
console = Console()


def get_azure_cli_token():
    """Get access token using Azure CLI."""
    try:
        import subprocess
        cmd = ['az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com/', '--query', 'accessToken', '-o', 'tsv']
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, shell=True)
        return result.stdout.strip()
    except Exception as e:
        rprint(f"[yellow]Could not get Azure CLI token: {e}[/yellow]")
        return None


def update_existing_notebook(agent_folder_name: str, workspace_id: str, notebook_id: str, fabric_python_content: str):
    """Update existing notebook content using direct Fabric REST API."""
    try:
        rprint("[cyan]Updating existing notebook...[/cyan]")
        
        # Get access token
        access_token = get_azure_cli_token()
        if not access_token:
            rprint("[red]Could not get access token[/red]")
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
        
        if response.status_code == 200:
            rprint("[green]Update completed[/green]")
            return True
        elif response.status_code == 202:
            rprint("[green]Update accepted, processing in background[/green]")
            return True
        else:
            rprint(f"[red]Update failed: {response.status_code}[/red]")
            return False
        
    except Exception as e:
        rprint(f"[red]Error updating notebook: {e}[/red]")
        return False


def upload_data_agent(agent_folder_name: str, workspace_id: Optional[str] = None, force_update: bool = False):
    """Upload a compiled data agent to Fabric workspace as a notebook."""
    
    rprint(f"[bold]Uploading: {agent_folder_name}[/bold]")
    
    # Get the agent folder path
    agent_folder = Path(agent_folder_name)
    
    if not agent_folder.exists():
        rprint(f"[red]Agent folder '{agent_folder_name}' not found[/red]")
        return False
    
    # Find the compiled Fabric Python file
    fabric_python_file = agent_folder / f"{agent_folder_name}_fabric.py"
    
    if not fabric_python_file.exists():
        rprint(f"[red]Compiled file not found: {agent_folder_name}_fabric.py[/red]")
        rprint(f"[yellow]Run 'dad-fw compile {agent_folder_name}' first[/yellow]")
        return False
    
    # Load configuration - workspace ID is required from config
    config_file = agent_folder / "config.json"
    agent_name = agent_folder_name
    config = {}
    workspace_id = None
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Get agent name from config if available
            if config.get('agent_name'):
                agent_name = config['agent_name']
            
            # Get workspace ID from config (required)
            workspace_id = config.get('workspace_id')
            if workspace_id:
                rprint(f"[dim]Using workspace: {workspace_id}[/dim]")
                
        except Exception as e:
            rprint(f"[yellow]Could not load config: {e}[/yellow]")
            config = {}
    
    # Try to load workspace ID from global config if not in agent config
    if not workspace_id:
        try:
            global_config_file = Path("config.json")
            if global_config_file.exists():
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
                            rprint("[dim]Using workspace from global config[/dim]")
        except Exception as e:
            pass
    
    # Check if workspace ID is available
    if not workspace_id:
        rprint("[red]No workspace ID found[/red]")
        rprint("[dim]Add 'workspace_id' to agent config.json or global config[/dim]")
        return False
    
    # Read the compiled Fabric Python content
    try:
        with open(fabric_python_file, 'r', encoding='utf-8') as f:
            fabric_python_content = f.read()
        
    except Exception as e:
        rprint(f"[red]Error reading compiled file: {e}[/red]")
        return False
    
    # Create notebook name following the pattern: {agent_name}_creation_notebook
    notebook_name = f"{agent_name}_creation_notebook"
    
    # Check if notebook already exists and should be updated
    existing_notebook_id = config.get('notebook_id') if config else None
    
    try:
        notebook_id = None
        display_name = notebook_name
        
        if existing_notebook_id and (force_update or typer.confirm(f"Notebook exists. Update existing notebook?")):
            success = update_existing_notebook(
                agent_folder_name=agent_folder_name,
                workspace_id=workspace_id,
                notebook_id=existing_notebook_id,
                fabric_python_content=fabric_python_content
            )
            
            if success:
                notebook_id = existing_notebook_id
                display_name = notebook_name
            else:
                rprint("[yellow]Update failed, creating new notebook[/yellow]")
                existing_notebook_id = None
        
        if not existing_notebook_id:
            rprint("[cyan]Creating notebook...[/cyan]")
            # Upload to Fabric
            notebook = create_notebook_from_fabric_python(
                workspace_id=workspace_id,
                fabric_python_content=fabric_python_content,
                notebook_name=notebook_name
            )
            
            rprint("[green]Upload completed[/green]")
            
            # Access notebook properties
            notebook_id = notebook.id if hasattr(notebook, 'id') else "Unknown"
            display_name = getattr(notebook, 'display_name', notebook_name)
        
        if notebook_id:
            rprint(f"[green]Notebook ID: {notebook_id}[/green]")
        else:
            rprint("[yellow]Upload cancelled[/yellow]")
            return True
        
        # Update config with notebook information if config exists and upload happened
        if notebook_id and config_file.exists():
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
                
                rprint("[dim]Updated config.json[/dim]")
                
            except Exception as e:
                rprint(f"[yellow]Could not update config: {e}[/yellow]")
        
        return True
        
    except Exception as e:
        rprint(f"[red]Upload failed: {e}[/red]")
        return False


@app.command()
def agent(
    name: str = typer.Argument(..., help="Name of the data agent to upload"),
    update: bool = typer.Option(False, "--update", "-u", help="Update existing notebook instead of creating new"),
):
    """Upload a data agent to Fabric workspace"""
    
    success = upload_data_agent(name, None, update)
    
    if success:
        rprint(f"[dim]Next: dad-fw run {name}[/dim]")
    else:
        raise typer.Exit(1)


if __name__ == "__main__":
    app()