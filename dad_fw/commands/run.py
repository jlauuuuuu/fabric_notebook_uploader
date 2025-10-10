#!/usr/bin/env python3
"""
Run command - Execute data agents in Fabric
"""

import typer
from rich.console import Console
from rich import print as rprint
from typing import Optional
import os
import sys
import json
from pathlib import Path
from datetime import datetime

# Add current directory to path for imports
current_dir = Path(__file__).parent.parent
sys.path.insert(0, str(current_dir))

from ..run_nb import run_notebook_by_name, run_notebook_by_id

app = typer.Typer()
console = Console()


def run_fabric_notebook(agent_folder_name: str):
    """Run a data agent notebook in Fabric workspace."""
    
    rprint(f"[bold]Running: {agent_folder_name}[/bold]")
    
    # Get the agent folder path
    agent_folder = Path(agent_folder_name)
    
    if not agent_folder.exists():
        rprint(f"[red]Agent folder '{agent_folder_name}' not found[/red]")
        return False
    
    # Load configuration
    config_file = agent_folder / "config.json"
    
    if not config_file.exists():
        rprint(f"[red]Config file not found in '{agent_folder_name}' folder[/red]")
        return False
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        agent_name = config.get('agent_name', agent_folder_name)
        notebook_id = config.get('notebook_id')
        notebook_name = config.get('notebook_name')
        workspace_id = config.get('workspace_id')
        
        rprint(f"[dim]Agent: {agent_name}[/dim]")
        rprint(f"[dim]Notebook: {notebook_name}[/dim]")
        
    except Exception as e:
        rprint(f"[red]Error loading config: {e}[/red]")
        return False
    
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
        except Exception as e:
            pass
    
    # Check if we have the necessary information
    if not workspace_id:
        rprint("[red]No workspace ID found[/red]")
        rprint("[dim]Add 'workspace_id' to agent config.json or global config[/dim]")
        return False
    
    if not notebook_id and not notebook_name:
        rprint("[red]No notebook ID or name found in config[/red]")
        rprint("[yellow]Make sure the notebook has been uploaded first[/yellow]")
        rprint(f"[dim]Run: dad-fw upload {agent_folder_name}[/dim]")
        return False
    
    try:
        rprint("[cyan]Starting execution...[/cyan]")
        
        # Run the notebook (prefer by name if available, fallback to ID)
        if notebook_name:
            rprint(f"[dim]Running by name: {notebook_name}[/dim]")
            result = run_notebook_by_name(
                workspace_id=workspace_id,
                notebook_name=notebook_name
            )
        elif notebook_id:
            rprint(f"[dim]Running by ID: {notebook_id}[/dim]")
            result = run_notebook_by_id(
                workspace_id=workspace_id,
                notebook_id=notebook_id
            )
        
        # Display execution summary
        rprint(f"\n[bold]Execution Summary[/bold]")
        rprint(f"Status: {result['status']}")
        rprint(f"Success: {result['success']}")
        rprint(f"Runtime: {result['total_runtime_str']}")
        rprint(f"Job ID: {result['job_id']}")
        
        # Update config with execution results
        try:
            config['last_execution'] = {
                'job_id': result['job_id'],
                'status': result['status'],
                'success': result['success'],
                'runtime': result['total_runtime_str'],
                'timestamp': datetime.now().isoformat()
            }
            
            # Update overall status
            config['status'] = 'executed_successfully' if result['success'] else 'execution_failed'
            
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            rprint("[dim]Updated config.json[/dim]")
            
        except Exception as e:
            rprint(f"[yellow]Could not update config: {e}[/yellow]")
        
        if result['success']:
            rprint(f"[green]Data agent '{agent_name}' executed successfully[/green]")
            rprint("[dim]Your data agent should now be created in Fabric[/dim]")
        else:
            rprint(f"[red]Execution failed for '{agent_name}'[/red]")
            rprint("[dim]Check the notebook in Fabric workspace for error details[/dim]")
        
        return result['success']
        
    except Exception as e:
        rprint(f"[red]Error during execution: {e}[/red]")
        return False


@app.command()
def agent(
    name: str = typer.Argument(..., help="Name of the data agent to run"),
):
    """Execute a data agent in Fabric workspace"""
    
    success = run_fabric_notebook(name)
    
    if not success:
        raise typer.Exit(1)


if __name__ == "__main__":
    app()