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
from ..debug.run_api import list_agents_in_workspace, get_azure_cli_token

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
    
    # Check if we have the necessary information
    if not workspace_id:
        rprint("[red]No workspace ID found in agent config[/red]")
        rprint("[dim]Add 'workspace_id' to agent config.json[/dim]")
        rprint("[dim]Example: {\"workspace_id\": \"your-workspace-id-here\"}[/dim]")
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
        
        # If successful, try to find the created data agent
        agent_id = None
        if result['success']:
            try:
                rprint("[cyan]Searching for created data agent...[/cyan]")
                
                # Get existing agent ID from config (backward compatibility)
                existing_agent_id = config.get('agent_id', '')
                
                # Handle backward compatibility - check for old data_agents array
                if not existing_agent_id and 'data_agents' in config and config['data_agents']:
                    existing_agent_id = config['data_agents'][0].get('id', '')
                
                # Handle backward compatibility - check for old test_url field
                elif not existing_agent_id and 'test_url' in config:
                    test_url = config['test_url']
                    if '/aiskills/' in test_url:
                        existing_agent_id = test_url.split('/aiskills/')[1].split('/')[0]
                
                # List all agents in workspace
                agents = list_agents_in_workspace(workspace_id)
                
                # Look for an agent with the exact name matching our command
                matching_agents = []
                for agent in agents:
                    agent_display_name = agent['displayName'].lower()
                    our_agent_name = agent_name.lower()
                    
                    # Check for exact match or close match (handle underscores/spaces)
                    normalized_agent_name = agent_display_name.replace(' ', '_').replace('-', '_')
                    normalized_our_name = our_agent_name.replace(' ', '_').replace('-', '_')
                    
                    if (normalized_agent_name == normalized_our_name or 
                        agent_display_name == our_agent_name or 
                        our_agent_name in agent_display_name):
                        matching_agents.append(agent)
                
                if matching_agents:
                    # Use the first matching agent (should typically be only one)
                    selected_agent = matching_agents[0]
                    agent_id = selected_agent['id']
                    agent_display_name = selected_agent['displayName']
                    
                    rprint(f"[green]Found data agent '{agent_display_name}' with ID: {agent_id}[/green]")
                    
                    if len(matching_agents) > 1:
                        rprint(f"[yellow]Warning: Found {len(matching_agents)} agents with similar names, using: {agent_display_name}[/yellow]")
                        for i, agent in enumerate(matching_agents[1:], 1):
                            rprint(f"[dim]  Alternative {i}: {agent['displayName']} (ID: {agent['id']})[/dim]")
                else:
                    rprint(f"[yellow]No data agent found with name '{agent_name}'. Make sure the notebook created the agent successfully.[/yellow]")
                    
            except Exception as e:
                rprint(f"[yellow]Could not search for created agent: {e}[/yellow]")
        
        # Update config with execution results
        try:
            config['last_execution'] = {
                'job_id': result['job_id'],
                'status': result['status'],
                'success': result['success'],
                'runtime': result['total_runtime_str'],
                'timestamp': datetime.now().isoformat()
            }
            
            # Add agent ID and URL if found
            if agent_id:
                config['agent_id'] = agent_id
                
                # Construct agent URL using workspace_id and agent_id
                workspace_id = config.get('workspace_id', '')
                if workspace_id:
                    config['agent_url'] = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/aiskills/{agent_id}/aiassistant/openai"
                    rprint(f"[green]Agent URL: {config['agent_url']}[/green]")
                else:
                    rprint("[yellow]Warning: workspace_id not found in config, cannot construct agent URL[/yellow]")
                
                rprint(f"[green]Recorded agent ID in config: {agent_id}[/green]")
            
            # Migrate from old structures to new agent_id field (if needed)
            migrated = False
            
            # Migration from old data_agents array
            if 'data_agents' in config and config['data_agents'] and not config.get('agent_id'):
                first_agent = config['data_agents'][0]
                if first_agent.get('id'):
                    config['agent_id'] = first_agent['id']
                    workspace_id = config.get('workspace_id', '')
                    if workspace_id:
                        config['agent_url'] = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/aiskills/{first_agent['id']}/aiassistant/openai"
                    rprint(f"[cyan]Migrated from data_agents array to agent_id: {first_agent['id']}[/cyan]")
                    migrated = True
            
            # Migration from old test_url field  
            elif 'test_url' in config and not config.get('agent_id'):
                test_url = config['test_url']
                if '/aiskills/' in test_url:
                    extracted_agent_id = test_url.split('/aiskills/')[1].split('/')[0]
                    config['agent_id'] = extracted_agent_id
                    config['agent_url'] = test_url
                    rprint(f"[cyan]Migrated from test_url to agent_id: {extracted_agent_id}[/cyan]")
                    migrated = True
            
            # Clean up old fields after migration
            old_fields_to_remove = []
            if 'data_agents' in config:
                old_fields_to_remove.append('data_agents')
            if 'test_url' in config:
                old_fields_to_remove.append('test_url')
            
            # Remove legacy bloated config fields
            legacy_fields = ['lakehouse_name', 'table_names', 'instructions', 'data_source_notes', 
                           'few_shot_examples', 'notebook_path', 'python_path']
            for field in legacy_fields:
                if field in config:
                    old_fields_to_remove.append(field)
            
            for field in old_fields_to_remove:
                del config[field]
                
            if old_fields_to_remove:
                rprint(f"[dim]Cleaned up legacy fields: {', '.join(old_fields_to_remove)}[/dim]")
            
            if migrated:
                rprint("[green]Config structure updated to new format[/green]")
            
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