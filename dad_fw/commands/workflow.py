"""
This file contains the key workflow commands for the typer cli.py file.
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

from dad_fw.core.framework_utils import FrameworkUtils

# Add parent directories to path
current_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(current_dir))

app = typer.Typer()

@app.command()
def init(
    name: str = typer.Argument(..., help="Name of the data agent to create"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing agent"),
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory (defaults to current directory)"),
):
    rprint(f"\n[bold blue]Creating Data Agent: {name}[/bold blue]")
    
    try:
        # Resolve base directory
        base_dir = project_dir.resolve() if project_dir else Path.cwd()
        
        # Validate workspace
        if not FrameworkUtils.validate_workspace(base_dir):
            rprint(f"[red]Invalid workspace directory: {base_dir}[/red]")
            sys.exit(1)
            
        agent = FrameworkUtils.create_agent(name, base_dir, force)
        
        rprint(f"[green]Created agent: {agent.name}[/green]")
        rprint(f"[green]Folder: {agent.folder_name}[/green]")
        rprint(f"[green]Location: {agent.agent_dir}[/green]")
        
        rprint(f"\n[cyan]Files created:[/cyan]")
        rprint(f"   {agent.get_config_file()}")
        rprint(f"   {agent.get_notebook_file()}") 
        rprint(f"   {agent.get_testing_file()}")
        rprint(f"   {agent.get_readme_file()}")
        
    except Exception as e:
        rprint(f"[red]Failed to create agent: {e}[/red]")
        sys.exit(1)
    


@app.command()
def compile(
    name: str = typer.Argument(..., help="Name of the data agent to compile"),
    custom_output_dir: Optional[Path] = typer.Option(None, "--custom-output-dir", "-d", help="Custom output directory for compiled .py file"),
    output_name: Optional[str] = typer.Option(None, "--output-name", "-n", help="Custom filename for the compiled .py file (without extension)"),
):
    """Compile the agent's notebook into Fabric Python format."""
    rprint(f"\n[bold blue]Compiling Data Agent: {name} into Fabric .py format[/bold blue]")
    try:
        # Find the agent in current working directory
        base_dir = Path.cwd()
        
        agent = FrameworkUtils.get_agent(name, base_dir)
        if not agent:
            rprint(f"[red]Agent '{name}' not found in {base_dir}[/red]")
            sys.exit(1)
        
        # Check if notebook exists
        if not agent.get_notebook_file().exists():
            rprint(f"[red]Notebook file not found: {agent.get_notebook_file()}[/red]")
            sys.exit(1)
        
        rprint(f"[cyan]Converting notebook: {agent.get_notebook_file()}[/cyan]")
        
        # Build custom output path if custom directory or name is provided
        output_file_path = None
        if custom_output_dir or output_name:
            # Determine output directory
            if custom_output_dir:
                output_dir = custom_output_dir.resolve()
                rprint(f"[cyan]Using custom output directory: {output_dir}[/cyan]")
            else:
                output_dir = agent.agent_dir
            
            # Determine filename
            if output_name:
                filename = f"{output_name}.py"
                rprint(f"[cyan]Using custom filename: {filename}[/cyan]")
            else:
                filename = f"{agent.folder_name}_fabric.py"
            
            output_file_path = str(output_dir / filename)
            agent.set_fabric_python_file(output_file_path)
            rprint(f"[cyan]Full output path: {output_file_path}[/cyan]")
        
        # Convert notebook to Fabric Python format
        result = agent.convert_ipynb_to_fabric_python(
            output_file_path=output_file_path
        )
        
        output_file = agent.get_fabric_python_file()
        rprint(f"[green]✓ Successfully compiled to: {output_file}[/green]")
        
        # Show file size
        file_size = output_file.stat().st_size
        rprint(f"[dim]File size: {file_size:,} bytes[/dim]")
        
    except Exception as e:
        rprint(f"[red]Failed to compile agent: {e}[/red]")
        sys.exit(1)

@app.command()
def upload(
    name: str = typer.Argument(..., help="Name of the data agent to upload"),
    workspace_id: Optional[str] = typer.Option(None, "--workspace-id", "-w", help="Fabric workspace ID (uses config if not provided)"),
    notebook_name: Optional[str] = typer.Option(None, "--custom-notebook-name", "-n", help="Display name for notebook in Fabric (defaults to agent name)"),
    use_ipynb: bool = typer.Option(False, "--use-ipynb", help="Upload raw .ipynb file instead of Fabric Python format"),
    update: bool = typer.Option(False, "--update", "-u", help="Force update existing notebook without asking"),
):
    """Upload the agent's notebook to Microsoft Fabric."""
    rprint(f"\n[bold blue]Uploading Data Agent: {name}[/bold blue]")
    try:
        base_dir = Path.cwd()
        
        agent = FrameworkUtils.get_agent(name, base_dir)
        if not agent:
            rprint(f"[red]Agent '{name}' not found in {base_dir}[/red]")
            sys.exit(1)
        agent.load_config()

        # Show upload details
        display_name = notebook_name if notebook_name else agent.name
        if workspace_id:
            rprint(f"[cyan]Using workspace ID: {workspace_id}[/cyan]")
        else:
            rprint("[cyan]Using workspace ID from agent config[/cyan]")
        
        rprint(f"[cyan]Notebook name: {display_name}[/cyan]")
        
        if use_ipynb:
            rprint("[cyan]Uploading as raw .ipynb file...[/cyan]")
        else:
            rprint("[cyan]Uploading as Fabric Python format...[/cyan]")
            # Check if auto-compile is needed
            fabric_file = agent.get_fabric_python_file()
            if not fabric_file.exists():
                rprint("[cyan]Auto-compiling notebook...[/cyan]")
        
        # Use the agent's upload method with update support
        result = agent.upload_to_fabric(
            workspace_id=workspace_id,
            notebook_name=notebook_name,
            use_ipynb=use_ipynb,
            force_update=update,
            ask_before_update=not update  # Don't ask if --update flag is used
        )
        
        # Show success message
        if result and result.get('updated'):
            rprint(f"[green]Successfully updated existing notebook in Fabric![/green]")
        else:
            rprint(f"[green]Successfully uploaded new notebook to Fabric![/green]")
        
        rprint(f"[green]Notebook Name: {display_name}[/green]")
        
        # Show additional details if available
        if result and isinstance(result, dict) and "id" in result:
            rprint(f"[dim]Notebook ID: {result['id']}[/dim]")

        rprint("[dim]Updated agent config with upload info[/dim]")
        
    except Exception as e:
        rprint(f"[red]Failed to upload agent: {e}[/red]")
        sys.exit(1)

@app.command()
def run(
    name: str = typer.Argument(..., help="Name of the data agent to run"),
    workspace_id: Optional[str] = typer.Option(None, "--workspace-id", "-w", help="Fabric workspace ID (uses config if not provided)"),
):
    """Execute the agent's notebook in Microsoft Fabric."""
    rprint(f"\n[bold blue]Running Data Agent: {name}[/bold blue]")
    try:
        base_dir = Path.cwd()
        
        agent = FrameworkUtils.get_agent(name, base_dir)
        if not agent:
            rprint(f"[red]Agent '{name}' not found in {base_dir}[/red]")
            sys.exit(1)
        
        # Load config to show execution details
        config = agent.load_config()
        
        # Show execution details
        if workspace_id:
            rprint(f"[cyan]Using workspace ID: {workspace_id}[/cyan]")
        else:
            rprint("[cyan]Using workspace ID from agent config[/cyan]")
        
        notebook_name = config.get("notebook_name")
        notebook_id = config.get("notebook_id")
        
        if notebook_name:
            rprint(f"[cyan]Notebook: {notebook_name}[/cyan]")
        elif notebook_id:
            rprint(f"[cyan]Notebook ID: {notebook_id}[/cyan]")
        else:
            rprint("[yellow]No notebook info found. Make sure the agent has been uploaded first.[/yellow]")
            rprint(f"[dim]Run: python -m dad.cli upload {name}[/dim]")
            sys.exit(1)
        
        rprint("[cyan]Starting execution...[/cyan]")
        
        # Use the agent's run method
        result = agent.run_in_fabric(workspace_id=workspace_id)
        
        # Show execution summary
        rprint(f"\n[bold]Execution Summary:[/bold]")
        rprint(f"[green]Status: {result['status']}[/green]" if result['success'] else f"[red]Status: {result['status']}[/red]")
        rprint(f"Runtime: {result['total_runtime_str']}")
        rprint(f"[dim]Job ID: {result['job_id']}[/dim]")
        
        # Show data agent info if found
        if result.get('agent_found'):
            rprint(f"\n[bold green]Data Agent Created:[/bold green]")
            rprint(f"Agent Name: {result['agent_display_name']}")
            rprint(f"Agent ID: {result['agent_id']}")
            rprint(f"[green]Agent URL: {result['agent_url']}[/green]")
        elif result['success']:
            rprint(f"\n[yellow]Data agent may have been created, but could not be found automatically.[/yellow]")
            rprint(f"[dim]Check the Fabric workspace for the new agent.[/dim]")
        
        if result['success']:
            rprint(f"\n[green]Data agent '{name}' executed successfully![/green]")
            rprint("[dim]Your data agent should now be available in Fabric[/dim]")
        else:
            rprint(f"\n[red]Execution failed for '{name}'[/red]")
            rprint("[dim]Check the notebook in Fabric workspace for error details[/dim]")
            sys.exit(1)
        
    except Exception as e:
        rprint(f"[red]Failed to run agent: {e}[/red]")
        sys.exit(1)

@app.command()
def list_cmd(
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory (defaults to current directory)"),
):
    """List all data agents in the project."""
    try:
        base_dir = project_dir.resolve() if project_dir else Path.cwd()
        
        if not FrameworkUtils.validate_workspace(base_dir):
            rprint(f"[red]Invalid workspace directory: {base_dir}[/red]")
            sys.exit(1)
            
        agents = FrameworkUtils.list_agents(base_dir)
        
        if not agents:
            rprint(f"[yellow]No agents found in {base_dir}[/yellow]")
            return
            
        rprint(f"\n[bold blue]Found {len(agents)} agent(s) in {base_dir}:[/bold blue]")
        for agent in agents:
            rprint(f"  • {agent.name} ({agent.folder_name})")
            rprint(f"    Config: {agent.get_config_file()}")
            rprint(f"    Notebook: {agent.get_notebook_file()}")
            
            # Show fabric python file if it exists
            fabric_file = agent.get_fabric_python_file()
            if fabric_file.exists():
                rprint(f"    Fabric Python: {fabric_file}")
            elif agent.has_fabric_python_file():
                rprint(f"    Fabric Python (set): {fabric_file}")
            rprint()
            
    except Exception as e:
        rprint(f"[red]Failed to list agents: {e}[/red]")
        sys.exit(1)