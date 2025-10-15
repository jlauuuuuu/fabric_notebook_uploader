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

from dad.core.framework_utils import FrameworkUtils

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
            raise typer.Exit(1)
            
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
        raise typer.Exit(1)
    


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
            raise typer.Exit(1)
        
        # Check if notebook exists
        if not agent.get_notebook_file().exists():
            rprint(f"[red]Notebook file not found: {agent.get_notebook_file()}[/red]")
            raise typer.Exit(1)
        
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
        raise typer.Exit(1)

def upload(
    name: str = typer.Argument(..., help="Name of the data agent to upload"),
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory (defaults to current directory)"),
):
    rprint(f"\n[bold blue]Uploading Data Agent: {name}[/bold blue]")
    try:
        base_dir = project_dir.resolve() if project_dir else Path.cwd()
        
        agent = FrameworkUtils.get_agent(name, base_dir)
        if not agent:
            rprint(f"[red]Agent '{name}' not found in {base_dir}[/red]")
            raise typer.Exit(1)
            
        rprint(f"[yellow]Upload functionality coming soon...[/yellow]")
        
    except Exception as e:
        rprint(f"[red]Failed to upload agent: {e}[/red]")
        raise typer.Exit(1)

def run(
    name: str = typer.Argument(..., help="Name of the data agent to run"),
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory (defaults to current directory)"),
):
    rprint(f"\n[bold blue]Running Data Agent: {name}[/bold blue]")
    try:
        base_dir = project_dir.resolve() if project_dir else Path.cwd()
        
        agent = FrameworkUtils.get_agent(name, base_dir)
        if not agent:
            rprint(f"[red]Agent '{name}' not found in {base_dir}[/red]")
            raise typer.Exit(1)
            
        rprint(f"[yellow]Run functionality coming soon...[/yellow]")
        
    except Exception as e:
        rprint(f"[red]Failed to run agent: {e}[/red]")
        raise typer.Exit(1)

@app.command()
def list_cmd(
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory (defaults to current directory)"),
):
    """List all data agents in the project."""
    try:
        base_dir = project_dir.resolve() if project_dir else Path.cwd()
        
        if not FrameworkUtils.validate_workspace(base_dir):
            rprint(f"[red]Invalid workspace directory: {base_dir}[/red]")
            raise typer.Exit(1)
            
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
        raise typer.Exit(1)


