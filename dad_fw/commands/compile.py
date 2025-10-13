#!/usr/bin/env python3
"""
Compile command - Compile data agents to Fabric format
"""

import subprocess
import typer
from rich.console import Console
from rich import print as rprint
from typing import Optional
import os
import sys
import json
from pathlib import Path

# Add current directory to path for imports
current_dir = Path(__file__).parent.parent
sys.path.insert(0, str(current_dir))

from ..convert_nb import convert_ipynb_to_fabric_python

app = typer.Typer()
console = Console()


def compile_data_agent(agent_folder_name: str, verbose: bool = False):
    """
    Compile a data agent notebook to Fabric Python format.
    
    Args:
        agent_folder_name: Name of the data agent folder
        verbose: Show detailed output
    """
    rprint(f"[bold blue]Compiling Data Agent: {agent_folder_name}[/bold blue]")
    
    # Get the agent folder path
    agent_folder = Path(agent_folder_name)
    
    if not agent_folder.exists():
        rprint(f"[red]Agent folder '{agent_folder_name}' not found[/red]")
        return False
    
    # Find the notebook file
    notebook_file = None
    expected_notebook = agent_folder / f"{agent_folder_name}.ipynb"
    
    if expected_notebook.exists():
        notebook_file = expected_notebook
    else:
        # Look for any .ipynb file in the folder
        ipynb_files = list(agent_folder.glob("*.ipynb"))
        if ipynb_files:
            notebook_file = ipynb_files[0]
        else:
            rprint(f"[red]No notebook file found in '{agent_folder_name}' folder[/red]")
            return False
    
    rprint(f"[green]Found notebook: {notebook_file.name}[/green]")
    
    # Define output file path
    output_file = agent_folder / f"{agent_folder_name}_fabric.py"
    
    # Load configuration if available
    config_file = agent_folder / "config.json"
    workspace_id = None
    lakehouse_id = None
    lakehouse_name = None
    include_metadata = False
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Extract lakehouse info if available
            lakehouse_name = config.get('lakehouse_name')
            workspace_id = config.get('workspace_id')
            lakehouse_id = config.get('lakehouse_id')
            
            if lakehouse_name and workspace_id and lakehouse_id:
                include_metadata = True
                rprint(f"[cyan]Using lakehouse metadata: {lakehouse_name}[/cyan]")
            elif lakehouse_name:
                rprint(f"[cyan]Lakehouse name found: {lakehouse_name} (no IDs for metadata)[/cyan]")
            else:
                rprint("[dim]No lakehouse configuration found[/dim]")
                
        except Exception as e:
            rprint(f"[yellow]Could not load config: {e}[/yellow]")
    
    try:
        # Convert the notebook to Fabric Python format
        rprint("[cyan]Converting notebook to Fabric Python format...[/cyan]")
        
        result = convert_ipynb_to_fabric_python(
            ipynb_file_path=str(notebook_file),
            output_file_path=str(output_file),
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            lakehouse_name=lakehouse_name,
            include_lakehouse_metadata=include_metadata
        )
        
        rprint(f"[green]Successfully compiled to: {output_file.name}[/green]")
        rprint(f"[dim]Output location: {output_file}[/dim]")
        
        # Show file size
        file_size = output_file.stat().st_size
        rprint(f"[dim]File size: {file_size:,} bytes[/dim]")
        
        # Show preview if verbose
        if verbose:
            rprint(f"\n[bold]Preview (first 10 lines):[/bold]")
            with open(output_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                for i, line in enumerate(lines[:10]):
                    console.print(f"{i+1:2d}: {line.rstrip()}", style="dim")
                if len(lines) > 10:
                    rprint(f"[dim]... ({len(lines) - 10} more lines)[/dim]")
        
        return True
        
    except Exception as e:
        rprint(f"[red]Error during compilation: {e}[/red]")
        if verbose:
            import traceback
            console.print_exception()
        return False


@app.command()
def agent(
    name: str = typer.Argument(..., help="Name of the data agent to compile"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed compilation output"),
):
    """Compile a data agent to Fabric notebook format"""
    
    success = compile_data_agent(name, verbose)
    
    if success:
        rprint(f"\n[bold]Next steps:[/bold]")
        rprint(f"[dim]dad-fw upload {name}[/dim]         # Upload to Fabric workspace")
        rprint(f"[dim]dad-fw run {name}[/dim]            # Execute in Fabric workspace")
    else:
        raise typer.Exit(1)


@app.command("all")
def compile_all(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed compilation output"),
):
    """🔨 Compile all data agents in the current directory"""
    
    rprint(f"\n[bold blue]🔨 Compiling All Data Agents[/bold blue]")
    
    # Find all agent directories (directories with agent files)
    agents = []
    for item in os.listdir("."):
        if os.path.isdir(item) and not item.startswith("."):
            # Check if it contains agent files
            agent_files = [f for f in os.listdir(item) if f.endswith("_fabric.py") or f == "config.json"]
            if agent_files:
                agents.append(item)
    
    if not agents:
        rprint("[yellow]No data agents found to compile[/yellow]")
        return
    
    rprint(f"[cyan]Found {len(agents)} agents to compile[/cyan]")
    
    compiled = 0
    failed = 0
    
    for agent in agents:
        try:
            rprint(f"\n[dim]Compiling {agent}...[/dim]")
            
            cmd = [sys.executable, "compile_script.py", agent]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                rprint(f"[green]{agent}[/green]")
                compiled += 1
            else:
                rprint(f"[red]{agent}[/red]")
                if verbose and result.stderr:
                    rprint(f"[red]{result.stderr}[/red]")
                failed += 1
                
        except Exception as e:
            rprint(f"[red]{agent}: {e}[/red]")
            failed += 1
    
    rprint(f"\n[bold]Compilation Summary:[/bold]")
    rprint(f"[green]Compiled: {compiled}[/green]")
    if failed > 0:
        rprint(f"[red]Failed: {failed}[/red]")


if __name__ == "__main__":
    app()