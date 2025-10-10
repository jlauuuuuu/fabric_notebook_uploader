#!/usr/bin/env python3
"""
Debug commands - Advanced debugging and API functionality
"""

import typer
from rich.console import Console
from rich import print as rprint
from typing import Optional
import os
import sys
import subprocess
from pathlib import Path

app = typer.Typer()
console = Console()


@app.command("run-api")
def run_api(
    name: str = typer.Argument(..., help="Name of the data agent to run"),
):
    """Execute a data agent using direct REST API (debug mode)"""
    
    rprint(f"[bold]Debug: Running {name} via API[/bold]")
    
    try:
        debug_script = Path(__file__).parent.parent / "debug" / "run_api.py"
        cmd = [sys.executable, str(debug_script), name]
        
        rprint(f"[dim]Running: {' '.join(cmd)}[/dim]")
        
        # Run the debug API version
        result = subprocess.run(cmd, text=True)
        
        if result.returncode == 0:
            rprint("[green]API execution completed[/green]")
        else:
            rprint("[red]API execution failed[/red]")
            raise typer.Exit(1)
            
    except Exception as e:
        rprint(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command("notebook")
def run_notebook(
    notebook_name: Optional[str] = typer.Option(None, "--name", "-n", help="Notebook name to run"),
    notebook_id: Optional[str] = typer.Option(None, "--id", "-i", help="Notebook ID to run"),
    workspace_id: str = typer.Option(..., "--workspace-id", "-w", help="Fabric workspace ID"),
):
    """Execute a notebook directly by name or ID (debug mode)"""
    
    if not notebook_name and not notebook_id:
        rprint("[red]Either --name or --id must be provided[/red]")
        raise typer.Exit(1)
    
    if notebook_name and notebook_id:
        rprint("[red]Provide either --name or --id, not both[/red]")
        raise typer.Exit(1)
    
    identifier = notebook_name or notebook_id
    rprint(f"[bold]Debug: Running notebook {identifier}[/bold]")
    
    try:
        debug_script = Path(__file__).parent.parent / "debug" / "run_api.py"
        cmd = [sys.executable, str(debug_script), workspace_id]
        
        if notebook_name:
            cmd.extend(["--name", notebook_name])
        else:
            cmd.extend(["--id", notebook_id])
        
        rprint(f"[dim]Running: {' '.join(cmd)}[/dim]")
        
        # Execute
        result = subprocess.run(cmd, text=True)
        
        if result.returncode == 0:
            rprint("[green]Notebook execution completed[/green]")
        else:
            rprint("[red]Notebook execution failed[/red]")
            raise typer.Exit(1)
            
    except Exception as e:
        rprint(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command("list")
def list_notebooks(
    workspace_id: str = typer.Option(..., "--workspace-id", "-w", help="Fabric workspace ID"),
):
    """List all notebooks in a Fabric workspace (debug mode)"""
    
    rprint(f"[bold]Debug: Listing notebooks in workspace[/bold]")
    
    try:
        debug_script = Path(__file__).parent.parent / "debug" / "run_api.py"
        cmd = [sys.executable, str(debug_script), workspace_id, "--list"]
        
        result = subprocess.run(cmd, text=True)
        
        if result.returncode != 0:
            rprint("[red]Failed to list notebooks[/red]")
            raise typer.Exit(1)
            
    except Exception as e:
        rprint(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()