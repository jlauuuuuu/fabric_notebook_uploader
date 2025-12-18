#!/usr/bin/env python3
"""
Data Agent Development Framework (DAD-FW) CLI - Object-Oriented Version
Main command-line interface for managing Fabric data agents.
"""

import typer
from rich.console import Console
from pathlib import Path
import sys

# Add parent directory to path for imports
current_dir = Path(__file__).parent.parent
sys.path.insert(0, str(current_dir))

from dad_fw.commands import workflow

app = typer.Typer(
    name="dad", 
    help="Data Agent Development Framework",
)

console = Console()

# Main workflow commands (easily accessible via direct command call instead of sub group command)
app.command("init", help="Initialize a new data agent")(workflow.init)
app.command("list", help="List all data agents")(workflow.list_cmd)
app.command("compile", help="Compile a data agent")(workflow.compile)
app.command("upload", help="Upload a data agent to Fabric")(workflow.upload)
app.command("download", help="Download a data agent from existing Fabric Notebook")(workflow.download)
app.command("run", help="Execute a data agent")(workflow.run)

# Additional CLI options
# app.add_typer(debug.app, name="debug", help="Debug commands")

if __name__ == "__main__":
    app()