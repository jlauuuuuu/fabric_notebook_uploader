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
sys.path.insert(0, str(Path(__file__).parent.parent))

from dad_fw.commands import workflow

app = typer.Typer(
    name="dad-fw-simple", 
    help="Simple Data Agent Development Framework",
)

console = Console()

# Add commands directly
app.command("create", help="Create a new data agent")(workflow.create)
app.command("list", help="List existing data agents")(workflow.list_agents)

if __name__ == "__main__":
    app()