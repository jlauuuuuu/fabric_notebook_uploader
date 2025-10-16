#!/usr/bin/env python3
"""
Data Agent Development Framework (DAD-FW) CLI
Main command-line interface for managing Fabric data agents.
"""

import typer
from rich.console import Console
from rich.table import Table
from rich import print as rprint
from typing import Optional
import os
import sys
from pathlib import Path

# Add the current directory to Python path for imports
current_dir = Path(__file__).parent.parent
sys.path.insert(0, str(current_dir))

from dad_fw.commands import create, compile, upload, run, debug

app = typer.Typer(
    name="dad-fw",
    help="Data Agent Development Framework",
)

console = Console()

# Add direct commands
app.command("init", help="Initialize a new data agent")(create.agent)
app.command("compile", help="Compile a data agent")(compile.agent)
app.command("upload", help="Upload a data agent to Fabric")(upload.agent)
app.command("run", help="Execute a data agent")(run.agent)
app.add_typer(debug.app, name="debug", help="Debug commands")


if __name__ == "__main__":
    app()