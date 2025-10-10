"""
Core utilities for DAD-FW CLI
"""

import os
import sys
import json
import subprocess
from pathlib import Path
from typing import Optional, Dict, Any


def get_workspace_root() -> Path:
    """Get the workspace root directory."""
    return Path.cwd()


def load_config() -> Dict[str, Any]:
    """Load configuration from config.json."""
    config_file = get_workspace_root() / "config.json"
    
    if not config_file.exists():
        return {}
    
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def save_config(config: Dict[str, Any]) -> None:
    """Save configuration to config.json."""
    config_file = get_workspace_root() / "config.json"
    
    try:
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
    except IOError as e:
        raise Exception(f"Failed to save configuration: {e}")


def get_default_workspace_id() -> Optional[str]:
    """Get the default workspace ID from configuration."""
    config = load_config()
    return config.get("workspace_id")


def find_script_path(script_name: str) -> Path:
    """Find a script in the workspace root."""
    workspace_root = get_workspace_root()
    script_path = workspace_root / script_name
    
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_name}")
    
    return script_path


def run_script(script_name: str, args: list, capture_output: bool = False, verbose: bool = False) -> subprocess.CompletedProcess:
    """Run a Python script with the given arguments."""
    script_path = find_script_path(script_name)
    cmd = [sys.executable, str(script_path)] + args
    
    if verbose:
        print(f"Running: {' '.join(cmd)}")
    
    return subprocess.run(
        cmd,
        capture_output=capture_output,
        text=True,
        cwd=get_workspace_root()
    )


def check_agent_exists(agent_name: str) -> bool:
    """Check if an agent directory exists."""
    agent_path = get_workspace_root() / agent_name
    return agent_path.exists() and agent_path.is_dir()


def check_agent_compiled(agent_name: str) -> bool:
    """Check if an agent has been compiled."""
    compiled_file = get_workspace_root() / agent_name / f"{agent_name}_fabric.py"
    return compiled_file.exists()


def get_agent_list() -> list:
    """Get a list of all agent directories."""
    workspace_root = get_workspace_root()
    agents = []
    
    for item in workspace_root.iterdir():
        if item.is_dir() and not item.name.startswith('.') and item.name not in ['dad_fw', '__pycache__', '.venv', '.git']:
            # Check if it contains agent files
            config_file = item / "config.json"
            fabric_file = list(item.glob("*_fabric.py"))
            
            if config_file.exists() or fabric_file:
                agents.append(item.name)
    
    return agents


def validate_azure_cli() -> bool:
    """Check if Azure CLI is available and authenticated."""
    try:
        result = subprocess.run(
            ["az", "account", "show"],
            capture_output=True,
            text=True,
            shell=True
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


def get_templates_dir() -> Path:
    """Get the templates directory path."""
    package_dir = Path(__file__).parent
    return package_dir / "templates"