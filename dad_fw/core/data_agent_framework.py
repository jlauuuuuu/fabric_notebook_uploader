"""
Strictly stateless FrameworkUtils for robust CLI and pipeline usage.
Each method is self-contained with no persistent state.
"""
from pathlib import Path
from typing import List, Optional

from .data_agent import DataAgent
from .file import FileManager


class FrameworkUtils:
    """Stateless utilities for managing data agents."""
    
    @staticmethod
    def create_agent(name: str, base_dir: Path, force: bool = False) -> DataAgent:
        """Create a new data agent in the specified directory."""
        agent = DataAgent(name, base_dir)
        agent.create(force=force)
        return agent
    
    @staticmethod
    def get_agent(name: str, base_dir: Path) -> Optional[DataAgent]:
        """Get an existing agent from the specified directory."""
        agent = DataAgent(name, base_dir)
        return agent if agent.exists() else None
    
    @staticmethod
    def list_agents(base_dir: Path) -> List[DataAgent]:
        """List all existing agents in the specified directory."""
        if not base_dir.exists() or not base_dir.is_dir():
            return []
        
        agents = []
        for item in base_dir.iterdir():
            if item.is_dir() and (item / "config.json").exists():
                # Try to create agent from folder name
                agent_name = item.name.replace("_", " ").title()
                agent = DataAgent(agent_name, base_dir)
                if agent.exists():  # Double-check it's valid
                    agents.append(agent)
        return agents
    
    @staticmethod
    def validate_workspace(base_dir: Path) -> bool:
        """Validate that the directory is a valid workspace for agents."""
        return base_dir.exists() and base_dir.is_dir()
    
    @staticmethod
    def agent_exists(name: str, base_dir: Path) -> bool:
        """Check if an agent exists in the specified directory."""
        agent = DataAgent(name, base_dir)
        return agent.exists()


# Convenience functions for direct use without instantiating the class
def create_agent(name: str, base_dir: Path, force: bool = False) -> DataAgent:
    """Create a new data agent."""
    return FrameworkUtils.create_agent(name, base_dir, force)

def get_agent(name: str, base_dir: Path) -> Optional[DataAgent]:
    """Get an existing agent."""
    return FrameworkUtils.get_agent(name, base_dir)

def list_agents(base_dir: Path) -> List[DataAgent]:
    """List all agents in directory."""
    return FrameworkUtils.list_agents(base_dir)

def validate_workspace(base_dir: Path) -> bool:
    """Validate workspace directory."""
    return FrameworkUtils.validate_workspace(base_dir)

def agent_exists(name: str, base_dir: Path) -> bool:
    """Check if agent exists."""
    return FrameworkUtils.agent_exists(name, base_dir)