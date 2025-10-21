"""
Strictly stateless FrameworkUtils for robust CLI and pipeline usage.
Each method is self-contained with no persistent state.
"""
from pathlib import Path
from typing import List, Optional

from .data_agent import DataAgent


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
    def get_all_agents(base_dir: Path) -> List[DataAgent]:
        """Get all agents in the workspace. Alias for list_agents for cleaner code."""
        return FrameworkUtils.list_agents(base_dir)
    
    @staticmethod
    def validate_workspace(base_dir: Path) -> bool:
        """Validate that the directory is a valid workspace for agents."""
        return base_dir.exists() and base_dir.is_dir()
    
    @staticmethod
    def agent_exists(name: str, base_dir: Path) -> bool:
        """Check if an agent exists in the specified directory."""
        agent = DataAgent(name, base_dir)
        return agent.exists()
    
    @staticmethod
    def compile_all_agents(
        base_dir: Path, 
        custom_output_dir: Optional[Path] = None,
        output_name_suffix: str = "_fabric"
    ) -> List[dict]:
        """Compile all agents in the workspace."""
        from rich import print as rprint
        
        agents = FrameworkUtils.list_agents(base_dir)
        if not agents:
            rprint(f"[yellow]No agents found in {base_dir}[/yellow]")
            return []
        
        rprint(f"[cyan]Found {len(agents)} agents to compile[/cyan]")
        results = []
        
        for agent in agents:
            try:
                rprint(f"\n[blue]Compiling: {agent.name}[/blue]")
                
                # Check if notebook exists
                if not agent.get_notebook_file().exists():
                    result = {
                        'agent': agent.name,
                        'success': False,
                        'error': f"Notebook file not found: {agent.get_notebook_file()}"
                    }
                    rprint(f"[red]  ✗ {result['error']}[/red]")
                    results.append(result)
                    continue
                
                # Build output path if custom directory is provided
                output_file_path = None
                if custom_output_dir:
                    output_dir = custom_output_dir.resolve()
                    filename = f"{agent.folder_name}{output_name_suffix}.py"
                    output_file_path = str(output_dir / filename)
                    agent.set_fabric_python_file(output_file_path)
                
                # Convert notebook
                agent.convert_ipynb_to_fabric_python(output_file_path=output_file_path)
                output_file = agent.get_fabric_python_file()
                file_size = output_file.stat().st_size
                
                result = {
                    'agent': agent.name,
                    'success': True,
                    'output_file': str(output_file),
                    'file_size': file_size
                }
                rprint(f"[green]  ✓ Compiled to: {output_file} ({file_size:,} bytes)[/green]")
                results.append(result)
                
            except Exception as e:
                result = {
                    'agent': agent.name,
                    'success': False,
                    'error': str(e)
                }
                rprint(f"[red]  ✗ Failed: {e}[/red]")
                results.append(result)
        
        return results
    

    
