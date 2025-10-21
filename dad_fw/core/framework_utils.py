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
    
    @staticmethod
    def upload_all_agents(
        base_dir: Path,
        workspace_id: Optional[str] = None,
        use_ipynb: bool = False,
        force_update: bool = False
    ) -> List[dict]:
        """Upload all agents in the workspace."""
        from rich import print as rprint
        
        agents = FrameworkUtils.list_agents(base_dir)
        if not agents:
            rprint(f"[yellow]No agents found in {base_dir}[/yellow]")
            return []
        
        rprint(f"[cyan]Found {len(agents)} agents to upload[/cyan]")
        results = []
        
        for agent in agents:
            try:
                rprint(f"\n[blue]Uploading: {agent.name}[/blue]")
                agent.load_config()
                
                # Auto-compile if needed and not using ipynb
                if not use_ipynb:
                    fabric_file = agent.get_fabric_python_file()
                    if not fabric_file.exists():
                        rprint("[cyan]  Auto-compiling notebook...[/cyan]")
                        agent.convert_ipynb_to_fabric_python()
                
                # Upload to Fabric
                upload_result = agent.upload_to_fabric(
                    workspace_id=workspace_id,
                    notebook_name=None,  # Use default agent name
                    use_ipynb=use_ipynb,
                    force_update=force_update,
                    ask_before_update=not force_update
                )
                
                result = {
                    'agent': agent.name,
                    'success': True,
                    'updated': upload_result.get('updated', False),
                    'notebook_id': upload_result.get('id') if upload_result else None
                }
                
                if result['updated']:
                    rprint(f"[green]  ✓ Updated existing notebook[/green]")
                else:
                    rprint(f"[green]  ✓ Uploaded new notebook[/green]")
                
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
    
    @staticmethod
    def run_all_agents(
        base_dir: Path,
        workspace_id: Optional[str] = None
    ) -> List[dict]:
        """Run all agents in the workspace."""
        from rich import print as rprint
        
        agents = FrameworkUtils.list_agents(base_dir)
        if not agents:
            rprint(f"[yellow]No agents found in {base_dir}[/yellow]")
            return []
        
        rprint(f"[cyan]Found {len(agents)} agents to run[/cyan]")
        results = []
        
        for agent in agents:
            try:
                rprint(f"\n[blue]Running: {agent.name}[/blue]")
                
                # Load config to check if agent was uploaded
                config = agent.load_config()
                notebook_name = config.get("notebook_name")
                notebook_id = config.get("notebook_id")
                
                if not notebook_name and not notebook_id:
                    result = {
                        'agent': agent.name,
                        'success': False,
                        'error': 'Agent not uploaded yet. Upload first before running.'
                    }
                    rprint(f"[yellow]  ! Skipped: {result['error']}[/yellow]")
                    results.append(result)
                    continue
                
                # Run in Fabric
                run_result = agent.run_in_fabric(workspace_id=workspace_id)
                
                result = {
                    'agent': agent.name,
                    'success': run_result['success'],
                    'status': run_result['status'],
                    'runtime': run_result['total_runtime_str'],
                    'job_id': run_result['job_id'],
                    'agent_found': run_result.get('agent_found', False),
                    'agent_id': run_result.get('agent_id'),
                    'error': None if run_result['success'] else run_result['status']
                }
                
                if result['success']:
                    rprint(f"[green]  ✓ Completed ({result['runtime']})[/green]")
                    if result['agent_found']:
                        rprint(f"[green]    Data agent created: {result['agent_id']}[/green]")
                else:
                    rprint(f"[red]  ✗ Failed: {result['status']}[/red]")
                
                results.append(result)
                
            except Exception as e:
                result = {
                    'agent': agent.name,
                    'success': False,
                    'error': str(e),
                    'status': 'Error',
                    'runtime': '0s',
                    'job_id': None
                }
                rprint(f"[red]  ✗ Exception: {e}[/red]")
                results.append(result)
        
        return results