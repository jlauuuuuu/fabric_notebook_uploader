from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import json
import shutil

from dad_fw.core.fabric_api import FabricAPI

class DataAgent:
    """A simple data agent with basic operations."""

    def __init__(self, name: str, base_dir: Path):
        self._name = name
        self._folder_name = name.replace(" ", "_").replace("-", "_").lower()
        self._base_dir = base_dir
        self._agent_dir = base_dir / self._folder_name

        # Simple file paths
        self._config_file = self._agent_dir / "config.json"
        self._notebook_file = self._agent_dir / f"{self._folder_name}.ipynb"
        self._readme_file = self._agent_dir / "README.md"
        self._testing_file = self._agent_dir / f"{self._folder_name}_testing.ipynb"
        self._fabric_python_file = None # File for compile notebook

        # Default config
        self._config: Dict[str, Any] = {
            "agent_name": self._name,
            "folder_name": self._folder_name,
            "created_date": datetime.now().isoformat(),
            "workspace_id": "",
            "status": "scaffolded",
            "tenant_id": "",
            "notebook_id": "",
            "notebook_name": "",
            "agent_id": "",
            "agent_url": ""
        }

    # Public properties for controlled access
    @property
    def name(self) -> str:
        """Get the agent name."""
        return self._name

    @property
    def folder_name(self) -> str:
        """Get the folder name."""
        return self._folder_name

    @property
    def agent_dir(self) -> Path:
        """Get the agent directory path."""
        return self._agent_dir

    def get_config_file(self) -> Path:
        """Get the config file path."""
        return self._config_file

    def get_notebook_file(self) -> Path:
        """Get the notebook file path."""
        return self._notebook_file

    def get_readme_file(self) -> Path:
        """Get the readme file path."""
        return self._readme_file

    def get_testing_file(self) -> Path:
        """Get the testing file path."""
        return self._testing_file

    def get_fabric_python_file(self) -> Path:
        """Get the fabric python file path."""
        if self._fabric_python_file is None:
            # Return default path if not set
            return self._agent_dir / f"{self._folder_name}_fabric.py"
        return self._fabric_python_file

    def set_fabric_python_file(self, file_path: str) -> None:
        """Set the fabric python file path."""
        self._fabric_python_file = Path(file_path)
    
    def has_fabric_python_file(self) -> bool:
        """Check if a fabric python file path has been set."""
        return self._fabric_python_file is not None

    def exists(self) -> bool:
        """Check if agent folder exists."""
        return self._agent_dir.exists()

    def create_default_config(self) -> Dict[str, Any]:
        """Return the default configuration."""
        return self._config

    def save_config(self, config: Dict[str, Any]) -> None:
        """Save configuration to file."""
        self._config_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self._config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)

    def load_config(self) -> Dict[str, Any]:
        """Load configuration from file."""
        if self._config_file.exists():
            with open(self._config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        print("Config file not found")
        return {}

    def create(self, force: bool = False) -> None:
        """Create the agent with all its files and directories."""
        # Check if agent already exists
        if self.exists() and not force:
            raise Exception(f"Agent '{self._folder_name}' already exists")
        
        # Create agent directory (will be created automatically when writing files)
        self._agent_dir.mkdir(parents=True, exist_ok=True)
        
        # Create config file
        config = self.create_default_config()
        self.save_config(config)
        self._create_notebook()
        self._create_readme()
        self._copy_testing_template()

    def _create_notebook(self) -> None:
        """Create notebook from template."""
        template_path = self._get_templates_dir() / "data_agent_template.ipynb"
        if template_path.exists():
            with open(template_path, 'r', encoding='utf-8') as f:
                content = f.read()

            content = content.replace("data-agent-name", self._name)
            self._notebook_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self._notebook_file, 'w', encoding='utf-8') as f:
                f.write(content)

    def _create_readme(self) -> None:
        """Create README from template."""
        template_path = self._get_templates_dir() / "readme_template.md"
        if template_path.exists():
            # Read template
            with open(template_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            content = content.replace("{agent_name}", self._name)
            content = content.replace("{folder_name}", self._folder_name)
            content = content.replace("{created_date}", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            
            self._readme_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self._readme_file, 'w', encoding='utf-8') as f:
                f.write(content)

    def _copy_testing_template(self) -> None:
        """Copy testing template if it exists."""
        template_path = self._get_templates_dir() / "testing_template.ipynb"
        if template_path.exists():
            self._testing_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(template_path, self._testing_file)

    def _get_templates_dir(self) -> Path:
        """Get the templates directory."""
        return Path(__file__).parent.parent / "templates"
    

    def convert_ipynb_to_fabric_python(self, output_file_path: str = None) -> str:
        if not self._notebook_file.exists():
            raise FileNotFoundError(f"Notebook file not found: {self._notebook_file}")
        
        # Load config for metadata
        config = self.load_config()
        
        # Determine output path: parameter > stored path > default
        if output_file_path is None:
            if self._fabric_python_file is not None:
                output_file_path = str(self._fabric_python_file)
            else:
                output_file_path = str(self._agent_dir / f"{self._folder_name}_fabric.py")
                # Store this as the default path
                self.set_fabric_python_file(output_file_path)
        
        # Read the .ipynb file
        with open(self._notebook_file, "r", encoding="utf-8") as f:
            notebook_data = json.load(f)
        
        # Start building the Fabric Python content
        fabric_content = []
        
        # Add header
        fabric_content.append("# Fabric notebook source\n\n")
        fabric_content.append("# METADATA ********************\n\n")
        fabric_content.append("# META {\n")
        fabric_content.append("# META   \"kernel_info\": {\n")
        fabric_content.append("# META     \"name\": \"synapse_pyspark\"\n")
        fabric_content.append("# META   }\n")
        fabric_content.append("# META }\n\n")
        
        # Process each cell
        for cell in notebook_data.get('cells', []):
            if cell.get("cell_type") == "code":
                # Check if it's a parameters cell
                is_param_cell = False
                try:
                    tags = cell.get("metadata", {}).get("tags", [])
                    is_param_cell = "parameters" in tags
                except:
                    pass
                
                # Get cell source
                source_lines = cell.get("source", [])
                if not source_lines:
                    continue
                
                first_line = source_lines[0] if source_lines else ""
                
                # Add cell header
                if is_param_cell:
                    fabric_content.append("# PARAMETERS CELL ********************\n\n")
                else:
                    fabric_content.append("# CELL ********************\n\n")
                
                # Handle different cell types
                if first_line.startswith("%%sql"):
                    # SQL cell
                    for line in source_lines:
                        fabric_content.append(f"# MAGIC {line}")
                    fabric_content.append("\n\n")
                    self._add_cell_metadata(fabric_content, "sparksql")
                    
                elif first_line.startswith("%%configure"):
                    # Configure cell
                    for line in source_lines:
                        fabric_content.append(f"# MAGIC {line}")
                    fabric_content.append("\n\n")
                    self._add_cell_metadata(fabric_content, "python")
                    
                elif first_line.startswith("%%") or first_line.startswith("%"):
                    # Magic commands
                    for line in source_lines:
                        fabric_content.append(f"# MAGIC {line}")
                    fabric_content.append("\n\n")
                    self._add_cell_metadata(fabric_content, "python")
                    
                else:
                    # Regular Python code
                    for line in source_lines:
                        fabric_content.append(line)
                    fabric_content.append("\n\n")
                    self._add_cell_metadata(fabric_content, "python")
                    
            elif cell.get("cell_type") == "markdown":
                # Markdown cell
                fabric_content.append("# MARKDOWN ********************\n\n")
                for line in cell.get("source", []):
                    fabric_content.append(f"# {line}")
                fabric_content.append("\n\n")
        
        # Join all content
        result = "".join(fabric_content)
        
        # Remove trailing newlines and ensure single newline at end
        result = result.rstrip() + "\n"
        
        output_path = Path(output_file_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(result)
        
        return result
    
    def _add_cell_metadata(self, fabric_content: list, language: str = "python") -> None:
        """Helper method to add cell metadata."""
        fabric_content.append("# METADATA ********************\n\n")
        fabric_content.append("# META {\n")
        fabric_content.append(f"# META   \"language\": \"{language}\",\n")
        fabric_content.append("# META   \"language_group\": \"synapse_pyspark\"\n")
        fabric_content.append("# META }\n\n")
    
    def upload_to_fabric(self, workspace_id: Optional[str] = None, notebook_name: Optional[str] = None, 
                        use_ipynb: bool = False, force_update: bool = False, 
                        ask_before_update: bool = True) -> Dict[str, Any]:
        # Import here to avoid circular imports
        from dad_fw.core.fabric_api import FabricAPI
        
        # Load agent config
        config = self.load_config()
        
        # Determine workspace ID
        if workspace_id:
            target_workspace_id = workspace_id
        elif config.get("workspace_id"):
            target_workspace_id = config["workspace_id"]
        else:
            raise ValueError("No workspace ID provided. Either pass workspace_id parameter or set workspace_id in agent config")
        
        # Validate workspace ID format
        if not FabricAPI.validate_workspace_id(target_workspace_id):
            raise ValueError(f"Invalid workspace ID format: {target_workspace_id}")
        
        # Determine notebook name
        display_name = notebook_name if notebook_name else self._name
        
        # Check for existing notebook
        existing_notebook = None
        update_mode = False
        
        # First try to find by stored notebook_id
        if config.get("notebook_id"):
            existing_notebook = FabricAPI.get_notebook_by_id(target_workspace_id, config["notebook_id"])
            if existing_notebook:
                update_mode = True
        
        # If not found by ID, try to find by name
        if not existing_notebook:
            existing_notebook = FabricAPI.find_notebook_by_name(target_workspace_id, display_name)
            if existing_notebook:
                update_mode = True
        
        # Handle update vs create logic
        if update_mode and existing_notebook:
            should_update = force_update
            
            # Ask user if they want to update (unless forced)
            if not force_update and ask_before_update:
                import typer
                should_update = typer.confirm(f"Notebook '{display_name}' exists. Update existing notebook?")
            
            if should_update:
                # Update existing notebook (only supports Fabric Python format for now)
                if use_ipynb:
                    raise ValueError("Updating existing notebooks with .ipynb format is not yet supported. Use Fabric Python format.")
                
                # Ensure we have the compiled file
                fabric_file = self.get_fabric_python_file()
                if not fabric_file.exists():
                    self.convert_ipynb_to_fabric_python()
                
                # Update the existing notebook
                success = FabricAPI.update_notebook_from_fabric_python_file(
                    workspace_id=target_workspace_id,
                    notebook_id=existing_notebook['id'],
                    fabric_python_file_path=str(fabric_file)
                )
                
                if success:
                    # Create a response-like dict for consistency
                    result = {
                        'id': existing_notebook['id'],
                        'display_name': display_name,
                        'updated': True
                    }
                else:
                    raise Exception("Failed to update existing notebook")
            else:
                raise ValueError("Notebook exists and update was declined")
        
        else:
            # Create new notebook
            if use_ipynb:
                # Upload raw .ipynb file
                if not self._notebook_file.exists():
                    raise FileNotFoundError(f"Notebook file not found: {self._notebook_file}")
                
                result = FabricAPI.create_notebook_from_ipynb(
                    workspace_id=target_workspace_id,
                    ipynb_file_path=str(self._notebook_file),
                    notebook_name=display_name
                )
            else:
                # Upload Fabric Python format
                fabric_file = self.get_fabric_python_file()
                
                # Auto-compile if Fabric Python file doesn't exist
                if not fabric_file.exists():
                    self.convert_ipynb_to_fabric_python()
                
                result = FabricAPI.create_notebook_from_fabric_python_file(
                    workspace_id=target_workspace_id,
                    fabric_python_file_path=str(fabric_file),
                    notebook_name=display_name
                )
        
        # Update agent config with upload info
        config["status"] = "uploaded"
        config["notebook_name"] = display_name
        config["workspace_id"] = target_workspace_id
        config["last_upload"] = datetime.now().isoformat()
        
        # Save notebook ID if available in response
        if result and isinstance(result, dict) and "id" in result:
            config["notebook_id"] = result["id"]
        
        self.save_config(config)
        
        return result
    
    def run_in_fabric(self, workspace_id: Optional[str] = None) -> Dict[str, Any]:
        # Import here to avoid circular imports
        from dad_fw.core.fabric_api import FabricAPI
        
        # Load agent config
        config = self.load_config()
        
        # Determine workspace ID
        if workspace_id:
            target_workspace_id = workspace_id
        elif config.get("workspace_id"):
            target_workspace_id = config["workspace_id"]
        else:
            raise ValueError("No workspace ID provided. Either pass workspace_id parameter or set workspace_id in agent config")
        
        # Validate workspace ID format
        if not FabricAPI.validate_workspace_id(target_workspace_id):
            raise ValueError(f"Invalid workspace ID format: {target_workspace_id}")
        
        # Check if we have notebook info
        notebook_id = config.get("notebook_id")
        notebook_name = config.get("notebook_name")
        
        if not notebook_id and not notebook_name:
            raise ValueError("No notebook ID or name found in config. Make sure the notebook has been uploaded first.")
        
        # Execute the notebook (prefer by name if available, fallback to ID)
        if notebook_name:
            result = FabricAPI.run_notebook_by_name(
                workspace_id=target_workspace_id,
                notebook_name=notebook_name
            )
        elif notebook_id:
            result = FabricAPI.run_notebook_by_id(
                workspace_id=target_workspace_id,
                notebook_id=notebook_id
            )
        
        # Try to find created data agent after successful execution
        agent_id = None
        agent_url = None
        
        if result['success']:
            try:
                # List all data agents in workspace
                agents = FabricAPI.list_data_agents_in_workspace(target_workspace_id)
                
                # Look for an agent with name matching our agent
                matching_agents = []
                for agent in agents:
                    agent_display_name = agent['displayName'].lower()
                    our_agent_name = self._name.lower()
                    
                    # Check for exact match or close match (handle underscores/spaces)
                    normalized_agent_name = agent_display_name.replace(' ', '_').replace('-', '_')
                    normalized_our_name = our_agent_name.replace(' ', '_').replace('-', '_')
                    
                    if (normalized_agent_name == normalized_our_name or 
                        agent_display_name == our_agent_name or 
                        our_agent_name in agent_display_name):
                        matching_agents.append(agent)
                
                if matching_agents:
                    # Use the first matching agent
                    selected_agent = matching_agents[0]
                    agent_id = selected_agent['id']
                    
                    # Construct agent URL
                    agent_url = f"https://api.fabric.microsoft.com/v1/workspaces/{target_workspace_id}/aiskills/{agent_id}/aiassistant/openai"
                    
                    result['agent_found'] = True
                    result['agent_id'] = agent_id
                    result['agent_url'] = agent_url
                    result['agent_display_name'] = selected_agent['displayName']
                else:
                    result['agent_found'] = False
                    
            except Exception as e:
                result['agent_found'] = False
                result['agent_search_error'] = str(e)
        
        # Update config with execution results
        config["last_execution"] = {
            'job_id': result['job_id'],
            'status': result['status'],
            'success': result['success'],
            'runtime': result['total_runtime_str'],
            'timestamp': datetime.now().isoformat()
        }
        
        # Update agent info if found
        if agent_id:
            config['agent_id'] = agent_id
            config['agent_url'] = agent_url
        
        # Update overall status
        config['status'] = 'executed_successfully' if result['success'] else 'execution_failed'
        
        self.save_config(config)
        
        return result