"""
Fabric API utilities for uploading notebooks and interacting with Microsoft Fabric.
Stateless utility class following the same pattern as FrameworkUtils.
"""
from pathlib import Path
import json
import base64
from typing import Dict, Any, Optional

from msfabricpysdkcore import FabricClientCore



class FabricAPI:
    """
    Stateless utility class for Microsoft Fabric API operations.
    All methods are static to maintain consistency with FrameworkUtils pattern.
    """
    @staticmethod
    def create_notebook_from_ipynb(workspace_id: str, ipynb_file_path: str, notebook_name: str) -> Dict[str, Any]:
        """
        Create a Fabric notebook directly from a .ipynb file (raw upload).
        
        Args:
            workspace_id: The Fabric workspace ID
            ipynb_file_path: Path to the .ipynb file
            notebook_name: Display name for the notebook in Fabric
            
        Returns:
            Dictionary containing notebook creation response
            
        Raises:
            ImportError: If Fabric SDK is not available
            FileNotFoundError: If ipynb file doesn't exist
        """
        ipynb_path = Path(ipynb_file_path)
        if not ipynb_path.exists():
            raise FileNotFoundError(f"Notebook file not found: {ipynb_path}")
        
        fc = FabricClientCore()
        
        # Read the .ipynb file
        with open(ipynb_path, 'r', encoding='utf-8') as file:
            ipynb_content = file.read()
        
        # Convert to Base64
        content_base64 = base64.b64encode(ipynb_content.encode('utf-8')).decode('utf-8')
        
        # Package for Fabric API
        notebook_definition = {
            'parts': [
                {
                    'path': 'notebook-content.py', 
                    'payload': content_base64, 
                    'payloadType': 'InlineBase64'
                }
            ]
        }
        
        # Create the notebook
        notebook = fc.create_notebook(
            workspace_id=workspace_id,
            definition=notebook_definition,
            display_name=notebook_name,
            description="Created from .ipynb file via DAD Framework"
        )
        
        # Convert notebook object to dictionary for consistent return type
        return {
            'id': notebook.id,
            'display_name': notebook.display_name,
            'type': getattr(notebook, 'type', 'Notebook'),
            'workspace_id': workspace_id
        }
    
    @staticmethod
    def create_notebook_from_fabric_python(workspace_id: str, fabric_python_content: str, notebook_name: str) -> Dict[str, Any]:
        """
        Create a Fabric notebook from Fabric Python format content.
        
        Args:
            workspace_id: The Fabric workspace ID
            fabric_python_content: The Fabric Python format content as string
            notebook_name: Display name for the notebook in Fabric
            
        Returns:
            Dictionary containing notebook creation response
            
        Raises:
            ImportError: If Fabric SDK is not available
        """
        fc = FabricClientCore()
        
        # Convert to Base64
        content_base64 = base64.b64encode(fabric_python_content.encode('utf-8')).decode('utf-8')
        
        # Package for Fabric API
        notebook_definition = {
            'parts': [
                {
                    'path': 'notebook-content.py', 
                    'payload': content_base64, 
                    'payloadType': 'InlineBase64'
                }
            ]
        }
        
        # Create the notebook
        notebook = fc.create_notebook(
            workspace_id=workspace_id,
            definition=notebook_definition,
            display_name=notebook_name,
            description="Created from Fabric Python format via DAD Framework"
        )
        
        # Convert notebook object to dictionary for consistent return type
        return {
            'id': notebook.id,
            'display_name': notebook.display_name,
            'type': getattr(notebook, 'type', 'Notebook'),
            'workspace_id': workspace_id
        }
    
    @staticmethod
    def create_notebook_from_fabric_python_file(workspace_id: str, fabric_python_file_path: str, notebook_name: str) -> Dict[str, Any]:
        """
        Create a Fabric notebook from a Fabric Python format file.
        
        Args:
            workspace_id: The Fabric workspace ID
            fabric_python_file_path: Path to the Fabric Python format file
            notebook_name: Display name for the notebook in Fabric
            
        Returns:
            Dictionary containing notebook creation response
            
        Raises:
            ImportError: If Fabric SDK is not available
            FileNotFoundError: If Fabric Python file doesn't exist
        """
        fabric_path = Path(fabric_python_file_path)
        if not fabric_path.exists():
            raise FileNotFoundError(f"Fabric Python file not found: {fabric_path}")
        
        # Read the Fabric Python file
        with open(fabric_path, 'r', encoding='utf-8') as file:
            fabric_python_content = file.read()
        
        return FabricAPI.create_notebook_from_fabric_python(
            workspace_id=workspace_id,
            fabric_python_content=fabric_python_content,
            notebook_name=notebook_name
        )
    
    @staticmethod
    def find_notebook_by_name(workspace_id: str, notebook_name: str) -> Optional[Dict[str, Any]]:
        """
        Find a notebook by name in the specified workspace.
        
        Args:
            workspace_id: The Fabric workspace ID
            notebook_name: The name of the notebook to find
            
        Returns:
            Dictionary containing notebook item info if found, None otherwise
        """
        fc = FabricClientCore()
        
        # List all items in the workspace
        items = fc.list_items(workspace_id=workspace_id)
        
        # Filter for notebooks with matching name
        for item in items:
            if (hasattr(item, 'type') and item.type == 'Notebook' and 
                hasattr(item, 'display_name') and item.display_name == notebook_name):
                return {
                    'id': item.id,
                    'display_name': item.display_name,
                    'type': item.type,
                    'workspace_id': workspace_id
                }
        
        return None
    
    @staticmethod
    def get_notebook_by_id(workspace_id: str, notebook_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a notebook by ID in the specified workspace.
        
        Args:
            workspace_id: The Fabric workspace ID
            notebook_id: The ID of the notebook to get
            
        Returns:
            Dictionary containing notebook item info if found, None otherwise
        """
        fc = FabricClientCore()
        
        try:
            item = fc.get_item(workspace_id=workspace_id, item_id=notebook_id)
            
            if hasattr(item, 'type') and item.type == 'Notebook':
                return {
                    'id': item.id,
                    'display_name': item.display_name,
                    'type': item.type,
                    'workspace_id': workspace_id
                }
        except Exception:
            # Item not found or not accessible
            pass
        
        return None
    
    @staticmethod
    def update_notebook_definition(workspace_id: str, notebook_id: str, fabric_python_content: str) -> bool:
        """
        Update an existing notebook's content with Fabric Python format.
        
        Args:
            workspace_id: The Fabric workspace ID
            notebook_id: The ID of the notebook to update
            fabric_python_content: The Fabric Python format content as string
            
        Returns:
            True if update successful, False otherwise
        """
        fc = FabricClientCore()
        
        try:
            # Convert to Base64
            content_base64 = base64.b64encode(fabric_python_content.encode('utf-8')).decode('utf-8')
            
            # Package for Fabric API
            notebook_definition = {
                'parts': [
                    {
                        'path': 'notebook-content.py', 
                        'payload': content_base64, 
                        'payloadType': 'InlineBase64'
                    }
                ]
            }
            
            # Update the notebook definition
            response = fc.update_item_definition(
                workspace_id=workspace_id,
                item_id=notebook_id,
                definition=notebook_definition
            )
            
            return True
            
        except Exception as e:
            print(f"Error updating notebook: {e}")
            return False
    
    @staticmethod
    def update_notebook_from_fabric_python_file(workspace_id: str, notebook_id: str, fabric_python_file_path: str) -> bool:
        """
        Update an existing notebook from a Fabric Python format file.
        
        Args:
            workspace_id: The Fabric workspace ID
            notebook_id: The ID of the notebook to update
            fabric_python_file_path: Path to the Fabric Python format file
            
        Returns:
            True if update successful, False otherwise
            
        Raises:
            FileNotFoundError: If Fabric Python file doesn't exist
        """
        fabric_path = Path(fabric_python_file_path)
        if not fabric_path.exists():
            raise FileNotFoundError(f"Fabric Python file not found: {fabric_path}")
        
        # Read the Fabric Python file
        with open(fabric_path, 'r', encoding='utf-8') as file:
            fabric_python_content = file.read()
        
        return FabricAPI.update_notebook_definition(
            workspace_id=workspace_id,
            notebook_id=notebook_id,
            fabric_python_content=fabric_python_content
        )
    
    @staticmethod
    def run_notebook_by_id(workspace_id: str, notebook_id: str) -> Dict[str, Any]:
        """
        Execute a notebook by ID and monitor until completion with real-time status updates.
        
        Args:
            workspace_id: The Fabric workspace ID
            notebook_id: The ID of the notebook to run
            
        Returns:
            Dictionary containing execution results
        """
        import time
        
        fc = FabricClientCore()
        
        print(f"Starting execution of notebook ID: {notebook_id}")
        start_time = time.time()
        
        # Enable inline installation for %pip commands
        execution_data = {"_inlineInstallationEnabled": True}
        
        # Start the notebook execution
        job_instance = fc.run_on_demand_item_job(
            workspace_id=workspace_id, 
            item_id=notebook_id, 
            job_type="RunNotebook",
            execution_data=execution_data
        )
        
        print(f"Job started with ID: {job_instance.id}")
        print(f"Initial status: {job_instance.status}")
        
        # Monitor the job until completion
        while job_instance.status in ["InProgress", "NotStarted"]:
            time.sleep(10)  # Wait 10 seconds before checking again
            
            # Get updated job status
            job_instance = fc.get_item_job_instance(
                workspace_id=workspace_id,
                item_id=notebook_id,
                job_instance_id=job_instance.id
            )
            
            # Calculate runtime
            runtime = time.time() - start_time
            runtime_str = time.strftime("%H:%M:%S", time.gmtime(runtime))
            
            print(f"Status: {job_instance.status} - Runtime: {runtime_str}")
        
        # Final status
        total_runtime = time.time() - start_time
        total_runtime_str = time.strftime("%H:%M:%S", time.gmtime(total_runtime))
        
        result = {
            'notebook_id': notebook_id,
            'job_id': job_instance.id,
            'status': job_instance.status,
            'total_runtime': total_runtime,
            'total_runtime_str': total_runtime_str,
            'success': job_instance.status == "Completed"
        }
        
        if job_instance.status == "Completed":
            print(f"Notebook execution completed successfully!")
            print(f"Total runtime: {total_runtime_str}")
        elif job_instance.status == "Failed":
            print(f"Notebook execution failed!")
            print(f"Runtime before failure: {total_runtime_str}")
            print(job_instance)
        else:
            print(f"Notebook execution ended with status: {job_instance.status}")
            print(f"Total runtime: {total_runtime_str}")

        return result
    
    @staticmethod
    def run_notebook_by_name(workspace_id: str, notebook_name: str) -> Dict[str, Any]:
        """
        Execute a notebook by name and monitor until completion with real-time status updates.
        
        Args:
            workspace_id: The Fabric workspace ID
            notebook_name: The display name of the notebook to run
            
        Returns:
            Dictionary containing execution results
        """
        # Find the notebook first
        notebook_info = FabricAPI.find_notebook_by_name(workspace_id, notebook_name)
        if not notebook_info:
            raise ValueError(f"Notebook '{notebook_name}' not found in workspace {workspace_id}")
        
        notebook_id = notebook_info['id']
        print(f"Found notebook: {notebook_name} (ID: {notebook_id})")
        
        import time
        
        fc = FabricClientCore()
        
        print(f"Starting execution of '{notebook_name}'...")
        start_time = time.time()
        
        # Enable inline installation for %pip commands
        execution_data = {"_inlineInstallationEnabled": True}
        
        # Start the notebook execution
        job_instance = fc.run_on_demand_item_job(
            workspace_id=workspace_id, 
            item_id=notebook_id, 
            job_type="RunNotebook",
            execution_data=execution_data
        )
        
        print(f"Job started with ID: {job_instance.id}")
        print(f"Initial status: {job_instance.status}")
        
        # Monitor the job until completion
        while job_instance.status in ["InProgress", "NotStarted"]:
            time.sleep(10)  # Wait 10 seconds before checking again
            
            # Get updated job status
            job_instance = fc.get_item_job_instance(
                workspace_id=workspace_id,
                item_id=notebook_id,
                job_instance_id=job_instance.id
            )
            
            # Calculate runtime
            runtime = time.time() - start_time
            runtime_str = time.strftime("%H:%M:%S", time.gmtime(runtime))
            
            print(f"Status: {job_instance.status} - Runtime: {runtime_str}")
        
        # Final status
        total_runtime = time.time() - start_time
        total_runtime_str = time.strftime("%H:%M:%S", time.gmtime(total_runtime))
        
        result = {
            'notebook_name': notebook_name,
            'notebook_id': notebook_id,
            'job_id': job_instance.id,
            'status': job_instance.status,
            'total_runtime': total_runtime,
            'total_runtime_str': total_runtime_str,
            'success': job_instance.status == "Completed"
        }
        
        if job_instance.status == "Completed":
            print(f"Notebook execution completed successfully!")
            print(f"Total runtime: {total_runtime_str}")
        elif job_instance.status == "Failed":
            print(f"Notebook execution failed!")
            print(f"Runtime before failure: {total_runtime_str}")
            print(job_instance)
        else:
            print(f"Notebook execution ended with status: {job_instance.status}")
            print(f"Total runtime: {total_runtime_str}")

        return result
    
    @staticmethod
    def list_data_agents_in_workspace(workspace_id: str) -> list:
        """
        List all data agents (AI skills) in a workspace.
        
        Args:
            workspace_id: The Fabric workspace ID
            
        Returns:
            List of data agent items
        """
        fc = FabricClientCore()
        
        try:
            # Note: This assumes there's a method to list AI skills/agents
            # The exact API method may vary - this is based on the upload.py reference
            items = fc.list_items(workspace_id=workspace_id)
            
            # Filter for data agents/AI skills (type might be different)
            agents = []
            for item in items:
                # The exact type name may need adjustment based on actual Fabric API
                if hasattr(item, 'type') and item.type in ['DataAgent', 'AISkill', 'Agent']:
                    agents.append({
                        'id': item.id,
                        'displayName': item.display_name,
                        'type': item.type
                    })
            
            return agents
            
        except Exception as e:
            print(f"Error listing data agents: {e}")
            return []
    
    @staticmethod
    def validate_workspace_id(workspace_id: str) -> bool:
        """
        Validate that a workspace ID is properly formatted.
        
        Args:
            workspace_id: The workspace ID to validate
            
        Returns:
            True if valid format, False otherwise
        """
        if not workspace_id or not isinstance(workspace_id, str):
            return False
        
        # Basic UUID format check (Fabric workspace IDs are typically UUIDs)
        if len(workspace_id) == 36 and workspace_id.count('-') == 4:
            return True
        
        return False
