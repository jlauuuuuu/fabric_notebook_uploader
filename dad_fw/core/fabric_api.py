from pathlib import Path
import json
import base64
import subprocess
import time
import requests
from typing import Dict, Any, Optional

from msfabricpysdkcore import FabricClientCore



class FabricAPI:
    @staticmethod
    def create_notebook_from_ipynb(workspace_id: str, ipynb_file_path: str, notebook_name: str) -> Dict[str, Any]:
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
        fc = FabricClientCore()
        
        # List all items in the workspace
        items = fc.list_items(workspace_id=workspace_id)
        
        # Filter for notebooks with matching name (case-insensitive)
        for item in items:
            if (hasattr(item, 'type') and item.type == 'Notebook' and 
                hasattr(item, 'display_name') and item.display_name.lower() == notebook_name.lower()):
                return {
                    'id': item.id,
                    'display_name': item.display_name,
                    'type': item.type,
                    'workspace_id': workspace_id
                }
        
        return None
    
    @staticmethod
    def get_notebook_by_id(workspace_id: str, notebook_id: str) -> Optional[Dict[str, Any]]:
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
        if not workspace_id or not isinstance(workspace_id, str):
            return False
        
        # Basic UUID format check (Fabric workspace IDs are typically UUIDs)
        if len(workspace_id) == 36 and workspace_id.count('-') == 4:
            return True
        
        return False

    @staticmethod
    def _get_bearer_token() -> str:
        try:
            result = subprocess.run(
                "az account get-access-token --resource https://api.fabric.microsoft.com",
                capture_output=True,
                text=True,
                check=True,
                shell=True
            )
            token_data = json.loads(result.stdout)
            return token_data["accessToken"]
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to get Azure token. Make sure you're logged in with 'az login'. Error: {e.stderr}")
        except (json.JSONDecodeError, KeyError) as e:
            raise RuntimeError(f"Failed to parse token response: {e}")
        except FileNotFoundError:
            raise RuntimeError("Azure CLI not found. Please install Azure CLI and run 'az login'")

    @staticmethod
    def _request_notebook_download(workspace_id: str, notebook_id: str, access_token: str, 
                                   as_ipynb: bool = True, verify_ssl: bool = False) -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks/{notebook_id}/getDefinition"
        if as_ipynb:
            url += "?format=ipynb"
        
        try:
            resp = requests.post(url, headers=headers, verify=verify_ssl)
            
            if resp.status_code == 202:
                location = resp.headers.get("Location")
                if not location:
                    return {
                        "success": False,
                        "status_code": 202,
                        "error": "202 response received but no Location header found"
                    }
                return {
                    "success": True,
                    "location": location,
                    "status_code": 202
                }
            else:
                try:
                    error_body = resp.json()
                    error_msg = error_body.get("message", resp.text)
                except:
                    error_msg = resp.text
                return {
                    "success": False,
                    "status_code": resp.status_code,
                    "error": f"HTTP {resp.status_code}: {error_msg}"
                }
        except requests.exceptions.RequestException as e:
            return {
                "success": False,
                "status_code": None,
                "error": f"Request failed: {str(e)}"
            }

    @staticmethod
    def _check_download_status(location_url: str, access_token: str, verify_ssl: bool = False) -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        try:
            resp = requests.get(location_url, headers=headers, verify=verify_ssl)
            
            if resp.status_code == 200:
                data = resp.json()
                status = data.get("status", "Unknown")
                percent = data.get("percentComplete", 0)
                
                if status == "Failed":
                    error_info = data.get("error", "Unknown error")
                    return {
                        "success": True,
                        "status": status,
                        "percent_complete": percent,
                        "error": f"Operation failed: {error_info}"
                    }
                
                return {
                    "success": True,
                    "status": status,
                    "percent_complete": percent
                }
            elif resp.status_code == 202:
                return {
                    "success": True,
                    "status": "InProgress",
                    "percent_complete": 0
                }
            else:
                return {
                    "success": False,
                    "status": "Error",
                    "error": f"Unexpected HTTP {resp.status_code}: {resp.text}"
                }
        except requests.exceptions.RequestException as e:
            return {
                "success": False,
                "status": "Error",
                "error": f"Request failed: {str(e)}"
            }

    @staticmethod
    def _get_download_result(location_url: str, access_token: str, verify_ssl: bool = False) -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        result_url = location_url.rstrip('/') + '/result'
        
        try:
            resp = requests.get(result_url, headers=headers, verify=verify_ssl)
            
            if resp.status_code == 200:
                data = resp.json()
                definition = data.get("definition", {})
                parts = definition.get("parts", [])
                
                if not parts:
                    return {
                        "success": False,
                        "error": "No parts found in response"
                    }
                
                return {
                    "success": True,
                    "parts": parts
                }
            else:
                try:
                    error_body = resp.json()
                    error_msg = error_body.get("message", resp.text)
                except:
                    error_msg = resp.text
                
                return {
                    "success": False,
                    "error": f"HTTP {resp.status_code}: {error_msg}"
                }
        except requests.exceptions.RequestException as e:
            return {
                "success": False,
                "error": f"Request failed: {str(e)}"
            }

    @staticmethod
    def download_notebook_by_id(workspace_id: str, notebook_id: str, data_agent_name: Optional[str] = None,
                               output_dir: str = "./downloaded_notebooks", timeout: int = 300, 
                               poll_interval: int = 5, as_ipynb: bool = True, verify_ssl: bool = False) -> Dict[str, Any]:
        from dad_fw.core.framework_utils import FrameworkUtils
        
        start_time = time.time()
        
        # Step 1: Get bearer token
        try:
            access_token = FabricAPI._get_bearer_token()
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to get token: {str(e)}",
                "elapsed_time": time.time() - start_time
            }
        
        # Step 2: Request notebook download
        request_result = FabricAPI._request_notebook_download(
            workspace_id, notebook_id, access_token, as_ipynb, verify_ssl
        )
        
        if not request_result["success"]:
            return {
                "success": False,
                "error": request_result.get("error", "Failed to request notebook"),
                "elapsed_time": time.time() - start_time
            }
        
        location_url = request_result["location"]
        
        # Step 3: Poll for completion
        poll_start = time.time()
        first_check = True
        
        while True:
            elapsed = time.time() - poll_start
            
            if elapsed > timeout:
                return {
                    "success": False,
                    "error": f"Operation timed out after {timeout} seconds",
                    "elapsed_time": time.time() - start_time
                }
            
            if not first_check:
                time.sleep(poll_interval)
            first_check = False
            
            status_result = FabricAPI._check_download_status(location_url, access_token, verify_ssl)
            
            if not status_result["success"]:
                return {
                    "success": False,
                    "error": status_result.get("error", "Failed to check status"),
                    "elapsed_time": time.time() - start_time
                }
            
            status = status_result["status"]
            
            if status == "Succeeded":
                break
            elif status == "Failed":
                return {
                    "success": False,
                    "error": status_result.get("error", "Operation failed"),
                    "elapsed_time": time.time() - start_time
                }
        
        # Step 4: Get the response
        response_result = FabricAPI._get_download_result(location_url, access_token, verify_ssl)
        
        if not response_result["success"]:
            return {
                "success": False,
                "error": response_result.get("error", "Failed to get response"),
                "elapsed_time": time.time() - start_time
            }
        
        parts = response_result["parts"]
        
        # Step 5: Decode and write files
        # Find the .ipynb file
        ipynb_part = None
        for part in parts:
            if part.get("path", "").endswith(".ipynb"):
                ipynb_part = part
                break
        
        if not ipynb_part:
            return {
                "success": False,
                "error": "No .ipynb file found in response",
                "elapsed_time": time.time() - start_time
            }
        
        # Decode the notebook content
        try:
            payload = ipynb_part.get("payload", "")
            decoded_bytes = base64.b64decode(payload)
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to decode notebook: {str(e)}",
                "elapsed_time": time.time() - start_time
            }
        
        # Determine where to write the file
        if data_agent_name:
            # Use framework to find agent and replace its notebook
            try:
                agent = FrameworkUtils.get_agent(data_agent_name, Path.cwd())
                if not agent:
                    return {
                        "success": False,
                        "error": f"Agent '{data_agent_name}' not found in workspace",
                        "elapsed_time": time.time() - start_time
                    }
                
                notebook_file = agent.get_notebook_file()
                notebook_file.write_bytes(decoded_bytes)
                written_files = [str(notebook_file)]
            except Exception as e:
                return {
                    "success": False,
                    "error": f"Failed to write to agent notebook: {str(e)}",
                    "elapsed_time": time.time() - start_time
                }
        else:
            # Write to output directory
            try:
                output_path = Path(output_dir)
                output_path.mkdir(parents=True, exist_ok=True)
                file_path = output_path / ipynb_part.get("path", "notebook.ipynb")
                file_path.write_bytes(decoded_bytes)
                written_files = [str(file_path)]
            except Exception as e:
                return {
                    "success": False,
                    "error": f"Failed to write file: {str(e)}",
                    "elapsed_time": time.time() - start_time
                }
        
        elapsed_total = time.time() - start_time
        return {
            "success": True,
            "written_files": written_files,
            "elapsed_time": elapsed_total
        }

    @staticmethod
    def download_notebook_by_name(workspace_id: str, notebook_name: str, data_agent_name: Optional[str] = None,
                                 output_dir: str = "./downloaded_notebooks", timeout: int = 300,
                                 poll_interval: int = 5, as_ipynb: bool = True, verify_ssl: bool = False) -> Dict[str, Any]:
        # Find the notebook by name to get its ID
        notebook_info = FabricAPI.find_notebook_by_name(workspace_id, notebook_name)
        
        if not notebook_info:
            return {
                'success': False,
                'error': f'Notebook "{notebook_name}" not found in workspace {workspace_id}'
            }
        
        # Download using ID
        return FabricAPI.download_notebook_by_id(
            workspace_id=workspace_id,
            notebook_id=notebook_info['id'],
            data_agent_name=data_agent_name,
            output_dir=output_dir,
            timeout=timeout,
            poll_interval=poll_interval,
            as_ipynb=as_ipynb,
            verify_ssl=verify_ssl
        )