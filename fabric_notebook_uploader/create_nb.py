from msfabricpysdkcore import FabricClientCore
import json
import base64

def create_notebook_from_ipynb(workspace_id, ipynb_file_path, notebook_name):
    """
    Create a Fabric notebook directly from a .ipynb file (raw upload).
    """
    fc = FabricClientCore(silent=True)
    
    # Read your .ipynb file
    with open(ipynb_file_path, 'r', encoding='utf-8') as file:
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
        description="Created from .ipynb file"
    )
    
    return notebook

def create_notebook_from_fabric_python(workspace_id, fabric_python_content, notebook_name):
    """
    Create a Fabric notebook from Fabric Python format content.
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
        description="Created from Fabric Python format"
    )
    
    return notebook