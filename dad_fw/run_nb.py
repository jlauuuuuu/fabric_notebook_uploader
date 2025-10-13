from msfabricpysdkcore import FabricClientCore
import time

def run_notebook_by_name(workspace_id, notebook_name):
    """
    Run a Fabric notebook by its display name.
    
    Args:
        workspace_id: The Fabric workspace ID
        notebook_name: The display name of the notebook to run
        
    Returns:
        dict: Job execution results with status and runtime info
    """
    fc = FabricClientCore()
    
    # Get workspace
    workspace = fc.get_workspace_by_id(id=workspace_id)
    workspace_id = workspace.id
    
    # List all items in workspace to find the notebook
    ws_items = fc.list_items(workspace_id)
    
    # Find the notebook by name
    notebook_item = None
    for item in ws_items:
        if item.type == 'Notebook' and item.display_name == notebook_name:
            notebook_item = item
            break
    
    if not notebook_item:
        raise Exception(f"Notebook '{notebook_name}' not found in workspace {workspace_id}")
    
    print(f"Found notebook: {notebook_item.display_name} (ID: {notebook_item.id})")
    
    # Start the notebook execution
    print(f"Starting execution of '{notebook_name}'...")
    start_time = time.time()
    
    # Enable inline installation for %pip commands
    execution_data = {"_inlineInstallationEnabled": True}
    
    job_instance = fc.run_on_demand_item_job(
        workspace_id=workspace_id, 
        item_id=notebook_item.id, 
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
            item_id=notebook_item.id,
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
        'notebook_id': notebook_item.id,
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

def run_notebook_by_id(workspace_id, notebook_id):
    """
    Run a Fabric notebook by its ID.
    
    Args:
        workspace_id: The Fabric workspace ID
        notebook_id: The ID of the notebook to run
        
    Returns:
        dict: Job execution results with status and runtime info
    """
    fc = FabricClientCore()
    
    print(f"Starting execution of notebook ID: {notebook_id}")
    start_time = time.time()
    
    # Start the notebook execution with inline installation enabled
    execution_data = {"_inlineInstallationEnabled": True}
    
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
    else:
        print(f"Notebook execution ended with status: {job_instance.status}")
        print(f"Total runtime: {total_runtime_str}")
    
    return result

def get_notebook_id_by_name(workspace_id, notebook_name):
    """
    Get a notebook's ID by its display name.
    
    Args:
        workspace_id: The Fabric workspace ID
        notebook_name: The display name of the notebook
        
    Returns:
        str: The notebook ID, or None if not found
    """
    fc = FabricClientCore()
    
    # Get workspace
    workspace = fc.get_workspace_by_id(id=workspace_id)
    workspace_id = workspace.id
    
    # List all items in workspace
    ws_items = fc.list_items(workspace_id)
    
    # Find the notebook by name
    for item in ws_items:
        if item.type == 'Notebook' and item.display_name == notebook_name:
            return item.id
    
    return None

def list_notebooks_in_workspace(workspace_id):
    """
    List all notebooks in a workspace.
    
    Args:
        workspace_id: The Fabric workspace ID
        
    Returns:
        list: List of notebook items with id and display_name
    """
    fc = FabricClientCore()
    
    # Get workspace
    workspace = fc.get_workspace_by_id(id=workspace_id)
    workspace_id = workspace.id
    
    # Get all notebooks
    notebooks = fc.list_notebooks(workspace_id)
    
    notebook_list = []
    for notebook in notebooks:
        notebook_list.append({
            'id': notebook.id,
            'display_name': notebook.display_name,
            'description': getattr(notebook, 'description', '')
        })
    
    return notebook_list