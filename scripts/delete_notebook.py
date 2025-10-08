import requests
from auth_cli import access_token  # Your user-based authentication token provider

API_BASE = "https://api.fabric.microsoft.com/v1"

def delete_notebook(workspace_id, notebook_name=None, notebook_id=None):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Find notebook ID by name if not provided
    if notebook_id is None:
        if not notebook_name:
            raise ValueError("Either notebook_id or notebook_name must be provided.")

        list_url = f"{API_BASE}/workspaces/{workspace_id}/items?filter=type eq 'Notebook'"
        resp = requests.get(list_url, headers=headers)
        resp.raise_for_status()
        notebooks = resp.json().get("value", [])

        notebook_id = None
        for nb in notebooks:
            if nb.get("displayName") == notebook_name:
                notebook_id = nb["id"]
                break

        if notebook_id is None:
            print(f"No notebook named '{notebook_name}' found in workspace {workspace_id}.")
            return False

    # Delete the notebook by notebook ID
    del_url = f"{API_BASE}/workspaces/{workspace_id}/items/{notebook_id}"
    del_resp = requests.delete(del_url, headers=headers)

    if del_resp.status_code == 204:
        print(f"Notebook [{notebook_id}] deleted successfully.")
        return True
    else:
        print(f"Failed to delete notebook [{notebook_id}]. Status: {del_resp.status_code} Response: {del_resp.text}")
        return False


# Example usage:
if __name__ == "__main__":
    workspace_id = "c7d1ecc1-d8fa-477c-baf1-47528abf9fe5"
    notebook_name = "test_notebook4"
    notebook_id = None

    delete_notebook(workspace_id, notebook_name=notebook_name, notebook_id=notebook_id)
