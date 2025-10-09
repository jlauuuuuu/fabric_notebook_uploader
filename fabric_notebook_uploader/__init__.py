"""
Fabric Notebook Uploader Module

This module provides utilities for converting Jupyter notebooks to Microsoft Fabric format
and uploading them to Fabric workspaces.
"""

from .convert_nb import convert_ipynb_to_fabric_python
from .create_nb import create_notebook_from_fabric_python, create_notebook_from_ipynb
from .run_nb import run_notebook_by_name, run_notebook_by_id, list_notebooks_in_workspace, get_notebook_id_by_name

__version__ = "1.0.0"
__all__ = [
    "convert_ipynb_to_fabric_python",
    "create_notebook_from_fabric_python", 
    "create_notebook_from_ipynb",
    "run_notebook_by_name",
    "run_notebook_by_id", 
    "list_notebooks_in_workspace",
    "get_notebook_id_by_name"
]