"""
Fabric Notebook Uploader

A Python toolkit for managing Jupyter notebooks in Microsoft Fabric and testing Data Agents locally.
"""

# Core notebook functionality
from .convert_nb import convert_ipynb_to_fabric_python
from .create_nb import create_notebook_from_fabric_python, create_notebook_from_ipynb
from .run_nb import run_notebook_by_name, run_notebook_by_id

# Data agent testing
from .data_agent_client import FabricDataAgentClient, create_data_agent_client_from_env
from .test_agent import DataAgentTester, run_quick_test, run_comprehensive_test

__version__ = "1.0.0"
__all__ = [
    # Notebook Management
    "convert_ipynb_to_fabric_python",
    "create_notebook_from_fabric_python", 
    "create_notebook_from_ipynb",
    "run_notebook_by_name",
    "run_notebook_by_id",
    # Data Agent Testing
    "FabricDataAgentClient",
    "create_data_agent_client_from_env", 
    "DataAgentTester",
    "run_quick_test",
    "run_comprehensive_test"
]