"""
Data Agent Development Framework (DAD-FW)
A comprehensive CLI toolkit for creating, managing, and deploying Microsoft Fabric data agents.
"""

__version__ = "1.0.0"
__author__ = "DAD-FW Contributors"
__description__ = "Data Agent Development Framework for Microsoft Fabric"

# Import and expose the FabricDataAgentClient for easy access
from .fabric_data_agent_client import FabricDataAgentClient

# Make it available when someone does 'from dad_fw import *'
__all__ = ["FabricDataAgentClient"]