"""
Data Agent Development Framework - Object-Oriented Version

A modern, object-oriented redesign of the DAD-FW CLI toolkit for creating, 
managing, and deploying Microsoft Fabric data agents.
"""

__version__ = "2.0.0"
__author__ = "DAD-FW Contributors" 
__description__ = "Data Agent Development Framework - Object-Oriented Version"

from .core import FrameworkUtils, DataAgent

__all__ = ['FrameworkUtils', 'DataAgent']