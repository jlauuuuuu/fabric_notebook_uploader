#!/usr/bin/env python3
"""
Global Configuration Manager for Fabric Notebook Uploader

This module manages all configuration including workspaces, tenants, and data agents
with their lifecycle states (notebook -> upload -> creation -> testing).
"""

import json
import os
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass, asdict, field


@dataclass
class DataAgent:
    """Represents a data agent with its lifecycle states."""
    name: str
    description: str = ""
    
    # File paths
    notebook_path: Optional[str] = None  # .ipynb file
    python_path: Optional[str] = None    # converted .py file
    
    # Fabric IDs (populated after upload/creation)
    notebook_id: Optional[str] = None    # Fabric notebook ID
    agent_id: Optional[str] = None       # Fabric data agent ID
    
    # Testing endpoint (populated after agent is published)
    test_url: Optional[str] = None       # Published data agent URL for testing
    
    # Metadata
    created_date: str = field(default_factory=lambda: datetime.now().isoformat())
    last_updated: str = field(default_factory=lambda: datetime.now().isoformat())
    status: str = "created"  # created, notebook_uploaded, agent_created, published, tested
    
    def update_status(self, new_status: str):
        """Update the status and last_updated timestamp."""
        self.status = new_status
        self.last_updated = datetime.now().isoformat()
    
    def get_lifecycle_stage(self) -> int:
        """Get the current lifecycle stage (1-5)."""
        stages = {
            "created": 1,
            "notebook_uploaded": 2,
            "agent_created": 3,
            "published": 4,
            "tested": 5
        }
        return stages.get(self.status, 1)
    
    def get_missing_config(self) -> List[str]:
        """Get list of missing configuration for current stage."""
        missing = []
        
        if not self.notebook_path:
            missing.append("notebook_path (.ipynb file)")
        if not self.python_path:
            missing.append("python_path (.py file)")
        
        # Stage-specific missing configs
        stage = self.get_lifecycle_stage()
        if stage >= 2 and not self.notebook_id:
            missing.append("notebook_id (upload notebook to Fabric)")
        if stage >= 3 and not self.agent_id:
            missing.append("agent_id (create data agent in Fabric)")
        if stage >= 4 and not self.test_url:
            missing.append("test_url (publish data agent for testing)")
        
        return missing


@dataclass
class WorkspaceConfig:
    """Represents a Fabric workspace configuration."""
    workspace_id: str
    workspace_name: str
    tenant_id: str
    description: str = ""
    
    # Optional lakehouse settings
    lakehouse_id: Optional[str] = None
    lakehouse_name: Optional[str] = None
    
    # Data agents in this workspace
    data_agents: Dict[str, DataAgent] = field(default_factory=dict)
    
    def add_data_agent(self, agent: DataAgent) -> None:
        """Add a data agent to this workspace."""
        self.data_agents[agent.name] = agent
    
    def get_data_agent(self, name: str) -> Optional[DataAgent]:
        """Get a data agent by name."""
        return self.data_agents.get(name)
    
    def list_data_agents(self) -> List[str]:
        """List all data agent names in this workspace."""
        return list(self.data_agents.keys())


class ConfigManager:
    """Global configuration manager for the Fabric Notebook Uploader."""
    
    def __init__(self, config_file: str = "config.json"):
        self.config_file = config_file
        self.workspaces: Dict[str, WorkspaceConfig] = {}
        self.active_workspace: Optional[str] = None
        self.load_config()
    
    def load_config(self) -> None:
        """Load configuration from file."""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                self.active_workspace = data.get('active_workspace')
                
                # Load workspaces
                for ws_id, ws_data in data.get('workspaces', {}).items():
                    # Convert data agents dict back to DataAgent objects
                    agents = {}
                    for agent_name, agent_data in ws_data.get('data_agents', {}).items():
                        agents[agent_name] = DataAgent(**agent_data)
                    
                    ws_data['data_agents'] = agents
                    self.workspaces[ws_id] = WorkspaceConfig(**ws_data)
                
                print(f"✅ Configuration loaded from {self.config_file}")
            except Exception as e:
                print(f"⚠️ Error loading config: {e}")
                self._create_default_config()
        else:
            self._create_default_config()
    
    def save_config(self) -> None:
        """Save configuration to file."""
        try:
            data = {
                'active_workspace': self.active_workspace,
                'workspaces': {}
            }
            
            # Convert workspaces to serializable format
            for ws_id, workspace in self.workspaces.items():
                ws_dict = asdict(workspace)
                # Convert DataAgent objects to dicts
                ws_dict['data_agents'] = {
                    name: asdict(agent) for name, agent in workspace.data_agents.items()
                }
                data['workspaces'][ws_id] = ws_dict
            
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Configuration saved to {self.config_file}")
        except Exception as e:
            print(f"❌ Error saving config: {e}")
    
    def _create_default_config(self) -> None:
        """Create default configuration."""
        print(f"📝 Creating default configuration at {self.config_file}")
        # Start with empty config - user will add workspaces
        self.workspaces = {}
        self.active_workspace = None
        self.save_config()
    
    def add_workspace(self, workspace_id: str, workspace_name: str, tenant_id: str, 
                     description: str = "", lakehouse_id: str = None, 
                     lakehouse_name: str = None) -> WorkspaceConfig:
        """Add a new workspace configuration."""
        workspace = WorkspaceConfig(
            workspace_id=workspace_id,
            workspace_name=workspace_name,
            tenant_id=tenant_id,
            description=description,
            lakehouse_id=lakehouse_id,
            lakehouse_name=lakehouse_name
        )
        
        self.workspaces[workspace_id] = workspace
        
        # Set as active if first workspace
        if not self.active_workspace:
            self.active_workspace = workspace_id
        
        self.save_config()
        print(f"✅ Added workspace: {workspace_name} ({workspace_id})")
        return workspace
    
    def set_active_workspace(self, workspace_id: str) -> bool:
        """Set the active workspace."""
        if workspace_id in self.workspaces:
            self.active_workspace = workspace_id
            self.save_config()
            workspace = self.workspaces[workspace_id]
            print(f"✅ Active workspace set to: {workspace.workspace_name}")
            return True
        else:
            print(f"❌ Workspace {workspace_id} not found")
            return False
    
    def get_active_workspace(self) -> Optional[WorkspaceConfig]:
        """Get the currently active workspace."""
        if self.active_workspace:
            return self.workspaces.get(self.active_workspace)
        return None
    
    def add_data_agent(self, name: str, description: str = "", notebook_path: str = None,
                      workspace_id: str = None) -> DataAgent:
        """Add a new data agent to the specified or active workspace."""
        target_workspace_id = workspace_id or self.active_workspace
        
        if not target_workspace_id or target_workspace_id not in self.workspaces:
            raise ValueError("No active workspace or invalid workspace specified")
        
        workspace = self.workspaces[target_workspace_id]
        
        # Generate Python path based on notebook path
        python_path = None
        if notebook_path:
            python_path = notebook_path.replace('.ipynb', '.py').replace('notebooks/', 'fabric_notebooks/')
        
        agent = DataAgent(
            name=name,
            description=description,
            notebook_path=notebook_path,
            python_path=python_path
        )
        
        workspace.add_data_agent(agent)
        self.save_config()
        
        print(f"✅ Added data agent: {name} to workspace {workspace.workspace_name}")
        return agent
    
    def update_data_agent(self, agent_name: str, **updates) -> bool:
        """Update data agent properties."""
        workspace = self.get_active_workspace()
        if not workspace:
            print("❌ No active workspace")
            return False
        
        agent = workspace.get_data_agent(agent_name)
        if not agent:
            print(f"❌ Data agent '{agent_name}' not found")
            return False
        
        # Update properties
        for key, value in updates.items():
            if hasattr(agent, key):
                setattr(agent, key, value)
        
        agent.last_updated = datetime.now().isoformat()
        self.save_config()
        
        print(f"✅ Updated data agent: {agent_name}")
        return True
    
    def check_prerequisites(self, agent_name: str, operation: str) -> bool:
        """Check if prerequisites are met for an operation."""
        workspace = self.get_active_workspace()
        if not workspace:
            print("❌ No active workspace configured")
            return False
        
        agent = workspace.get_data_agent(agent_name)
        if not agent:
            print(f"❌ Data agent '{agent_name}' not found")
            return False
        
        missing = []
        
        # Check prerequisites based on operation
        if operation == "convert":
            if not agent.notebook_path:
                missing.append("notebook_path")
        
        elif operation == "upload":
            if not agent.python_path or not os.path.exists(agent.python_path):
                missing.append("python_path (run convert first)")
        
        elif operation == "create_agent":
            if not agent.notebook_id:
                missing.append("notebook_id (upload notebook first)")
        
        elif operation == "test":
            if not agent.test_url:
                missing.append("test_url (publish agent first)")
        
        if missing:
            print(f"❌ Missing prerequisites for {operation}:")
            for item in missing:
                print(f"   - {item}")
            return False
        
        return True
    
    def show_status(self) -> None:
        """Show current configuration status."""
        print("\n" + "="*60)
        print("FABRIC NOTEBOOK UPLOADER - CONFIGURATION STATUS")
        print("="*60)
        
        if not self.workspaces:
            print("❌ No workspaces configured")
            print("\nNext steps:")
            print("1. Add a workspace: config.add_workspace(...)")
            return
        
        # Show active workspace
        active_ws = self.get_active_workspace()
        if active_ws:
            print(f"🏢 Active Workspace: {active_ws.workspace_name}")
            print(f"   ID: {active_ws.workspace_id}")
            print(f"   Tenant: {active_ws.tenant_id}")
            if active_ws.lakehouse_id:
                print(f"   Lakehouse: {active_ws.lakehouse_name} ({active_ws.lakehouse_id})")
        else:
            print("❌ No active workspace")
        
        # Show all workspaces
        print(f"\n📂 Available Workspaces ({len(self.workspaces)}):")
        for ws_id, workspace in self.workspaces.items():
            active_marker = "🟢" if ws_id == self.active_workspace else "⚫"
            agent_count = len(workspace.data_agents)
            print(f"   {active_marker} {workspace.workspace_name} ({agent_count} agents)")
        
        # Show data agents in active workspace
        if active_ws and active_ws.data_agents:
            print(f"\n🤖 Data Agents in {active_ws.workspace_name}:")
            for name, agent in active_ws.data_agents.items():
                stage = agent.get_lifecycle_stage()
                status_emoji = ["📝", "📤", "🤖", "🚀", "✅"][stage - 1]
                print(f"   {status_emoji} {name} (Stage {stage}/5: {agent.status})")
                
                missing = agent.get_missing_config()
                if missing:
                    print(f"      ⚠️ Missing: {', '.join(missing)}")
        
        elif active_ws:
            print(f"\n📝 No data agents in {active_ws.workspace_name}")
            print("   Next steps: Add a data agent with config.add_data_agent(...)")
        
        print("\n" + "="*60)
    
    def get_env_vars(self) -> Dict[str, str]:
        """Get environment variables for the active configuration."""
        workspace = self.get_active_workspace()
        if not workspace:
            return {}
        
        return {
            "TENANT_ID": workspace.tenant_id,
            "WORKSPACE_ID": workspace.workspace_id
        }
    
    def get_data_agent_test_url(self, agent_name: str) -> Optional[str]:
        """Get the test URL for a data agent."""
        workspace = self.get_active_workspace()
        if not workspace:
            return None
        
        agent = workspace.get_data_agent(agent_name)
        if not agent:
            return None
        
        return agent.test_url


# Global config instance
config = ConfigManager()


def show_config():
    """Show current configuration status."""
    config.show_status()


def add_workspace(workspace_id: str, workspace_name: str, tenant_id: str, 
                 description: str = "", lakehouse_id: str = None, 
                 lakehouse_name: str = None):
    """Add a new workspace configuration."""
    return config.add_workspace(workspace_id, workspace_name, tenant_id, 
                               description, lakehouse_id, lakehouse_name)


def add_data_agent(name: str, description: str = "", notebook_path: str = None):
    """Add a new data agent to the active workspace."""
    return config.add_data_agent(name, description, notebook_path)


def set_active_workspace(workspace_id: str):
    """Set the active workspace."""
    return config.set_active_workspace(workspace_id)


if __name__ == "__main__":
    # Show current status when run directly
    show_config()