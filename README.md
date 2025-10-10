# Data Agent Development Framework (DAD-FW)

CLI toolkit for creating and managing data agent notebooks in Microsoft Fabric.

## Features

- Template-based data agent creation
- Jupyter notebook to Fabric Python conversion
- Direct upload and execution in Fabric workspaces
- Clean command-line interface

## Installation

```bash
git clone https://github.com/jlauuuuuu/fabric_notebook_uploader.git
cd fabric_notebook_uploader
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -e .
dad-fw --help  # Verify installation
```

## Usage

```bash
# Create new data agent
dad-fw init my-agent

# Compile to Fabric format
dad-fw compile my-agent

# Upload to Fabric workspace
dad-fw upload my-agent

# Run in Fabric
dad-fw run my-agent
```

## Commands

- `dad-fw init <name>` - Create new data agent
- `dad-fw compile <name>` - Convert notebook to Fabric format
- `dad-fw upload <name>` - Upload to Fabric workspace
- `dad-fw run <name>` - Execute in Fabric

## Configuration

Create `config.json` in your project root:

```json
{
  "workspace_id": "your-fabric-workspace-id",
  "tenant_id": "your-azure-tenant-id"
}
```

Requires Azure CLI authentication: `az login`