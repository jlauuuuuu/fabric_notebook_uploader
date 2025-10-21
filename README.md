# Data Agent Development Framework (DAD-FW)

CLI toolkit for creating, managing, and deploying data agent notebooks in Microsoft Fabric (and testing locally).

Combines the usage of the following tools
- 
- [**Fabric Data Agent External Client**](https://github.com/microsoft/fabric_data_agent_client)
- [**Fabric Data Agent SDK**](https://pypi.org/project/fabric-data-agent-sdk/)
- [**Fabric REST API**](https://learn.microsoft.com/en-us/rest/api/fabric/articles/)
- [**Microsoft Fabric SDK**](https://github.com/DaSenf1860/ms-fabric-sdk-core) - note this one is not by Microsoft...
## Features

- **Template-based scaffolding**: Create data agents from pre-configured templates
- **Direct Fabric integration**: Compile notebooks into a Fabric readable format and upload to target workspaces
- **Automated Creation and Publishing**: Create Fabric Data Agents from Fabric notebooks run from your local machine. 
- **Evaluations and Testing**: Test your Data Agent from you local machine with the Fabric Data Agent External Client

## Installation

### Prerequisites
- Python 3.11+
- Azure CLI installed and configured (`az login`)
- Microsoft Fabric workspace access
- You must add a default environment in your workspace that has the fabric-data-agent-sdk installed. (For some reason I can't magic install libraries in my notebooks without adding a setting that I can't find atm.You can **NOT** use a %pip install command in the notebook.)

### User Install
If you plan to use the tool install this way
```bash
# Create isolated environment
python -m venv .venv
.venv\Scripts\activate  # Windows

# Install from GitHub
pip install git+https://github.com/jlauuuuuu/fabric_notebook_uploader.git
# or
pixi add --pypi "dad-fw @ git+https://github.com/jlauuuuuu/fabric_notebook_uploader.git"

# Verify installation
dad-fw --help
```


### Development Install
If you want to add stuff to the tool then install this way
```bash
git clone https://github.com/jlauuuuuu/fabric_notebook_uploader.git
cd fabric_notebook_uploader
python -m venv .venv
.venv\Scripts\activate
pip install -e .
```

## Workflow/Usage

```bash
# Create new data agent
dad-fw init my-agent # See configuration heading for more instructions after this step

# Compile to Fabric format
dad-fw compile my-agent # This compiles the .ipynb notebook into a .py file for Fabric upload.

# Upload to Fabric workspace
dad-fw upload my-agent

# Run in Fabric
dad-fw run my-agent
```
**Note**: Testing is done via a created notebook in the agent directory...

## Configuration

Set up your config file for each agent by filling these settings

```json
{
  "workspace_id": "your-fabric-workspace-id",
  "tenant_id": "your-azure-tenant-id"
}
```

### Authentication
```bash
# One-time Azure CLI login
az login

# Verify access
az account show
```

## Project Structure

After running `dad-fw init my-agent`, you'll get:

```
my-agent/
├── config.json              # Agent configuration
├── README.md                 # Agent documentation
├── my-agent.ipynb           # Main data agent notebook
└── testing.ipynb           # Testing framework
```

## Setup with Sample Data
You can download the .csv files from the Sample Data Folder and upload to a lakehouse if you want to be able to run and test the default configured Data Agent in the template following the below steps.

1. Download the 5 .csv files from the Sample Data Folder
2. Go to your Fabric Workspace and create a lakehouse called  ```DataAgentDefaultLH```
3. Upload your .csv files
4. Go to Lakehouse Files and turn them into Tables
4. Done! You can now run the default template notebook yay


## License

This project is licensed under the MIT License
