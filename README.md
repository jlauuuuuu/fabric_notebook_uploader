# Fabric Notebook Uploader

A comprehensive Python toolkit for managing Jupyter notebooks and data agents in Microsoft Fabric. Features centralized configuration management, automated lifecycle tracking, and local testing capabilities.

## 🚀 Features

- **🔧 Configuration Management**: Centralized workspace and data agent lifecycle tracking
- **📓 Notebook Conversion**: Convert `.ipynb` files to Fabric Python format
- **☁️ Fabric Integration**: Direct upload and execution in Fabric workspaces
- **🧪 Local Testing**: Test data agents locally before deployment
- **📊 Lifecycle Tracking**: 5-stage progression from notebook to deployed agent

## 📋 Prerequisites

- Python 3.7+
- Microsoft Fabric workspace access
- Azure Active Directory authentication

## ⚡ Quick Start

### 1. Installation
```bash
git clone <repository-url>
cd fabric_notebook_uploader
pip install -r requirements.txt
```

### 2. Initial Setup
```bash
python setup_config.py
```
Follow the interactive prompts to configure your workspace and first data agent.

### 3. Basic Usage
```bash
# 1. Upload notebook to Fabric
python main.py [data_agent_name]

# 2. Run notebook in Fabric (creates data agent)
python test_run.py [data_agent_name]  

# 3. Get published URL from Fabric and update config
# 4. Test data agent locally
python example.py [data_agent_name]
```

### 🔥 Quick Workflow Summary
```bash
# One-time setup
python setup_config.py

# For each data agent:
python main.py my_agent        # Upload to Fabric  
python test_run.py my_agent    # Run in Fabric (creates agent)
# → Get URL from Fabric → Update config
python test_agent.py my_agent  # Test locally (optional)
```

## 🔄 Complete Workflow

### Step 1: Initial Configuration
```bash
python setup_config.py
```
- Configure your Fabric workspace (workspace ID, tenant ID)
- Add lakehouse information (optional)
- Set up your first data agent with notebook path

### Step 2: Upload Notebook to Fabric
```bash
python main.py your_agent_name
```
This will:
- Convert `.ipynb` to Fabric Python format
- Save converted file locally
- Upload notebook to your Fabric workspace
- Update config with notebook ID

### Step 3: Execute Notebook in Fabric (Creates Data Agent)
```bash
python test_run.py your_agent_name
```
This will:
- Run the uploaded notebook in Fabric workspace
- The notebook execution creates the data agent automatically
- Update agent status to "notebook_executed"

### Step 4: Get Published Data Agent URL
1. Go to your Fabric workspace
2. Navigate to the Data Agents section
3. Find your newly created data agent
4. Copy the published API URL

### Step 5: Update Configuration with Test URL
```python
from config import config
config.update_data_agent("your_agent_name",
    test_url="https://api.fabric.microsoft.com/v1/workspaces/.../datamarts/.../executeQuery",
    status="agent_deployed"
)
```

### Step 6: Test Data Agent Locally (Optional)
```bash
python test_agent.py your_agent_name
```
- Test your deployed data agent with basic questions
- Validate responses work correctly
- Verify everything works before production use
- This step is optional but recommended for validation

## 📊 Data Agent Lifecycle

Each data agent progresses through 5 tracked stages:

| Stage | Status | Description | Script Used |
|-------|--------|-------------|-------------|
| 1 | `notebook_created` | Notebook file exists locally | Initial setup |
| 2 | `notebook_uploaded` | Uploaded to Fabric workspace | `main.py` |
| 3 | `notebook_executed` | Notebook run in Fabric (creates data agent) | `test_run.py` |  
| 4 | `agent_deployed` | Data agent published with URL | Manual + config update |
| 5 | `tested` | Locally tested and validated | `example.py` |

## ⚙️ Configuration System

### Workspace Management
- Support for multiple Fabric workspaces
- Tenant and authentication configuration
- Lakehouse integration settings

### Configuration API
```python
from config import config

# Add workspace
config.add_workspace(
    workspace_name="Production",
    workspace_id="workspace-guid",
    tenant_id="tenant-guid",
    lakehouse_id="lakehouse-guid",  # optional
    lakehouse_name="DataLakehouse"  # optional
)

# Add data agent
config.add_data_agent(
    agent_name="sales_agent",
    notebook_path="notebooks/sales_analysis.ipynb",
    python_path="output/sales_agent.py"  # optional
)

# Update with deployment info
config.update_data_agent("sales_agent", 
    agent_id="agent-guid",
    test_url="https://api.fabric.microsoft.com/...",
    status="agent_deployed"
)

# Switch workspace
config.set_active_workspace("Production")
```

## 🧪 Data Agent Testing

### Environment Setup
Create `.env` file in `fabric_notebook_uploader/` directory:
```env
TENANT_ID=your-azure-tenant-id
DATA_AGENT_URL=your-fabric-data-agent-url
```

### Testing Features
- **Quick Tests**: Simple question-answer validation
- **Comprehensive Tests**: Full test suite with multiple scenarios  
- **SQL Analysis**: Extract and analyze generated SQL queries
- **Performance Metrics**: Response time and success rate tracking
- **Test Reports**: Detailed JSON reports with analysis

### Testing Commands
```bash
# Quick test with a single question
python -c "from fabric_notebook_uploader import run_quick_test; run_quick_test('What data is available?')"

# Comprehensive test suite
python -c "from fabric_notebook_uploader import run_comprehensive_test; run_comprehensive_test()"

# Interactive testing
python example.py [agent_name]
```

## 📋 Command Reference

| Command | Description | Stage Required | Result | Required |
|---------|-------------|----------------|--------|----------|
| `python setup_config.py` | Interactive configuration setup | None | Stage 1 | ✅ Yes |
| `python main.py [agent]` | Convert and upload notebook to Fabric | Stage 1+ | Stage 2 | ✅ Yes |
| `python test_run.py [agent]` | Execute notebook in Fabric (creates data agent) | Stage 2+ | Stage 3 | ✅ Yes |
| `python test_agent.py [agent]` | Test deployed data agent locally | Stage 4+ | Stage 5 | ❌ Optional |
| `python check_config.py` | View current configuration status | Any | Status report | ❌ Optional |

## 📁 Project Structure & File Purposes

### 🔧 Core Scripts (Required)
```
├── 📄 main.py                 # Convert .ipynb → Fabric format & upload to workspace
├── 📄 test_run.py             # Execute notebook in Fabric (creates data agent)
├── 📄 config.py               # Global configuration management system
└── 📄 setup_config.py         # One-time interactive configuration setup
```

### 🛠️ Helper Scripts (Optional)
```
├── 📄 test_agent.py           # Test deployed data agents locally
├── 📄 check_config.py         # View current configuration status
└── 📄 requirements.txt        # Python dependencies
```

### 📦 Core Package
```
├── 📁 fabric_notebook_uploader/  # Core functionality modules
│   ├── 📄 __init__.py         # Package exports and convenience functions
│   ├── 📄 convert_nb.py       # Jupyter → Fabric Python conversion
│   ├── 📄 create_nb.py        # Upload notebooks to Fabric workspace
│   ├── 📄 run_nb.py           # Execute notebooks in Fabric
│   ├── 📄 data_agent_client.py # External data agent testing client
│   └── 📄 test_agent.py       # Local data agent testing framework
```

### 📁 Data Directories
```
├── 📁 notebooks/              # Your Jupyter notebooks (.ipynb files)
├── 📁 output/                 # Generated Fabric Python files (.py files)
└── 📁 test_results/           # Data agent test reports (auto-created)
```

### 📋 Detailed File Descriptions

| File | Purpose | When to Use | Required |
|------|---------|-------------|----------|
| `main.py` | Converts notebooks and uploads to Fabric | Step 2 of workflow | ✅ Yes |
| `test_run.py` | Runs notebooks in Fabric (creates data agents) | Step 3 of workflow | ✅ Yes |
| `config.py` | Manages all configurations and data agent lifecycle | Used by all scripts | ✅ Yes |
| `setup_config.py` | Interactive setup for first-time configuration | Step 1 of workflow | ✅ Yes |
| `test_agent.py` | Tests deployed data agents locally | Step 6 of workflow | ❌ Optional |
| `check_config.py` | Shows current configuration status | Troubleshooting | ❌ Optional |

### 🎯 Essential vs Optional
- **Essential (4 files)**: `setup_config.py`, `main.py`, `test_run.py`, `config.py`
- **Optional (2 files)**: `test_agent.py`, `check_config.py`

The core workflow only requires 4 files. Testing and configuration checking are optional but helpful for validation and troubleshooting.

## 🔧 Troubleshooting

### Common Issues

**Configuration not found:**
```bash
# Run setup first
python setup_config.py
```

**Notebook file not found:**
```python
# Update path in configuration
from config import config
config.update_data_agent("agent_name", notebook_path="correct/path.ipynb")
```

**Authentication errors:**
- Verify `.env` file contains correct `TENANT_ID`
- Ensure you have access to the Fabric workspace
- Check that data agent URL is correct and published

**Upload failures:**
- Verify workspace ID is correct
- Check network connectivity
- Ensure you have contributor permissions

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is open source. Please check the license file for details.

---

**Ready to build amazing data agents with Microsoft Fabric! 🚀**