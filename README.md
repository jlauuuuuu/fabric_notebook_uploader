# Fabric Notebook Uploader

A comprehensive Python toolkit for creating, compiling, uploading, and running data agent notebooks in Microsoft Fabric. Features template-based scaffolding, automated notebook management, and complete workflow automation.

## 🚀 Features

- **� Template-Based Scaffolding**: Create data agent notebooks from templates with embedded configuration
- **🔄 Complete Workflow Pipeline**: Create → Compile → Upload → Run automation
- **� Notebook Conversion**: Convert `.ipynb` files to Fabric Python format
- **☁️ Fabric Integration**: Direct upload and execution in Fabric workspaces
- **📊 Execution Monitoring**: Real-time job status tracking with detailed reporting
- **🖥️ Remote Execution**: Run Fabric notebooks from your local machine with full monitoring
- **🔧 Configuration Management**: Centralized workspace and agent lifecycle tracking
- **🔄 Smart Update Mode**: Update existing notebooks to avoid name availability timing issues
- **⚡ Immediate Execution**: No waiting periods after updates - run notebooks right away

## 📋 Prerequisites

- Python 3.7+
- Microsoft Fabric workspace access
- Azure Active Directory authentication

## ⚡ Complete Workflow

### 1. Create Data Agent
```bash
python create_data_agent.py <agent_name>
```
Creates a new data agent folder with notebook from template.

### 2. Compile to Fabric Format  
```bash
python compile_script.py <agent_name>
```
Converts the notebook to Fabric Python format.

### 3. Upload to Fabric Workspace
```bash
python upload_agent.py <agent_name>
```
Uploads the compiled Python file as a notebook to Fabric.

### 4. Run in Fabric (Creates Data Agent)
```bash
python run_fabric_notebook.py <agent_name>
```
Executes the notebook remotely in Fabric from your local machine to create the data agent.

### 🔄 **Update Workflow (Avoids Timing Issues)**
For existing agents, use the update mode to avoid name availability delays:

```bash
# Recompile after changes
python compile_script.py <agent_name>

# Update existing notebook (no timing conflicts!)
python upload_agent.py <agent_name> --update

# Run immediately (no waiting!)
python run_fabric_notebook.py <agent_name>
```

## 🔥 Quick Workflow Example
```bash
# Create a new data agent named "sales_bot"
python create_data_agent.py sales_bot

# Compile the notebook to Fabric format
python compile_script.py sales_bot

# Upload to Fabric workspace (creates new notebook)
python upload_agent.py sales_bot

# Run in Fabric to create the data agent
python run_fabric_notebook.py sales_bot

# ✨ FOR UPDATES (No timing issues!):
python compile_script.py sales_bot    # Recompile changes
python upload_agent.py sales_bot --update    # Update existing notebook
python run_fabric_notebook.py sales_bot      # Run immediately
```

## � Notebook Compilation

### Option A: Static Compilation (embeds all data in notebook)
```bash
python compile_data_agent.py jeff
```
Creates `jeff/jeff_compiled.ipynb` with all configuration embedded.

### Option B: Dynamic Loading (recommended - loads files at runtime)
```bash
python compile_data_agent_v2.py jeff
```
Creates `jeff/jeff_dynamic.ipynb` that loads configuration files at runtime.

**Advantages of Dynamic Loading:**
- 📝 Edit instructions without recompiling
- 🔧 Update configuration without code changes  
- 📊 Modify examples and schema info easily
- 🔄 Version control friendly - separate data from code
- ✨ Edit files directly in Fabric workspace

**Usage with Dynamic Loading:**
1. Upload both the notebook AND the agent folder to Fabric
2. Edit text files directly in the Fabric workspace
3. Re-run cells to apply changes - no recompilation needed!

### Compilation Method Comparison

| Feature | Static Compilation | Dynamic Loading |
|---------|-------------------|-----------------|
| **File Size** | Larger (embedded data) | Smaller (references files) |
| **Setup** | Upload notebook only | Upload notebook + folder |
| **Editing** | Recompile required | Direct file editing |
| **Version Control** | Mixed code+data | Clean separation |
| **Flexibility** | Fixed at compile time | Runtime flexibility |
| **Best For** | Simple, one-time agents | Iterative development |

## �🔄 Complete Workflow

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
├── 📄 setup_config.py         # One-time interactive configuration setup
└── 📄 create_data_agent.py    # Data agent scaffolding tool (creates folder structure)
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
| `setup_config.py` | Interactive setup for first-time configuration | Step 1 of workflow | ✅ Yes |
| `create_data_agent.py` | Creates data agent folder structure and config files | Step 2 of workflow | ✅ Yes |
| `main.py` | Converts notebooks and uploads to Fabric | Step 3 of workflow | ✅ Yes |
| `test_run.py` | Runs notebooks in Fabric (creates data agents) | Step 4 of workflow | ✅ Yes |
| `config.py` | Manages all configurations and data agent lifecycle | Used by all scripts | ✅ Yes |
| `test_agent.py` | Tests deployed data agents locally | Step 7 of workflow | ❌ Optional |
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