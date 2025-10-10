# Data Agent Development Framework (DAD-FW)

Professional CLI toolkit for creating, managing, and deploying data agent notebooks in Microsoft Fabric.

## Features

- **Template-based scaffolding**: Create data agents from professional templates
- **Jupyter to Fabric conversion**: Seamless notebook format conversion
- **Direct Fabric integration**: Upload and execute notebooks remotely
- **Clean CLI interface**: Simple, intuitive command structure
- **Configuration management**: Automated workspace and agent tracking
- **Professional packaging**: Installable Python package with dependencies

## Installation

### Prerequisites
- Python 3.8+
- Azure CLI installed and configured (`az login`)
- Microsoft Fabric workspace access

### Option 1: Clean Install
```bash
# Create isolated environment
python -m venv .venv
.venv\Scripts\activate  # Windows

# Install from GitHub
pip install git+https://github.com/jlauuuuuu/fabric_notebook_uploader.git

# Verify installation
dad-fw --help
```

### Option 2: Install from Package
```bash
# Download and install wheel file
pip install dad_fw-1.0.0-py3-none-any.whl
```

### Option 3: Development Install
```bash
git clone https://github.com/jlauuuuuu/fabric_notebook_uploader.git
cd fabric_notebook_uploader
python -m venv .venv
.venv\Scripts\activate
pip install -e .
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

## Quick Start

1. **Initialize a new data agent**:
   ```bash
   dad-fw init sales-analysis --description "Analyzes sales data trends"
   ```

2. **Compile the notebook**:
   ```bash
   dad-fw compile sales-analysis
   ```

3. **Upload to Fabric**:
   ```bash
   dad-fw upload sales-analysis
   ```

4. **Run in Fabric workspace**:
   ```bash
   dad-fw run sales-analysis
   ```

## Commands

| Command | Description | Example |
|---------|-------------|---------|
| `dad-fw init <name>` | Create new data agent with templates | `dad-fw init my-agent -d "Description"` |
| `dad-fw compile <name>` | Convert notebook to Fabric Python format | `dad-fw compile my-agent` |
| `dad-fw upload <name>` | Upload compiled agent to Fabric workspace | `dad-fw upload my-agent` |
| `dad-fw run <name>` | Execute agent in Fabric to create data agent | `dad-fw run my-agent` |
| `dad-fw debug` | Advanced debugging and API commands | `dad-fw debug --help` |

## Configuration

Create `config.json` in your project root:

```json
{
  "workspace_id": "your-fabric-workspace-id",
  "tenant_id": "your-azure-tenant-id",
  "lakehouse_id": "your-lakehouse-id",
  "lakehouse_name": "YourLakehouse"
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

## License

This project is licensed under the MIT License - see below for details.

### What is the MIT License?

The **MIT License** is one of the most permissive and popular open-source licenses. Here's what it means:

#### ✅ **What you CAN do:**
- **Use** the software for any purpose (personal, commercial, etc.)
- **Copy** and distribute the software freely
- **Modify** the source code as needed
- **Merge** it with other projects
- **Publish** your own versions
- **Sell** software that includes this code

#### ⚠️ **What you MUST do:**
- **Include the original license** in any copy/distribution
- **Include the copyright notice** when redistributing

#### ❌ **What we DON'T provide:**
- **No warranty** - software is provided "as is"
- **No liability** - we're not responsible for any issues
- **No support guarantee** - though we try to help

#### 📝 **In simple terms:**
"Feel free to use this however you want, just don't blame us if something goes wrong, and give us credit."

This means you can:
- Use DAD-FW in your company's internal projects
- Build commercial products that include DAD-FW
- Modify it to fit your specific needs
- Share it with others

It's business-friendly and encourages widespread adoption and contribution.
