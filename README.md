# Fabric Notebook Uploader

A Python package for uploading and running Jupyter notebooks (.ipynb) in Microsoft Fabric workspaces.

## Features

- Convert Jupyter notebooks to Fabric Python format
- Upload notebooks to Microsoft Fabric workspaces  
- Run notebooks via both SDK and direct API approaches
- Handle magic commands and environment setup
- Monitor execution status with runtime tracking

## Main Scripts

- **`main.py`** - Convert and upload notebooks to Fabric workspace
- **`test_run.py`** - Test runner for executing notebooks using msfabricpysdkcore SDK

## Module Structure

- **`fabric_notebook_uploader/`** - Main module containing core functionality
  - `convert_nb.py` - Notebook conversion utilities
  - `create_nb.py` - Notebook creation functions  
  - `run_nb.py` - Notebook execution functions

## Usage

1. Install dependencies: `pip install -r requirements.txt`
2. Convert and upload: `python main.py`
3. Test notebook execution: `python test_run.py`

## Debugging Tools

The `debugging/` folder contains additional utilities:
- `direct_api_run.py` - Direct REST API implementation for troubleshooting
- `create_clean_notebook.py` - Remove %pip commands from notebooks
- `generate_notebook.py` - Generate sample test notebooks