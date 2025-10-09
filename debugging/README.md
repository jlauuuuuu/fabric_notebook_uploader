# Debugging Tools

This directory contains utility scripts for debugging and direct API testing:

## Scripts

- **`direct_api_run.py`** - Direct Fabric REST API calls (bypasses SDK)
- **`create_clean_notebook.py`** - Generate clean test notebooks
- **`generate_notebook.py`** - Notebook generation utilities

## Usage

These tools are primarily for debugging SDK issues or testing direct API functionality:

```bash
# Test direct API without SDK
python debugging/direct_api_run.py

# Generate clean test notebooks
python debugging/create_clean_notebook.py
```

These scripts were essential for troubleshooting and are kept for reference.