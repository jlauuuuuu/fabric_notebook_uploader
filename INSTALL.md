# DAD-FW Installation Guide

## Option 1: Install from Source (Development)

```bash
# Clone and install in development mode
git clone https://github.com/jlauuuuuu/fabric_notebook_uploader.git
cd fabric_notebook_uploader
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -e .
```

## Option 2: Install from Built Package (Clean)

```bash
# Create new virtual environment anywhere
python -m venv dad-fw-env
dad-fw-env\Scripts\activate  # Windows

# Install from wheel file
pip install /path/to/fabric_notebook_uploader/dist/dad_fw-1.0.0-py3-none-any.whl

# Or install from source tarball
pip install /path/to/fabric_notebook_uploader/dist/dad-fw-1.0.0.tar.gz
```

## Option 3: Install from Git (Direct)

```bash
# Create new virtual environment
python -m venv dad-fw-env
dad-fw-env\Scripts\activate

# Install directly from GitHub
pip install git+https://github.com/jlauuuuuu/fabric_notebook_uploader.git
```

## Usage After Installation

Once installed, you can use `dad-fw` from anywhere:

```bash
# Works from any directory
dad-fw --help
dad-fw init my-agent
dad-fw compile my-agent
dad-fw upload my-agent
dad-fw run my-agent
```

## Clean Installation Benefits

- **No source files visible**: Only the package is installed in site-packages
- **Use from anywhere**: CLI available globally in the virtual environment
- **Clean workspace**: Your working directory only contains your agents
- **Professional deployment**: Distribute as a wheel file

## Package Structure

When installed, the package provides:
- `dad-fw` CLI command
- All templates included
- All dependencies managed automatically
- Clean namespace in `site-packages/dad_fw/`