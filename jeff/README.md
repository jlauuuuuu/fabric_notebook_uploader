# Data Agent: jeff

Created: 2025-10-10 12:00:38
Folder: jeff

## Files

- `config.json` - Basic configuration (optional for reference)
- `jeff.ipynb` - Complete data agent notebook with embedded configuration
- `README.md` - This file

## Next Steps

1. Open `jeff.ipynb` in your editor or Fabric workspace
2. Update the configuration section in the notebook:
   - Set your `lakehouse_name`
   - Update `table_names` list
   - Customize the instructions section
   - Add your data source notes
   - Update few-shot examples

## Features

The generated notebook includes:
- ✅ Complete data agent creation workflow
- ✅ Embedded configuration (no external files needed)
- ✅ Template sections for instructions and examples
- ✅ Helper functions for common SDK issues
- ✅ Ready to run in Fabric workspace

## Usage

1. Upload `jeff.ipynb` to your Fabric workspace
2. Edit the configuration cells as needed
3. Run all cells to create your data agent

## Integration

For use with fabric_notebook_uploader workflow:
```bash
python main.py jeff      # Upload notebook to Fabric
python test_run.py jeff  # Execute in Fabric
```
