# Data Agent: {agent_name}

Created: {created_date}
Folder: {folder_name}

## Files

- `config.json` - Agent configuration (add your workspace_id here)
- `{folder_name}.ipynb` - Main data agent notebook
- `{folder_name}_testing.ipynb` - Testing notebook for deployed agent
- `README.md` - This file

## Setup

1. Edit `config.json` and add your workspace_id:
   ```json
   {{
     "workspace_id": "your-fabric-workspace-id-here"
   }}
   ```

2. Compile and upload:
   ```bash
   dad-fw compile {folder_name}
   dad-fw upload {folder_name}
   dad-fw run {folder_name}
   ```

## Next Steps

1. Open `{folder_name}.ipynb` and update the configuration cells
2. Customize lakehouse_name, table_names, instructions, and examples
3. Use dad-fw commands for automated workflow
4. Test with `{folder_name}_testing.ipynb` after deployment