# Data Agent: jeff

Created: 2025-10-22 09:48:02
Folder: jeff

## Files

- `config.json` - Agent configuration (add your workspace_id here)
- `jeff.ipynb` - Main data agent notebook
- `jeff_testing.ipynb` - Testing notebook for deployed agent
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
   dad-fw compile jeff
   dad-fw upload jeff
   dad-fw run jeff
   ```

## Next Steps

1. Open `jeff.ipynb` and update the configuration cells
2. Customize lakehouse_name, table_names, instructions, and examples
3. Use dad-fw commands for automated workflow
4. Test with `jeff_testing.ipynb` after deployment