import json
import os
from pathlib import Path

def convert_ipynb_to_fabric_python(
    ipynb_file_path: str, 
    output_file_path: str = None,
    workspace_id: str = None,
    lakehouse_id: str = None,
    lakehouse_name: str = None,
    include_lakehouse_metadata: bool = False
) -> str:
    """
    Convert a Jupyter notebook (.ipynb) to Microsoft Fabric's Python format.
    
    Args:
        ipynb_file_path: Path to the input .ipynb file
        output_file_path: Path for output .py file (optional, defaults to same name with .py extension)
        workspace_id: Fabric workspace ID (required if include_lakehouse_metadata=True)
        lakehouse_id: Fabric lakehouse ID (required if include_lakehouse_metadata=True)
        lakehouse_name: Fabric lakehouse name (required if include_lakehouse_metadata=True)
        include_lakehouse_metadata: Whether to include lakehouse connection metadata
    
    Returns:
        String containing the Fabric Python format content
    """
    
    # Read the .ipynb file
    with open(ipynb_file_path, "r", encoding="utf-8") as f:
        notebook_data = json.load(f)
    
    # Start building the Fabric Python content
    fabric_content = []
    
    # Add header
    fabric_content.append("# Fabric notebook source\n\n")
    fabric_content.append("# METADATA ********************\n\n")
    fabric_content.append("# META {\n")
    fabric_content.append("# META   \"kernel_info\": {\n")
    fabric_content.append("# META     \"name\": \"synapse_pyspark\"\n")
    
    # Add lakehouse metadata if requested
    if include_lakehouse_metadata and all([workspace_id, lakehouse_id, lakehouse_name]):
        fabric_content.append("# META   },\n")
        fabric_content.append("# META   \"dependencies\": {\n")
        fabric_content.append("# META     \"lakehouse\": {\n")
        fabric_content.append(f"# META       \"default_lakehouse\": \"{lakehouse_id}\",\n")
        fabric_content.append(f"# META       \"default_lakehouse_name\": \"{lakehouse_name}\",\n")
        fabric_content.append(f"# META       \"default_lakehouse_workspace_id\": \"{workspace_id}\",\n")
        fabric_content.append("# META     }\n")
    
    fabric_content.append("# META   }\n")
    fabric_content.append("# META }\n\n")
    
    # Process each cell
    for cell in notebook_data.get('cells', []):
        if cell.get("cell_type") == "code":
            # Check if it's a parameters cell
            is_param_cell = False
            try:
                tags = cell.get("metadata", {}).get("tags", [])
                is_param_cell = "parameters" in tags
            except:
                pass
            
            # Get cell source
            source_lines = cell.get("source", [])
            if not source_lines:
                continue
            
            first_line = source_lines[0] if source_lines else ""
            
            # Add cell header
            if is_param_cell:
                fabric_content.append("# PARAMETERS CELL ********************\n\n")
            else:
                fabric_content.append("# CELL ********************\n\n")
            
            # Handle different cell types
            if first_line.startswith("%%sql"):
                # SQL cell
                for line in source_lines:
                    fabric_content.append(f"# MAGIC {line}")
                fabric_content.append("\n\n")
                fabric_content.append("# METADATA ********************\n\n")
                fabric_content.append("# META {\n")
                fabric_content.append("# META   \"language\": \"sparksql\",\n")
                fabric_content.append("# META   \"language_group\": \"synapse_pyspark\"\n")
                fabric_content.append("# META }\n\n")
                
            elif first_line.startswith("%%configure"):
                # Configure cell
                for line in source_lines:
                    fabric_content.append(f"# MAGIC {line}")
                fabric_content.append("\n\n")
                fabric_content.append("# METADATA ********************\n\n")
                fabric_content.append("# META {\n")
                fabric_content.append("# META   \"language\": \"python\",\n")
                fabric_content.append("# META   \"language_group\": \"synapse_pyspark\"\n")
                fabric_content.append("# META }\n\n")
                
            elif first_line.startswith("%%"):
                # Other magic commands
                for line in source_lines:
                    fabric_content.append(f"# MAGIC {line}")
                fabric_content.append("\n\n")

            elif first_line.startswith("%"):
                # Single-line magic commands (like %pip, %conda, etc.)
                for line in source_lines:
                    fabric_content.append(f"# MAGIC {line}")
                fabric_content.append("\n\n")
                fabric_content.append("# METADATA ********************\n\n")
                fabric_content.append("# META {\n")
                fabric_content.append("# META   \"language\": \"python\",\n")
                fabric_content.append("# META   \"language_group\": \"synapse_pyspark\"\n")
                fabric_content.append("# META }\n\n")
                
            else:
                # Regular Python code
                for line in source_lines:
                    fabric_content.append(line)
                fabric_content.append("\n\n")
                fabric_content.append("# METADATA ********************\n\n")
                fabric_content.append("# META {\n")
                fabric_content.append("# META   \"language\": \"python\",\n")
                fabric_content.append("# META   \"language_group\": \"synapse_pyspark\"\n")
                fabric_content.append("# META }\n\n")
                
        elif cell.get("cell_type") == "markdown":
            # Markdown cell
            fabric_content.append("# MARKDOWN ********************\n\n")
            for line in cell.get("source", []):
                fabric_content.append(f"# {line}")
            fabric_content.append("\n\n")
    
    # Join all content
    result = "".join(fabric_content)
    
    # Remove trailing newlines
    result = result.rstrip() + "\n"
    
    # Save to file if output path provided
    if output_file_path:
        os.makedirs(os.path.dirname(output_file_path) if os.path.dirname(output_file_path) else ".", exist_ok=True)
        with open(output_file_path, "w", encoding="utf-8") as f:
            f.write(result)
    
    return result