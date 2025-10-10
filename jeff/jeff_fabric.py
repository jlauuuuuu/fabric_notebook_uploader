# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Creating Fabric Data Agent via Code

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": {  
# MAGIC         "name": "data_agent_lh",
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Imports

# CELL ********************

from fabric.dataagent.client import (
    FabricDataAgentManagement,
    create_data_agent,
    delete_data_agent,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Configs for Data Agent Name and Data Sources

# CELL ********************

# Configuration
data_agent_name = "jeff"
lakehouse_name = "data_agent_lh"
table_names = [ "BST10_ACTIVE_PROJECTS"
                ,"TPF_DUMP"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create
data_agent = create_data_agent(data_agent_name)
data_agent = FabricDataAgentManagement(data_agent_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Update the data agent's configuration with detailed instructions for query expansion and retrieve the updated configuration
data_agent.update_configuration(
    instructions= """
    You are a helpful and focused data agent assisting users querying a Microsoft Fabric Lakehouse.

    The Lakehouse contains two tables:
    1. `TPF_DUMP` — time-series financial metrics at the project level (monthly granularity).
    2. `BST10_ACTIVE_PROJECTS` — metadata about active projects (client, start/end dates, etc.).

    These tables are joined using:
    `TPF_DUMP.PROJECT_CODE = BST10_ACTIVE_PROJECTS.Proj:_Project_Code`

    Important behavior guidelines:
    - Respond only to questions answerable by the data. If a question is ambiguous or not supported, ask for clarification.
    - Format your SQL queries in **T-SQL** syntax, compatible with Microsoft Fabric.
    - Use the join only when required (e.g., when project name or client name is requested).
    - Fiscal period (`FISCPER`) is in `YYYYMM` format and **shifted 6 months forward**. For example, `202304` represents **October 2022**. Consider this when calculating dates.
    - Be aware that `TPF_DUMP` may contain null values and scientific notation (e.g., `0E-9`) — handle these gracefully.
    - Prioritize `TD_` prefixed columns (e.g., `TD_BUDGET_EFFORT`, `TD_VARIANCE`) for effort and variance-based calculations.

    Respond with **concise summaries and well-structured SQL**. Avoid overly technical explanations unless requested. Include fiscal period conversion logic in queries where time context is needed.

    Examples of supported question types:
    - What is the total TD budget effort in a given fiscal period?
    - List all projects with a TD variance greater than a threshold.
    - Show WIP balances for the last 3 fiscal periods.
    - Join with project metadata (name, client) when requested.

    Do not make up fields or relationships that are not present in the schema.

    Tables:
    - TPF_DUMP (metrics)
    - BST10_ACTIVE_PROJECTS (project metadata)

    Default join key: `PROJECT_CODE` = `Proj:_Project_Code`

    Make sure when any date is used, you convert it into its fiscal period form. Only use fiscal period form in questions and answers if the format is YYYYMM.
    """,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def safe_get_datasources(): # add_datasource calls get_datasources, which doesn't handle None types.... This is a modified function which handles this
    config = data_agent._client.get_configuration()
    data_sources = config.value.get("dataSources") or []
    return [data_agent._client.get_datasource(ds["id"]) for ds in data_sources]

data_agent._client.get_datasources = safe_get_datasources

data_agent.add_datasource(lakehouse_name, type="lakehouse")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

datasource = data_agent.get_datasources()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table_name in table_names:
    datasource.select("dbo", table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

datasource.pretty_print()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Notes for data source

# CELL ********************

ds_notes ="""
This Lakehouse contains project delivery and financial data from two joined tables. It is used to analyze budget, variance, effort, and WIP metrics across active projects. Data is refreshed monthly and already filtered to only include active projects.

TABLE 1: TPF_DUMP - Time-Series Financial Metrics
Each row represents a project at a specific fiscal period. Contains numeric metrics in string or scientific notation format.

PROJECT_CODE (string): join key

FISCPER (string, format YYYYMM): fiscal period, shifted 6 months into the future (e.g., 202304 = October 2022)

TD_BUDGET_EFFORT (string): to-date budgeted effort

TD_VARIANCE (string): to-date variance

TD_EFF_EFFORT_AT_COMP (string): estimate at completion

WIP_BALANCE, R_BALANCE, VARIANCE (string): financial balances

WIP_AGEING1/2, RB_AGEING1/2/3 (string): aged balances

Notes:

Contains many nulls and "0E-9" (treat as 0)

Use CAST(... AS FLOAT) for aggregations

Use FISCPER for period-based filtering or sorting

TABLE 2: BST10_ACTIVE_PROJECTS - Project Metadata
Each row is a unique active project with descriptive info.

Proj:_Project_Code (string): primary key for joining with TPF_DUMP

Proj:_Project_Name (string): project label

Proj:_Client_Name (string): client or funding agency

Proj:_Start_Date / Proj:_End_Date (date): project duration

Proj:_Service_Line (string): business domain

Proj:_Market_Segment_Code (string): vertical or classification

Proj:_Project_Currency, Proj:_Company_Currency (string): currency codes

Proj:_Manager_Code (string): owner or responsible staff

Proj:_Has_Chargeable, Proj:_Has_Proposal (flag): 1 = true, blank/0 = false

Notes:

All projects are already active

Use for grouping/filtering by client, market, or date range

RELATIONSHIP:
Join TPF_DUMP.PROJECT_CODE = BST10_ACTIVE_PROJECTS.Proj:_Project_Code
(one-to-many, since financials are time-based)

TIPS FOR THE AGENT:

Many numeric fields contain null or "0E-9" → handle with care

Use TD_ columns for effort/variance-related analysis

Use [Proj:_...] columns for metadata grouping or labels

Use FISCPER as the time filter, remembering the 6-month shift

Use CAST for all aggregations on numeric columns

Useful metadata grouping columns:

Proj:_Client_Name

Proj:_Service_Line

Proj:_Market_Segment_Code

Proj:_Start_Date

Proj:_Manager_Code

We can group projects by different details such as by Manager Code, Start Dates etc. pretty much anything in the BST10_ACTIVE_PROJECTS Table
"""


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

datasource.update_configuration(instructions=ds_notes)
datasource.get_configuration()["additional_instructions"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

json_key_pairs_dict = {
    "What is the total TD budget effort across all projects in fiscal period 202304?": "-- Enter SQL query\r\nSELECT \r\n    SUM(CAST(TD_BUDGET_EFFORT AS FLOAT)) AS total_td_budget_effort\r\nFROM TPF_DUMP\r\nWHERE FISCPER = '202304';",
    "List project names and TD variance for all projects with TD variance over 5000 in fiscal period 202305.": "-- Enter SQL query\r\nSELECT \r\n    B.[Proj:_Project_Name],\r\n    A.TD_VARIANCE,\r\n    A.FISCPER\r\nFROM TPF_DUMP A\r\nJOIN BST10_ACTIVE_PROJECTS B\r\n    ON A.PROJECT_CODE = B.[Proj:_Project_Code]\r\nWHERE A.FISCPER = '202305'\r\n  AND CAST(A.TD_VARIANCE AS FLOAT) > 5000;",
    "Show top 5 projects with the highest WIP balance in fiscal period 202304.": "-- Enter SQL query\r\nSELECT TOP 5\r\n    B.[Proj:_Project_Name],\r\n    CAST(A.WIP_BALANCE AS FLOAT) AS wip_balance,\r\n    A.FISCPER\r\nFROM TPF_DUMP AS A\r\nJOIN BST10_ACTIVE_PROJECTS AS B\r\n    ON A.PROJECT_CODE = B.[Proj:_Project_Code]\r\nWHERE A.FISCPER = '202304'\r\nORDER BY CAST(A.WIP_BALANCE AS FLOAT) DESC;",
    "How many active projects are there?": "-- Enter SQL query\r\nSELECT COUNT(*) AS active_project_count FROM BST10_ACTIVE_PROJECTS;",
    "Show all project names and their start and end dates.": "-- Enter SQL query\r\nSELECT \r\n  [Proj:_Project_Name], \r\n  [Proj:_Start_Date], \r\n  [Proj:_End_Date]\r\nFROM BST10_ACTIVE_PROJECTS;",
    "Get the list of distinct fiscal periods available in the dataset.": "-- Enter SQL query\r\nSELECT DISTINCT FISCPER FROM TPF_DUMP ORDER BY FISCPER;",
    "Which projects had a TD variance below zero in fiscal period 202304?": "-- Enter SQL query\r\nSELECT \r\n    B.[Proj:_Project_Name], \r\n    A.TD_VARIANCE\r\nFROM TPF_DUMP A\r\nJOIN BST10_ACTIVE_PROJECTS B\r\n  ON A.PROJECT_CODE = B.[Proj:_Project_Code]\r\nWHERE A.FISCPER = '202304'\r\n  AND CAST(A.TD_VARIANCE AS FLOAT) < 0;",
    "List the top 3 clients by number of active projects.": "-- Enter SQL query\r\nSELECT TOP 3\r\n    [Proj:_Client_Name], \r\n    COUNT(*) AS project_count\r\nFROM BST10_ACTIVE_PROJECTS\r\nGROUP BY [Proj:_Client_Name]\r\nORDER BY project_count DESC;",
    "Find all projects in the 'Wastewater Treatment & Recycling' service line.": "-- Enter SQL query\r\nSELECT \r\n    [Proj:_Project_Name], \r\n    [Proj:_Service_Line]\r\nFROM BST10_ACTIVE_PROJECTS\r\nWHERE [Proj:_Service_Line] = 'Wastewater Treatment & Recycling';",
    "Show total R_BALANCE for fiscal period 202304.": "-- Enter SQL query\r\nSELECT \r\n    SUM(CAST(R_BALANCE AS FLOAT)) AS total_r_balance\r\nFROM TPF_DUMP\r\nWHERE FISCPER = '202304';",
    "Get average TD_EFF_EFFORT_AT_COMP across all projects for fiscal period 202305.": "-- Enter SQL query\r\nSELECT \r\n    AVG(CAST(TD_EFF_EFFORT_AT_COMP AS FLOAT)) AS avg_td_effort\r\nFROM TPF_DUMP\r\nWHERE FISCPER = '202305';",
    "Which projects had a WIP balance greater than 1000 in fiscal period 202304?": "-- Enter SQL query\r\nSELECT \r\n    B.[Proj:_Project_Name], \r\n    A.WIP_BALANCE\r\nFROM TPF_DUMP A\r\nJOIN BST10_ACTIVE_PROJECTS B\r\n  ON A.PROJECT_CODE = B.[Proj:_Project_Code]\r\nWHERE A.FISCPER = '202304'\r\n  AND CAST(A.WIP_BALANCE AS FLOAT) > 1000;",
    "What columns are available in the TPF_DUMP table?": "-- Enter SQL query\r\nSELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TPF_DUMP';",
    "What columns are available in the BST10_ACTIVE_PROJECTS table?": "-- Enter SQL query\r\nSELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'BST10_ACTIVE_PROJECTS';"
}
datasource.add_fewshots(json_key_pairs_dict)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

datasource.get_fewshots()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # delete data agent
# delete_data_agent(data_agent_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
