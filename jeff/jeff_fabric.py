# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Create Data Agent
# 
# This notebook utilises the Fabric Data Agent SDK to create configure and publish Fabric Data Agents via Code.
# 
# Please refers to examples by Microsoft for more information.
# 
# - https://github.com/microsoft/fabric-samples/tree/main/docs-samples/data-science/data-agent-sdk
# 
# #### Structure
# 0. Imports and Configuring Default Lakehouse Connection - this may be removed later though
# 1. Initialising and Configuring Agent Instructions
# 2. Connecting Data Sources
# 3. Configuring Data Source Instructions and Example Queries
# 4. Publishing Agent


# MARKDOWN ********************

# ## Imports and Configuring Default Lakehouse Connection
# 
# Not sure if this is fully necessary yet... requires a bit more testing but I think this will be removed further along the line

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": {  
# MAGIC         "name": "DataAgentDefaultLH",
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Importing tools from Fabric Data Agent SDK
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

# ## Initialising and Configuring Agent Instructions


# CELL ********************

# Configuration
data_agent_name = "jeff"
data_sources = {
    "lakehouse": ["DataAgentDefaultLH"] # I will add more example data sources later in the future but for now its just Lakehouses cause idk the other type names...
    }
lakehouse_table_names = {
    "DataAgentDefaultLH": ["projects", "employees", "invoices", "clients", "project_tasks"]
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create Data Agent
data_agent = create_data_agent(data_agent_name)
data_agent = FabricDataAgentManagement(data_agent_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Update or specify (if done for the first time) the data agent instructions. Below is a sample...
data_agent.update_configuration(
    instructions= """
    You are a helpful, precise, and context-aware data agent that assists users in querying project-related data for an engineering consulting company.

    Your goal is to translate natural-language questions into valid SQL queries that retrieve relevant information from the available data tables.  
    The data represents ongoing and past engineering consulting projects, their clients, staff, tasks, and invoices.

    DATA TABLES
    ------------
    - projects
    - clients
    - employees
    - project_tasks
    - invoices

    JOIN KEYS
    ----------
    projects.client_id = clients.client_id  
    projects.project_manager_id = employees.employee_id  
    projects.project_id = project_tasks.project_id  
    projects.project_id = invoices.project_id  
    project_tasks.assigned_to = employees.employee_id  

    BEHAVIOR GUIDELINES
    --------------------
    • Always generate ANSI-SQL syntax (compatible with SQLite or standard SQL engines).  
    • Query only existing columns and relationships. Ask clarifying questions when uncertain.  
    • Join only when needed (e.g., when client or manager info is requested).  
    • Use aggregation functions (SUM, COUNT, AVG) and GROUP BY appropriately.  
    • Summarize clearly when responding in natural language.  
    • Use consistent table aliases:  
    - p for projects  
    - c for clients  
    - e for employees  
    - t for project_tasks  
    - i for invoices  

    EXAMPLE BEHAVIOR
    -----------------
    User: “Show me all projects managed by Ben Li with pending invoices.”  
    → Join projects + employees + invoices.  
    → Filter e.name = 'Ben Li' AND i.status = 'Pending'.  
    → Return project name, client, and invoice amount.
    """,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Connecting Data Sources

# CELL ********************

# Run this if you developing within the Fabric Portal and can actively retrieve the list of data sources available in the workspace

def safe_get_datasources(): # add_datasource calls get_datasources, which doesn't handle None types.... This is a modified function which handles this
    config = data_agent._client.get_configuration()
    data_sources = config.value.get("dataSources") or []
    return [data_agent._client.get_datasource(ds["id"]) for ds in data_sources]

data_agent._client.get_datasources = safe_get_datasources

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adding all lakehouse data sources
for lakehouse_name in data_sources['lakehouse']:
    data_agent.add_datasource(lakehouse_name, type="lakehouse")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuring Data Source Instructions and Example Queries
# 
# In this section, we setup the data source instructions and examples queries for **one** lakehouse connection but if there are others data source connections, you must repeat this setup for each individual data source.

# CELL ********************

# Get the first data source
datasource = data_agent.get_datasources()[0] # change to 1 for the 2nd data source and 2 for the 3rd and so on.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table_name in lakehouse_table_names["DataAgentDefaultLH"]:
    datasource.select("dbo", table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Run this if you developing within the Fabric Portal and want to see the structure of the data source.
# datasource.pretty_print()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Update or specify (if done for the first time) the data source instructions. Below is a sample...
datasource.update_configuration(instructions="""
This dataset represents the projects, clients, staff, and financial operations of an engineering consulting company.  
It supports queries about project progress, management, clients, staffing, and billing.

TABLE 1 - projects
------------------
Each row represents a unique project.
• project_id - Primary Key  
• project_name - Project title  
• client_id - FK → clients  
• start_date / end_date - Project timeline  
• project_manager_id - FK → employees  

TABLE 2 - clients
-----------------
Each row describes a client organization.
• client_id - Primary Key  
• client_name - Client name  
• industry - Sector  
• contact_email - Contact address  

TABLE 3 - employees
-------------------
Each row represents a staff member.
• employee_id - Primary Key  
• name - Full name  
• role - Title (Project Manager, Engineer, etc.)  
• department - Discipline  
• email - Work email  

TABLE 4 - project_tasks
-----------------------
Each row represents a task assigned under a project.
• task_id - Primary Key  
• project_id - FK → projects  
• task_name - Task description  
• assigned_to - FK → employees  
• start_date / end_date - Task timeline  
• status - Task state (Completed, In Progress …)  

TABLE 5 - invoices
------------------
Each row represents a financial record.
• invoice_id - Primary Key  
• project_id - FK → projects  
• amount - Invoice value  
• issue_date / due_date - Billing dates  
• status - Invoice state (Paid, Pending, Draft …)  

RELATIONSHIPS
--------------
• One client → many projects  
• One project → one project manager (employee)  
• One project → many tasks and invoices  
• Tasks → employees through assigned_to  

This structure allows multidimensional queries such as revenue by client, overdue projects, task completion rates, or manager performance.
"""
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# You can check if the instructions have been sucessfully applied
datasource.get_configuration()["additional_instructions"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Update or specify (if done for the first time) the example queries. Below is a sample...
json_key_pairs_dict = {
    "List all projects.": "SELECT project_id, project_name, start_date, end_date FROM projects;",
    "Show all clients and their industries.": "SELECT client_name, industry FROM clients;",
    "Which employees are project managers?": "SELECT name, department FROM employees WHERE role = 'Project Manager';",
    "List all projects along with their client names.": "SELECT p.project_name, c.client_name FROM projects p JOIN clients c ON p.client_id = c.client_id;",
    "Show all projects managed by 'Ben Li'.": "SELECT p.project_name, c.client_name, p.start_date, p.end_date FROM projects p JOIN employees e ON p.project_manager_id = e.employee_id JOIN clients c ON p.client_id = c.client_id WHERE e.name = 'Ben Li';",
    "List all tasks for the project 'Bridge Strength Assessment'.": "SELECT t.task_name, e.name AS assigned_to, t.status FROM project_tasks t JOIN projects p ON t.project_id = p.project_id JOIN employees e ON t.assigned_to = e.employee_id WHERE p.project_name = 'Bridge Strength Assessment';",
    "Show total invoiced amount per project.": "SELECT p.project_name, SUM(i.amount) AS total_invoiced FROM projects p JOIN invoices i ON p.project_id = i.project_id GROUP BY p.project_name;",
    "Which clients have the highest total billed amount?": "SELECT c.client_name, SUM(i.amount) AS total_billed FROM clients c JOIN projects p ON c.client_id = p.client_id JOIN invoices i ON p.project_id = i.project_id GROUP BY c.client_name ORDER BY total_billed DESC;",
    "How many tasks are still in progress for each project?": "SELECT p.project_name, COUNT(*) AS tasks_in_progress FROM projects p JOIN project_tasks t ON p.project_id = t.project_id WHERE t.status = 'In Progress' GROUP BY p.project_name;",
    "List all pending invoices with project manager and client details.": "SELECT i.invoice_id, i.amount, p.project_name, e.name AS project_manager, c.client_name FROM invoices i JOIN projects p ON i.project_id = p.project_id JOIN employees e ON p.project_manager_id = e.employee_id JOIN clients c ON p.client_id = c.client_id WHERE i.status = 'Pending';",
    "Which projects are overdue (end date before today)?": "SELECT p.project_name, c.client_name, p.end_date FROM projects p JOIN clients c ON p.client_id = c.client_id WHERE p.end_date < DATE('now');",
    "For each project manager, show the number of projects they manage and the total invoice amount.": "SELECT e.name AS project_manager, COUNT(DISTINCT p.project_id) AS num_projects, SUM(i.amount) AS total_invoiced FROM employees e JOIN projects p ON e.employee_id = p.project_manager_id JOIN invoices i ON p.project_id = i.project_id GROUP BY e.name;",
    "Show all tasks completed by employees in the Civil Engineering department.": "SELECT t.task_name, p.project_name, e.name AS engineer_name FROM project_tasks t JOIN projects p ON t.project_id = p.project_id JOIN employees e ON t.assigned_to = e.employee_id WHERE e.department = 'Civil Engineering' AND t.status = 'Completed';",
    "Find clients with projects that have more than 2 pending invoices.": "SELECT c.client_name, COUNT(i.invoice_id) AS pending_invoices FROM clients c JOIN projects p ON c.client_id = p.client_id JOIN invoices i ON p.project_id = i.project_id WHERE i.status = 'Pending' GROUP BY c.client_name HAVING COUNT(i.invoice_id) > 2;"
}
datasource.add_fewshots(json_key_pairs_dict)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# You can check if the example queries have been sucessfully applied
datasource.get_fewshots()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Publishing Agent
# Most important step.... You will not be able to test or call agent if not published...

# CELL ********************

# Publishing Agent
data_agent.publish()

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
