# auth_cli.py
from azure.identity import AzureCliCredential

# The Azure CLI must be logged in under your user context.
credential = AzureCliCredential()

# Acquire token scoped to Fabric REST APIs.
fabric_scope = "https://api.fabric.microsoft.com/.default"
token = credential.get_token(fabric_scope)

access_token = token.token
