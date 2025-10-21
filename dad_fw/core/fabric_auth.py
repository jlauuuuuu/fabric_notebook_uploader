"""
Authentication utilities for Microsoft Fabric API operations.
Optimized for Azure DevOps pipelines using AzureCLI@2 task with Service Principal.
"""
import os
import json
import subprocess
from typing import Optional, Dict, Any


class FabricAuth:
    """
    Authentication utility for Fabric API that works with Azure DevOps AzureCLI@2 task.
    Supports both service principal and Azure CLI authentication methods.
    """
    
    @staticmethod
    def get_azure_cli_info() -> Optional[Dict[str, Any]]:
        """
        Get Azure CLI account information to detect service principal context.
        
        Returns:
            Dictionary with Azure CLI account info or None if not available
        """
        try:
            result = subprocess.run(
                ['az', 'account', 'show'],
                capture_output=True,
                text=True,
                check=True
            )
            return json.loads(result.stdout)
        except (subprocess.CalledProcessError, json.JSONDecodeError, FileNotFoundError):
            return None
    
    @staticmethod
    def create_fabric_client():
        """
        Create a FabricClientCore instance with appropriate authentication.
        
        Handles multiple authentication scenarios:
        1. Azure DevOps AzureCLI@2 task with service principal
        2. Service Principal with environment variables
        3. Azure CLI authentication (local development)
        
        Returns:
            Authenticated FabricClientCore instance
        """
        try:
            from msfabricpysdkcore import FabricClientCore
            
            # Method 1: Check for explicit service principal environment variables
            client_id = os.getenv("FABRIC_CLIENT_ID") or os.getenv("AZURE_CLIENT_ID")
            client_secret = os.getenv("FABRIC_CLIENT_SECRET") or os.getenv("AZURE_CLIENT_SECRET") 
            tenant_id = os.getenv("FABRIC_TENANT_ID") or os.getenv("AZURE_TENANT_ID")
            
            if client_id and client_secret and tenant_id:
                print("Using Service Principal authentication (environment variables)")
                fc = FabricClientCore(
                    tenant_id=tenant_id,
                    client_id=client_id,
                    client_secret=client_secret
                )
                print("Fabric client created successfully")
                return fc
            
            # Method 2: Check Azure CLI context (AzureCLI@2 task scenario)
            cli_info = FabricAuth.get_azure_cli_info()
            if cli_info:
                user_type = cli_info.get('user', {}).get('type')
                tenant_id = cli_info.get('tenantId')
                
                if user_type == 'servicePrincipal' and tenant_id:
                    print(f"Using Azure CLI Service Principal authentication")
                    print(f"Tenant ID: {tenant_id}")
                    print(f"Subscription: {cli_info.get('name', 'Unknown')}")
                    
                    # For service principal via Azure CLI, we can use FabricClientCore()
                    # It will automatically use the Azure CLI context
                    fc = FabricClientCore()
                    print("Fabric client created successfully")
                    return fc
                else:
                    print("Using Azure CLI authentication (user account)")
                    fc = FabricClientCore()
                    print("Fabric client created successfully")
                    return fc
            
            # Method 3: Fallback to default Azure CLI authentication
            print("Using Azure CLI authentication (fallback)")
            fc = FabricClientCore()
            print("Fabric client created successfully")
            return fc
            
        except ImportError as e:
            raise ImportError(
                f"Microsoft Fabric Python SDK is required: {e}. "
                "Install with: pip install msfabricpysdkcore"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to create authenticated Fabric client: {e}")
    
    @staticmethod
    def test_authentication():
        """
        Test the current authentication setup by creating a client and checking context.
        
        Returns:
            Dictionary with authentication test results
        """
        result = {
            "success": False,
            "method": "unknown",
            "error": None,
            "cli_info": None
        }
        
        try:
            # Get Azure CLI context
            cli_info = FabricAuth.get_azure_cli_info()
            if cli_info:
                result["cli_info"] = {
                    "tenant_id": cli_info.get('tenantId'),
                    "subscription": cli_info.get('name'),
                    "user_type": cli_info.get('user', {}).get('type'),
                    "environment": cli_info.get('environmentName')
                }
            
            # Determine authentication method
            client_id = os.getenv("FABRIC_CLIENT_ID") or os.getenv("AZURE_CLIENT_ID")
            client_secret = os.getenv("FABRIC_CLIENT_SECRET") or os.getenv("AZURE_CLIENT_SECRET") 
            tenant_id = os.getenv("FABRIC_TENANT_ID") or os.getenv("AZURE_TENANT_ID")
            
            if client_id and client_secret and tenant_id:
                result["method"] = "service_principal_env_vars"
            elif cli_info and cli_info.get('user', {}).get('type') == 'servicePrincipal':
                result["method"] = "azure_cli_service_principal"
            else:
                result["method"] = "azure_cli_user"
            
            # Try to create client
            fc = FabricAuth.create_fabric_client()
            result["success"] = True
            
        except Exception as e:
            result["error"] = str(e)
        
        return result