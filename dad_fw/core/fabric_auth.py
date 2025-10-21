"""
Authentication utilities for Microsoft Fabric API operations.
Optimized for Azure DevOps pipelines using AzureCLI@2 task.
"""
import os
from typing import Optional
from azure.identity import AzureCliCredential, DefaultAzureCredential, InteractiveBrowserCredential


class FabricAuth:
    """
    Simplified authentication utility for Fabric API.
    Designed to work seamlessly with Azure DevOps AzureCLI@2 task.
    """
    
    @staticmethod
    def get_credential():
        """
        Get Azure credential with priority for Azure CLI (from AzureCLI@2 task).
        
        Priority order:
        1. Azure CLI credential (from AzureCLI@2 task in Azure DevOps)
        2. Default Azure credential (fallback)
        3. Interactive browser (local development only)
        
        Returns:
            Azure credential object suitable for Fabric authentication
        """
        # Method 1: Azure CLI (from AzureCLI@2 task)
        try:
            credential = AzureCliCredential()
            # Test if Azure CLI is available and authenticated
            credential.get_token("https://analysis.windows.net/powerbi/api/.default")
            print("🔐 Using Azure CLI authentication (from AzureCLI@2 task)")
            return credential
        except Exception as e:
            print(f"Azure CLI auth not available: {e}")
        
        # Method 2: Default Azure Credential
        try:
            credential = DefaultAzureCredential()
            credential.get_token("https://analysis.windows.net/powerbi/api/.default")
            print("🔐 Using Default Azure Credential")
            return credential
        except Exception as e:
            print(f"Default credential not available: {e}")
        
        # Method 3: Interactive Browser (local development only)
        print("🔐 Falling back to Interactive Browser authentication")
        return InteractiveBrowserCredential()
    
    @staticmethod
    def create_fabric_client():
        """
        Create a FabricClientCore instance with appropriate authentication.
        
        Returns:
            Authenticated FabricClientCore instance
        """
        try:
            from msfabricpysdkcore import FabricClientCore
            
            credential = FabricAuth.get_credential()
            
            # Create Fabric client with credential
            fc = FabricClientCore(credential=credential)
            print("✅ Fabric client created successfully")
            
            return fc
            
        except ImportError:
            raise ImportError(
                "Microsoft Fabric Python SDK is required. "
                "Install with: pip install msfabricpysdkcore"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to create authenticated Fabric client: {e}")
    
    @staticmethod
    def test_authentication():
        """
        Test the current authentication setup.
        
        Returns:
            Dictionary with authentication test results
        """
        result = {
            "success": False,
            "method": "unknown",
            "error": None
        }
        
        try:
            credential = FabricAuth.get_credential()
            
            # Try to get a token to test authentication
            token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
            
            if isinstance(credential, AzureCliCredential):
                result["method"] = "azure_cli"
            elif isinstance(credential, DefaultAzureCredential):
                result["method"] = "default_credential"
            else:
                result["method"] = "interactive_browser"
            
            result["success"] = True
            result["token_expires"] = token.expires_on
            
        except Exception as e:
            result["error"] = str(e)
        
        return result