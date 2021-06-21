terraform {
  required_providers {
    azurerm = "2.56.0"
    databricks = {
      source  = "databrickslabs/databricks"
      version = "0.3.5"
    }
  }
  backend "azurerm" {
    resource_group_name  = "rg-aitf-dev-we-001"
    storage_account_name = "dlsaitfdevwe001"
    container_name       = "tfstate"
    key                  = "rg-ai-dev-we-001/dev.terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
}

provider "databricks" {
  azure_workspace_resource_id      = azurerm_databricks_workspace.dbw.id
  azure_pat_token_duration_seconds = "8600000"
}
