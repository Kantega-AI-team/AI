terraform {
  required_providers {
    azurerm = "2.56.0"
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

resource "azurerm_resource_group" "rg" {
  name     = var.resource_group
  location = var.region
}

resource "azurerm_storage_account" "dls" {
  name                     = var.datalake_storage_account_name
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = var.datalake_storage_account_replication_type
  is_hns_enabled           = true

  tags = {
    environment = var.environment
  }
}
