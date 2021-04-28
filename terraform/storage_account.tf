

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
