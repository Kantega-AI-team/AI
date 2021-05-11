resource "azurerm_resource_group" "rg" {
  name     = "${var.rg_prefix}-${var.environment}-${var.standard_suffix}"
  location = var.region
}

resource "azurerm_storage_account" "dls" {
  name                     = "${var.dls_prefix}${var.environment}${var.dls_suffix}"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = var.datalake_storage_account_replication_type
  is_hns_enabled           = true

  tags = {
    environment = var.environment
  }
}

resource "azurerm_storage_container" "st" {
  name                  = var.databricks_storage_container
  storage_account_name  = azurerm_storage_account.dls.name
  container_access_type = "private"
}

