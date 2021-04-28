resource "azurerm_databricks_workspace" "dbw" {
  name                = var.databricks_workspace_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "standard"

  tags = {
    Environment = var.environment
  }
}

output "databricks_host" {

  value = "https://${azurerm_databricks_workspace.dbw.workspace_url}/"
}


