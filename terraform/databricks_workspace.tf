resource "azurerm_databricks_workspace" "dbw" {
  name                = "${var.workspace_prefix}-${var.environment}-${var.standard_suffix}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"

  tags = {
    Environment = var.environment
  }
}

output "databricks_host" {
  value = "https://${azurerm_databricks_workspace.dbw.workspace_url}/"
}


