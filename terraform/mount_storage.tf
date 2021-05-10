# Create azure ad app

resource "azuread_application" "dbw" {
  display_name = var.adapp_name
  required_resource_access {
    resource_app_id = "e406a681-f3d4-42a8-90b6-c2b029497af1"
    resource_access {
      id   = "03e0da56-190b-40ad-a80c-ea378c433f7f"
      type = "Scope"
    }
  }
  required_resource_access {
    resource_app_id = "00000003-0000-0000-c000-000000000000"
    resource_access {
      id   = "e1fe6dd8-ba31-4d61-89e7-88639da4683d"
      type = "Scope"
    }
  }
}

# Generate password
resource "random_password" "password" {
  length  = 64
  special = false
}

# Set generated password as app password
resource "azuread_application_password" "password" {
  end_date              = "2023-12-30T23:00:00Z"
  application_object_id = azuread_application.dbw.object_id
  value                 = random_password.password.result
}

# Create service principal
resource "azuread_service_principal" "dbw_sp" {
  application_id = azuread_application.dbw.application_id
}

# Give service principal access to storage
resource "azurerm_role_assignment" "storage_contributor" {
  scope                = azurerm_storage_account.dls.id
  role_definition_name = "Storage blob data contributor"
  principal_id         = azuread_service_principal.dbw_sp.object_id
}

# Create databricks secret scope
resource "databricks_secret_scope" "terraform" {
  name = "application"
}

# Put service principal secret in secret scope
resource "databricks_secret" "service_principal_key" {
  key          = "secret"
  string_value = random_password.password.result
  scope        = databricks_secret_scope.terraform.name
}
# Get current config
data "azurerm_client_config" "current" {
}

# Mount raw storage
resource "databricks_azure_adls_gen2_mount" "public" {
  container_name         = azurerm_storage_container.st.name
  storage_account_name   = azurerm_storage_account.dls.name
  mount_name             = azurerm_storage_container.st.name
  tenant_id              = data.azurerm_client_config.current.tenant_id
  client_id              = azuread_application.dbw.application_id
  client_secret_scope    = databricks_secret_scope.terraform.name
  client_secret_key      = databricks_secret.service_principal_key.key
  initialize_file_system = true
}
output "application_id" {
  value = azuread_application.dbw.application_id
}
output "tenant_id" {
  value = data.azurerm_client_config.current.tenant_id
}
