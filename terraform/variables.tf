/* region="West Europe"
databricks_workspace_resource_name="Kantega AI project"
datalake_storage_account_name="dlsaidevwesteu001"
datalake_storage_account_replication_type="ZRS"
environment="development"
*/

variable "region" {
  type        = string
  default     = "West Europe"
  description = "Azure region of all resources"
}

variable "resource_group" {
  type        = string
  default     = "rg-ai-dev-we-001"
  description = "Name of Databricks Workspace Resource"
}

variable "datalake_storage_account_name" {
  type        = string
  default     = "dlsaidevwe001"
  description = "Name of Datalake Storage Account"
}

variable "datalake_storage_account_replication_type" {
  type        = string
  default     = "LRS"
  description = "Data in an Azure Storage account is always replicated locally, acrooss the primary region, or across regions"
}

variable "environment" {
  type        = string
  default     = "dev"
  description = "Name of environment. Example 'dev' or 'prod'"
}
