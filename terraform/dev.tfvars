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

variable "databricks_workspace_name" {
  type        = string
  default     = "dbw-ai-dev-we-001"
  description = "Name of Databricks Workspace"
}

variable "databricks_cluster_name" {
  type        = string
  default     = "spark-ai-dev-we-001"
  description = "Name of Databricks Cluster"
}

variable "databricks_cluster_node_type" {
  type        = string
  default     = "Standard_F4s_v2"
  description = "Node type id"
}

variable "databricks_storage_container" {
  type        = string
  default     = "public"
  description = "Storage container name"
}

variable "adapp_name" {
  type        = string
  default     = "ad-ai-dev-we-001"
  description = "App name for AD used in mount"
}
