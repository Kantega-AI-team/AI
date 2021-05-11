variable "region" {
  type        = string
  default     = "West Europe"
  description = "Azure region of all resources"
}

variable "rg_prefix" {
  type        = string
  default     = "rg-ai"
  description = "Resource group for all Azure resources"
}

variable "dls_prefix" {
  type        = string
  default     = "dlsai"
  description = "Prefix for storage account"
}

variable "dls_suffix" {
  type        = string
  default     = "we001"
  description = "Suffix for storage account"
}

variable "datalake_storage_account_replication_type" {
  type        = string
  default     = "LRS"
  description = "Data in an Azure Storage account is always replicated locally, acrooss the primary region, or across regions"
}

variable "environment" {
  type        = string
  description = "Name of environment. Example 'dev' or 'prod'"
}

variable "workspace_prefix" {
  type        = string
  default     = "dbw-ai"
  description = "Databricks workspace name prefix"
}

variable "cluster_prefix" {
  type        = string
  default     = "spark-ai"
  description = "Prefix for Spark Cluster used in Databricks"
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

variable "adapp_prefix" {
  type        = string
  default     = "ad-ai"
  description = "Prefix for azure ad app"
}

variable "standard_suffix" {
  type        = string
  default     = "we-001"
  description = "Standard suffix for azure resources. Region and instance count"
}