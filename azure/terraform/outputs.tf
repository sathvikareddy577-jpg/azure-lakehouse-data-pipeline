output "resource_group_name" {
  description = "Resource group containing the lakehouse platform."
  value       = azurerm_resource_group.lakehouse.name
}

output "storage_account_name" {
  description = "Unique ADLS Gen2 storage account name."
  value       = azurerm_storage_account.lakehouse.name
}

output "storage_dfs_endpoint" {
  description = "Hierarchical namespace endpoint for ABFS access."
  value       = azurerm_storage_account.lakehouse.primary_dfs_endpoint
}

output "databricks_workspace_url" {
  description = "Azure Databricks workspace URL."
  value       = azurerm_databricks_workspace.lakehouse.workspace_url
}

output "databricks_workspace_resource_id" {
  description = "Resource ID used by the ADF Databricks linked service."
  value       = azurerm_databricks_workspace.lakehouse.id
}

output "databricks_access_connector_id" {
  description = "Managed-identity access connector for ADLS permissions."
  value       = azurerm_databricks_access_connector.lakehouse.id
}

output "data_factory_name" {
  description = "Azure Data Factory orchestrator name."
  value       = azurerm_data_factory.lakehouse.name
}

output "adls_filesystems" {
  description = "Created lakehouse filesystem names."
  value       = sort(keys(azurerm_storage_data_lake_gen2_filesystem.layers))
}
