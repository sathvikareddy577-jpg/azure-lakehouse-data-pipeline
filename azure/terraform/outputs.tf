output "resource_group_name" {
  value       = azurerm_resource_group.lakehouse.name
  description = "Resource Group name"
}

output "resource_group_id" {
  value       = azurerm_resource_group.lakehouse.id
  description = "Resource Group ID"
}

output "storage_account_id" {
  value       = azurerm_storage_account.lakehouse.id
  description = "ADLS Gen2 Storage Account ID"
}

output "storage_account_name" {
  value       = azurerm_storage_account.lakehouse.name
  description = "ADLS Gen2 Storage Account name"
}

output "storage_primary_dfs_endpoint" {
  value       = azurerm_storage_account.lakehouse.primary_dfs_endpoint
  description = "Primary DFS endpoint for ADLS Gen2"
}

output "raw_filesystem_id" {
  value       = azurerm_storage_data_lake_gen2_filesystem.raw.id
  description = "Raw filesystem ID"
}

output "warehouse_filesystem_id" {
  value       = azurerm_storage_data_lake_gen2_filesystem.warehouse.id
  description = "Warehouse filesystem ID"
}

output "quarantine_filesystem_id" {
  value       = azurerm_storage_data_lake_gen2_filesystem.quarantine.id
  description = "Quarantine filesystem ID"
}

output "databricks_workspace_id" {
  value       = azurerm_databricks_workspace.lakehouse.id
  description = "Databricks Workspace ID"
}

output "databricks_workspace_url" {
  value       = azurerm_databricks_workspace.lakehouse.workspace_url
  description = "Databricks Workspace URL"
}

output "vnet_id" {
  value       = azurerm_virtual_network.lakehouse.id
  description = "Virtual Network ID"
}

output "databricks_subnet_id" {
  value       = azurerm_subnet.databricks.id
  description = "Databricks Subnet ID"
}
