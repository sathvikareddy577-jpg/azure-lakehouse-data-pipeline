terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

locals {
  compact_prefix       = substr(replace(lower("${var.project_name}${var.environment}"), "-", ""), 0, 17)
  storage_account_name = substr("${local.compact_prefix}${random_string.suffix.result}", 0, 24)
  resource_prefix      = "${var.project_name}-${var.environment}"
  common_tags = merge(var.tags, {
    environment = var.environment
    project     = var.project_name
    managed_by  = "terraform"
  })
}

resource "azurerm_resource_group" "lakehouse" {
  name     = "${local.resource_prefix}-rg"
  location = var.location
  tags     = local.common_tags
}

resource "azurerm_storage_account" "lakehouse" {
  name                            = local.storage_account_name
  resource_group_name             = azurerm_resource_group.lakehouse.name
  location                        = azurerm_resource_group.lakehouse.location
  account_tier                    = "Standard"
  account_replication_type        = var.storage_replication_type
  account_kind                    = "StorageV2"
  is_hns_enabled                  = true
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false
  public_network_access_enabled   = var.enable_public_network
  tags                            = local.common_tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "layers" {
  for_each = toset(["raw", "warehouse", "quarantine", "checkpoints"])

  name               = each.value
  storage_account_id = azurerm_storage_account.lakehouse.id
}

resource "azurerm_databricks_workspace" "lakehouse" {
  name                        = "${local.resource_prefix}-dbw"
  resource_group_name         = azurerm_resource_group.lakehouse.name
  location                    = azurerm_resource_group.lakehouse.location
  sku                         = var.databricks_sku
  managed_resource_group_name = "${local.resource_prefix}-dbw-managed-rg"
  tags                        = local.common_tags
}

resource "azurerm_databricks_access_connector" "lakehouse" {
  name                = "${local.resource_prefix}-access-connector"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = azurerm_resource_group.lakehouse.location

  identity {
    type = "SystemAssigned"
  }

  tags = local.common_tags
}

resource "azurerm_data_factory" "lakehouse" {
  name                = "${local.resource_prefix}-adf-${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = azurerm_resource_group.lakehouse.location

  identity {
    type = "SystemAssigned"
  }

  public_network_enabled = var.enable_public_network
  tags                   = local.common_tags
}

resource "azurerm_role_assignment" "databricks_storage" {
  scope                = azurerm_storage_account.lakehouse.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.lakehouse.identity[0].principal_id
}

resource "azurerm_role_assignment" "data_factory_storage" {
  scope                = azurerm_storage_account.lakehouse.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.lakehouse.identity[0].principal_id
}
