"""
Terraform configuration for Azure Lakehouse infrastructure
This creates:
- Resource Group
- ADLS Gen2 Storage Account
- Databricks Workspace
- Virtual Network
"""

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
  
  # Uncomment to use remote backend
  # backend "azurerm" {
  #   resource_group_name  = "terraform-state"
  #   storage_account_name = "terraformstate"
  #   container_name       = "tfstate"
  #   key                  = "lakehouse.tfstate"
  # }
}

provider "azurerm" {
  features {}
}

# Resource Group
resource "azurerm_resource_group" "lakehouse" {
  name     = "${var.project_name}-rg-${var.environment}"
  location = var.region
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# ADLS Gen2 Storage Account
resource "azurerm_storage_account" "lakehouse" {
  name                     = replace("${var.project_name}${var.environment}", "-", "")
  resource_group_name      = azurerm_resource_group.lakehouse.name
  location                 = azurerm_resource_group.lakehouse.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# ADLS Containers
resource "azurerm_storage_data_lake_gen2_filesystem" "raw" {
  name               = "raw"
  storage_account_id = azurerm_storage_account.lakehouse.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "warehouse" {
  name               = "warehouse"
  storage_account_id = azurerm_storage_account.lakehouse.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "quarantine" {
  name               = "quarantine"
  storage_account_id = azurerm_storage_account.lakehouse.id
}

# Virtual Network for Databricks
resource "azurerm_virtual_network" "lakehouse" {
  name                = "${var.project_name}-vnet-${var.environment}"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.lakehouse.location
  resource_group_name = azurerm_resource_group.lakehouse.name
}

resource "azurerm_subnet" "databricks" {
  name                 = "databricks-subnet"
  resource_group_name  = azurerm_resource_group.lakehouse.name
  virtual_network_name = azurerm_virtual_network.lakehouse.name
  address_prefixes     = ["10.0.1.0/24"]
}

# Databricks Workspace
resource "azurerm_databricks_workspace" "lakehouse" {
  name                = "${var.project_name}-workspace-${var.environment}"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = azurerm_resource_group.lakehouse.location
  sku                 = var.databricks_sku
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
  
  depends_on = [
    azurerm_resource_group.lakehouse
  ]
}

# Note: Additional configurations may be needed for:
# - Network security groups
# - Azure Key Vault for secrets
# - Azure Data Factory
# - Diagnostic settings
