variable "project_name" {
  description = "Lowercase project prefix used in Azure resource names."
  type        = string
  default     = "srg-lakehouse"

  validation {
    condition     = can(regex("^[a-z0-9-]{3,20}$", var.project_name))
    error_message = "project_name must contain 3-20 lowercase letters, numbers, or hyphens."
  }
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "location" {
  description = "Azure region for all regional resources."
  type        = string
  default     = "eastus"
}

variable "databricks_sku" {
  description = "Azure Databricks workspace SKU. Premium supports Unity Catalog scenarios."
  type        = string
  default     = "premium"

  validation {
    condition     = contains(["standard", "premium", "trial"], var.databricks_sku)
    error_message = "databricks_sku must be standard, premium, or trial."
  }
}

variable "storage_replication_type" {
  description = "ADLS Gen2 replication strategy."
  type        = string
  default     = "LRS"

  validation {
    condition     = contains(["LRS", "ZRS", "GRS", "RAGRS", "GZRS", "RAGZRS"], var.storage_replication_type)
    error_message = "storage_replication_type must be a supported Azure replication code."
  }
}

variable "enable_public_network" {
  description = "Enable public endpoints for a portfolio/dev deployment. Disable after private endpoints are configured."
  type        = bool
  default     = true
}

variable "tags" {
  description = "Additional tags applied to Azure resources."
  type        = map(string)
  default = {
    owner       = "data-engineering-portfolio"
    cost_center = "learning"
  }
}
