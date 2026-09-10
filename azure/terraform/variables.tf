variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "lakehouse"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "region" {
  description = "Azure region"
  type        = string
  default     = "eastus"
}

variable "databricks_sku" {
  description = "Databricks workspace SKU"
  type        = string
  default     = "standard"
  
  validation {
    condition     = contains(["standard", "premium"], var.databricks_sku)
    error_message = "SKU must be standard or premium."
  }
}

variable "storage_replication_type" {
  description = "Storage account replication type"
  type        = string
  default     = "LRS"
  
  validation {
    condition     = contains(["LRS", "GRS", "RAGRS"], var.storage_replication_type)
    error_message = "Replication type must be LRS, GRS, or RAGRS."
  }
}
