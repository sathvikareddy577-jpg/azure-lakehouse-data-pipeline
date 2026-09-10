# Safe Azure deployment guide

## Before spending money

This project can be reviewed and run locally without Azure. A real deployment
creates an Azure Databricks workspace and compute, ADLS Gen2, and Azure Data
Factory. Check your subscription, quota, region, and estimated price first.

## 1. Authenticate and validate

```bash
az login
az account show --output table
az account set --subscription "<subscription-id>"

cd azure/terraform
terraform init
terraform fmt -check -recursive
terraform validate
terraform plan -out lakehouse.tfplan
terraform show lakehouse.tfplan
```

Do not run `terraform apply` until the plan contains only the resources you
expect. Terraform state may contain infrastructure identifiers; keep it out of
Git and use an encrypted remote backend for a shared environment.

## 2. Provision core resources

```bash
terraform apply lakehouse.tfplan
terraform output
```

The configuration provisions:

- one resource group;
- one hierarchical-namespace storage account;
- `raw`, `warehouse`, `quarantine`, and `checkpoints` filesystems;
- one Azure Databricks workspace and access connector;
- one Azure Data Factory with system-assigned identity; and
- Storage Blob Data Contributor assignments for the managed identities.

## 3. Configure Databricks

1. Connect this GitHub repository through Databricks Repos.
2. Replace `<user>` in `azure/databricks/job_config.json` with the actual Repo
   owner/path visible in Databricks.
3. Replace `<storage-account>` widget defaults or pass real ABFSS paths from ADF.
4. If using Unity Catalog, configure an external location backed by the access
   connector before running the notebook.
5. Import the job configuration through the Databricks CLI/API and leave the
   schedule paused until a successful manual development run.

## 4. Configure Azure Data Factory

1. Import the linked-service templates and `pipeline_main.json`.
2. Parameterize `workspaceDomain`, `workspaceResourceId`, and
   `storageAccountName` from Terraform outputs.
3. Replace `<user>` in all notebook paths.
4. Grant the Data Factory managed identity access to the Databricks workspace.
5. Validate the pipeline in ADF Studio before publishing.
6. Trigger a manual run with paths similar to:

```text
rawDataPath       = abfss://raw@<account>.dfs.core.windows.net
warehousePath     = abfss://warehouse@<account>.dfs.core.windows.net
cdcPath           = abfss://raw@<account>.dfs.core.windows.net/cdc/customer_cdc_events.json
```

## 5. Verify a cloud run

- ADF shows Bronze, Silver, and Gold as successful in order.
- Bronze rerun does not increase counts for the same source files.
- Quarantine contains the intentionally invalid demo records and reasons.
- Silver has unique business keys and no orphan orders.
- Gold reconciliation succeeds.
- No token, account key, or secret appears in source control or activity output.

## 6. Stop charges

Keep the Databricks workflow paused when it is not being demonstrated. For a
temporary learning deployment, destroy the resources after exporting evidence:

```bash
terraform plan -destroy
terraform destroy
```

Review the destroy plan first. In a real organization, retention, backup, and
change-management policy must be followed instead of directly destroying a
shared environment.
