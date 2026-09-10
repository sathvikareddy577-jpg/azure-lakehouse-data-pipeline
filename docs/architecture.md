# Architecture and engineering decisions

## End-to-end flow

1. Azure Data Factory triggers three dependent Databricks notebook activities.
2. Bronze reads explicit source schemas, attaches lineage, hashes normalized
   source values, and uses Delta MERGE to avoid file-replay growth.
3. Silver standardizes values and applies required-field, numeric-range,
   deduplication, and dimension-reference checks.
4. Invalid rows are serialized into entity-specific quarantine Delta tables.
5. CRM customer changes are normalized, deduplicated, reduced to the latest
   event per customer, and merged into the current Silver customer table.
6. Gold creates dimensions, a fact, and daily KPIs. The run fails before output
   publication if fact and KPI counts or revenue do not reconcile.

## Why Delta Lake instead of plain Parquet?

Parquet supplies the columnar file format, but it does not by itself provide a
transaction log, atomic table commits, schema enforcement, or MERGE semantics.
Delta Lake adds those controls while retaining Parquet data files. This project
uses MERGE for Bronze replay protection and customer CDC, and uses atomic writes
for curated layers.

## Idempotency boundaries

| Boundary | Control | Repeated-run result |
|---|---|---|
| Source file → Bronze | Source system + SHA-256 normalized-record hash | No extra identical Bronze row |
| Silver batch | Deterministic transforms + overwrite | Same curated state |
| Customer CDC | Event-ID dedupe + latest event + MERGE | Same customer count and state |
| Gold batch | Deterministic aggregate + overwrite | Same dimensions, fact, and KPI |

The Bronze hash is appropriate for this synthetic file source. A production
system with legitimate identical events should use a source-supplied immutable
event ID or a file ID plus source row number instead.

## Failure behavior

- A missing source file fails Bronze; downstream ADF activities do not run.
- Invalid business rows do not fail Silver; they go to quarantine with a reason.
- A missing Silver dependency fails the requested Gold-only run with a clear
  table-read error.
- A Gold reconciliation mismatch raises an exception and prevents output writes.
- ADF retries a failed Databricks activity twice with a 60-second interval.
- The Databricks job allows one concurrent workflow run to avoid overlapping
  batch writes.

## Scaling path

- Replace batch CSV reads with the provided Auto Loader helper.
- Partition high-volume Bronze tables by ingestion date only after measuring
  file sizes; avoid tiny partitions.
- Use liquid clustering or `OPTIMIZE` based on observed query predicates rather
  than applying Z-ORDER blindly.
- Broadcast genuinely small dimensions; use adaptive query execution for skew.
- Move from current-state customer dimensions to SCD Type 2 when the business
  needs historical attribute reporting.
- Add Azure Monitor/Log Analytics diagnostics and an alert on rejection ratio.

## Deliberate limitations

- Synthetic retail data only; no confidential or regulated records.
- Current customer dimension (SCD Type 1 plus soft delete), not full SCD Type 2.
- ADF JSON contains environment parameters and a placeholder Databricks Repo
  user path that must be configured after provisioning.
- Private endpoints are not created in this portfolio template. Public network
  access defaults to true for a simple development deployment and should be
  disabled only after private networking is configured.
