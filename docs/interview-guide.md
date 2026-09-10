# Interview guide

## A natural 60-second explanation

I built this project around a retail company receiving customers, products,
orders, and CRM customer changes. I used a medallion design because each layer
has a clear responsibility. Bronze preserves source data and attaches lineage.
Silver standardizes types, checks required values and business ranges, removes
duplicate business keys, validates order references, and stores rejected rows
with a reason. Customer inserts, updates, and deletes are applied with Delta
MERGE. Gold creates customer and product dimensions, an order fact, and daily
sales KPIs. Before publishing Gold, I reconcile both order count and revenue.
ADF controls the Databricks stage order, Terraform provisions the Azure
services and managed identities, and GitHub Actions tests the code and cloud
templates.

## Why did you use ADF and Databricks together?

ADF is the orchestrator: scheduling, dependencies, retries, parameters, and
monitoring. Databricks is the compute engine: distributed transformations,
Delta transactions, CDC MERGE, and analytics aggregation. Replacing Databricks
with ADF mapping flows is possible for some transformations, but PySpark gives
this workload stronger code reuse and testing. Replacing ADF with a Databricks
workflow is also possible; this project shows the common enterprise separation
between orchestration and processing.

## How is rerunning safe?

Bronze creates a SHA-256 identity from normalized source values and merges only
unseen identities. Silver and Gold are deterministic rebuilds. CDC events are
deduplicated by event ID, reduced to the latest event per customer within the
batch, and merged by customer ID. Integration tests ingest the same file and
event twice and confirm row counts do not grow.

## What happens to bad data?

Bad business records do not silently disappear and do not poison Gold. Silver
separates them into quarantine tables. Each rejection stores the entity, record
ID, original row as JSON, reason, and time. Operationally, I would alert when
the rejection percentage crosses a threshold, investigate the source contract,
and replay corrected data.

## How did you test it?

The fast suite tests transformations, required fields, date and numeric rules,
foreign keys, dimensional metrics, reconciliation, schemas, configuration,
logging, and Azure artifact contracts. A separate suite starts Delta Lake and
tests Bronze replay, real CDC INSERT/UPDATE/DELETE behavior, and the complete
pipeline. CI runs Python 3.10 and 3.12 plus Terraform validation.

## What would you change for production scale?

I would replace polling batch files with Auto Loader and source event IDs,
measure file sizes before choosing partitions, add observability and rejection
alerts, use an encrypted remote Terraform backend, add private endpoints and
diagnostic settings, and implement SCD Type 2 only if analysts require historical
customer attributes.

## A real debugging story

If Gold revenue is lower than Silver, I first stop publication because the
reconciliation check fails. Then I compare record counts stage by stage, inspect
quarantine reasons, check whether a cast converted values to null, and verify
foreign-key anti-joins. If the source changed a column, I update the explicit
contract and add a regression fixture before replaying the failed batch. This
keeps the investigation evidence-based instead of rerunning blindly.

## Important honesty in an interview

This is a synthetic portfolio implementation with deployable Azure templates.
Say that clearly. Do not claim that it processed a real employer's production
data or that the Azure template was deployed unless you personally deploy and
verify it in your subscription.
