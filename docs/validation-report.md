# Validation report

Validation date: 2026-09-10

## Baseline audit

The first repository version displayed a completion message, but a clean test
run collected 16 tests and all 16 failed. Confirmed root causes included:

- Delta catalog classes configured without attaching the Delta JVM package;
- missing `lower` and `countDistinct` imports;
- fixtures that did not satisfy the transformation contracts;
- tests labeled as CDC tests that never executed the CDC merge;
- an AWS-only cluster property inside the Azure Databricks job;
- an incorrect ADF activity type for a Databricks notebook;
- an invalid Terraform triple-quoted header; and
- README claims and files that did not match.

## Current local evidence

```text
Fast tests:       42 passed
Delta tests:       7 collected separately
Unit coverage:    86%
Ruff:             passed
Python compile:   passed
Azure JSON parse: passed
```

The execution workspace used for the repair blocks Maven artifact downloads, so
the seven tests requiring the Delta JVM package are intentionally isolated and
run in GitHub Actions, where the dependency can be resolved. This limitation is
reported instead of presenting skipped integration tests as local passes.

## Reproduce the fast validation

```bash
python -m pip install -r requirements-dev.txt
ruff check .
python -m compileall -q src scripts azure/databricks tests
pytest -m "not integration" -q --cov=src --cov-report=term-missing
```

Expected result: 42 passed, 7 deselected, and coverage above the 75% CI floor.

## Reproduce Delta validation

```bash
RUN_DELTA_TESTS=1 pytest -m integration -q
```

The first run needs access to Maven Central so Spark can download the Delta Lake
JVM artifact paired with the installed `delta-spark` package.

## CI acceptance criteria

- Unit matrix passes on Python 3.10 and 3.12.
- Coverage is at least 75%.
- Ruff and Python compilation pass.
- All seven Delta integration tests pass.
- Every Azure JSON file parses.
- Terraform formatting, initialization, and validation pass.

The GitHub Actions run is the source of truth for cross-environment validation.
