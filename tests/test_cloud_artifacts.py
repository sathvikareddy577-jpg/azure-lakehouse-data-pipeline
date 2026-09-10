"""Static contract tests for Azure deployment templates."""

from __future__ import annotations

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
AZURE_ROOT = PROJECT_ROOT / "azure"


def _load(relative_path: str):
    return json.loads((AZURE_ROOT / relative_path).read_text(encoding="utf-8"))


def test_all_azure_json_is_valid():
    for path in AZURE_ROOT.rglob("*.json"):
        json.loads(path.read_text(encoding="utf-8"))


def test_adf_pipeline_uses_notebook_activity():
    pipeline = _load("data_factory/pipeline_main.json")
    activity_types = {activity["type"] for activity in pipeline["properties"]["activities"]}
    assert activity_types == {"DatabricksNotebook"}


def test_linked_services_do_not_store_account_keys():
    documents = [
        _load("data_factory/linked_service_adls.json"),
        _load("data_factory/linked_service_databricks.json"),
    ]
    serialized = json.dumps(documents).lower()
    assert "accountkey" not in serialized
    assert "access_token" not in serialized
    assert "***" not in serialized
    assert "managed-identity" in serialized


def test_databricks_job_is_azure_specific_and_paused_by_default():
    job = _load("databricks/job_config.json")
    serialized = json.dumps(job).lower()
    assert "aws_attributes" not in serialized
    assert job["schedule"]["pause_status"] == "PAUSED"
    assert [task["task_key"] for task in job["tasks"]] == [
        "bronze",
        "silver",
        "gold",
    ]


def test_terraform_provisions_claimed_core_services():
    terraform = (AZURE_ROOT / "terraform" / "main.tf").read_text(encoding="utf-8")
    assert 'resource "azurerm_storage_account"' in terraform
    assert 'resource "azurerm_databricks_workspace"' in terraform
    assert 'resource "azurerm_data_factory"' in terraform
    assert 'resource "azurerm_role_assignment"' in terraform
    assert '"""' not in terraform
