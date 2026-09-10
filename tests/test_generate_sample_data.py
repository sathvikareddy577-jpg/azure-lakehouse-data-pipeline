"""Tests for deterministic and intentional synthetic-data behavior."""

from __future__ import annotations

import csv
import json
import random
from pathlib import Path

from scripts.generate_sample_data import (
    generate_customer_cdc,
    generate_customers,
    generate_orders,
    generate_products,
)


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_seeded_customer_generation_is_reproducible(tmp_path: Path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    first_path = generate_customers(first, 5, random.Random(42))
    second_path = generate_customers(second, 5, random.Random(42))
    assert first_path.read_text(encoding="utf-8") == second_path.read_text(encoding="utf-8")


def test_generator_injects_documented_quality_failures(tmp_path: Path):
    rng = random.Random(42)
    customers = _csv_rows(generate_customers(tmp_path, 10, rng))
    products = _csv_rows(generate_products(tmp_path, 10, rng))
    orders = _csv_rows(generate_orders(tmp_path, 10, 10, 10, rng))
    assert customers[1]["email"] == ""
    assert float(products[1]["unit_price"]) < 0
    assert orders[1]["order_date"] == "invalid-date"
    assert float(orders[2]["order_amount"]) < 0
    assert orders[3]["customer_id"] == "CUST_MISSING"
    assert orders[4]["product_id"] == "PROD_MISSING"


def test_cdc_generator_includes_an_exact_replay(tmp_path: Path):
    path = generate_customer_cdc(tmp_path, customer_count=10, count=6)
    events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert len(events) == 7
    assert events[0] == events[-1]
    assert {event["operation"] for event in events} == {"INSERT", "UPDATE", "DELETE"}
