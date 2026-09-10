"""Generate deterministic retail source files and customer CDC events."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def generate_customers(
    output_dir: Path,
    count: int,
    rng: random.Random,
) -> Path:
    path = output_dir / "customers.csv"
    countries = ["USA", "UK", "Canada", "Australia", "Germany", "India"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["customer_id", "name", "email", "age", "country"])
        for index in range(count):
            email = "" if count >= 10 and index == 1 else f"customer{index}@example.com"
            writer.writerow(
                [
                    f"CUST_{index:05d}",
                    f"Customer {index}",
                    email,
                    rng.randint(18, 80),
                    rng.choice(countries),
                ]
            )
    logger.info("Generated %s customer rows -> %s", count, path)
    return path


def generate_products(
    output_dir: Path,
    count: int,
    rng: random.Random,
) -> Path:
    path = output_dir / "products.csv"
    categories = ["Electronics", "Clothing", "Home", "Sports", "Books", "Beauty"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["product_id", "product_name", "category", "unit_price"])
        for index in range(count):
            price = -5.0 if count >= 10 and index == 1 else round(rng.uniform(10, 500), 2)
            writer.writerow(
                [
                    f"PROD_{index:05d}",
                    f"Product {index}",
                    rng.choice(categories),
                    price,
                ]
            )
    logger.info("Generated %s product rows -> %s", count, path)
    return path


def generate_orders(
    output_dir: Path,
    customer_count: int,
    product_count: int,
    count: int,
    rng: random.Random,
) -> Path:
    path = output_dir / "orders.csv"
    base_date = datetime(2025, 1, 1)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "order_id",
                "customer_id",
                "product_id",
                "order_date",
                "order_amount",
                "order_quantity",
            ]
        )
        for index in range(count):
            customer_id = f"CUST_{rng.randrange(customer_count):05d}"
            product_id = f"PROD_{rng.randrange(product_count):05d}"
            order_date = (base_date + timedelta(days=rng.randrange(365))).date().isoformat()
            amount = round(rng.uniform(25, 2500), 2)

            if count >= 10 and index == 1:
                order_date = "invalid-date"
            elif count >= 10 and index == 2:
                amount = -100.0
            elif count >= 10 and index == 3:
                customer_id = "CUST_MISSING"
            elif count >= 10 and index == 4:
                product_id = "PROD_MISSING"

            writer.writerow(
                [
                    f"ORD_{index:06d}",
                    customer_id,
                    product_id,
                    order_date,
                    amount,
                    rng.randint(1, 10),
                ]
            )
    logger.info("Generated %s order rows -> %s", count, path)
    return path


def generate_customer_cdc(
    output_dir: Path,
    customer_count: int,
    count: int,
) -> Path:
    path = output_dir / "customer_cdc_events.json"
    base_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    events = []
    for index in range(count):
        operation = ("UPDATE", "DELETE", "INSERT")[index % 3]
        is_insert = operation == "INSERT"
        customer_id = (
            f"CUST_NEW_{index:04d}" if is_insert else f"CUST_{index % max(customer_count, 1):05d}"
        )
        events.append(
            {
                "event_id": f"CDC_{index:05d}",
                "customer_id": customer_id,
                "operation": operation,
                "name": f"CDC Customer {index}" if operation != "DELETE" else None,
                "email": f"cdc{index}@example.com" if operation != "DELETE" else None,
                "age": 25 + index % 30 if operation != "DELETE" else None,
                "country": "USA" if operation != "DELETE" else None,
                "event_timestamp": (base_time + timedelta(minutes=index)).isoformat(),
                "source_system": "crm_cdc",
            }
        )

    # An exact duplicate proves event replay does not create an extra customer.
    if events:
        events.append(dict(events[0]))

    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, sort_keys=True) + "\n")
    logger.info("Generated %s CDC rows plus one replay -> %s", count, path)
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default="data/raw")
    parser.add_argument("--cdc-output-dir", default="data/cdc")
    parser.add_argument("--customers", type=int, default=1000)
    parser.add_argument("--products", type=int, default=500)
    parser.add_argument("--orders", type=int, default=5000)
    parser.add_argument("--cdc-events", type=int, default=50)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if min(args.customers, args.products, args.orders) <= 0 or args.cdc_events < 0:
        parser.error("record counts must be positive and cdc-events cannot be negative")

    raw_dir = Path(args.output_dir)
    cdc_dir = Path(args.cdc_output_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    cdc_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(args.seed)

    generate_customers(raw_dir, args.customers, rng)
    generate_products(raw_dir, args.products, rng)
    generate_orders(raw_dir, args.customers, args.products, args.orders, rng)
    generate_customer_cdc(cdc_dir, args.customers, args.cdc_events)
    logger.info("Synthetic data generation complete (seed=%s)", args.seed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
