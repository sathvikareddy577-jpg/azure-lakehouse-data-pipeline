"""Generate synthetic retail data for the lakehouse pipeline"""

import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
import argparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_customers(output_dir: str, num_records: int = 1000):
    """Generate synthetic customer data."""
    countries = ["USA", "UK", "Canada", "Australia", "Germany", "France", "Japan", "Brazil"]
    
    customers_csv = Path(output_dir) / "customers.csv"
    
    with open(customers_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "name", "email", "age", "country"])
        
        for i in range(num_records):
            customer_id = f"CUST_{i:05d}"
            name = f"Customer {i}"
            email = f"customer{i}@example.com"
            age = random.randint(18, 80)
            country = random.choice(countries)
            
            # Add some nulls (2%)
            if random.random() < 0.02:
                email = ""
            
            writer.writerow([customer_id, name, email, age, country])
    
    logger.info(f"✓ Generated {num_records} customers → {customers_csv}")


def generate_products(output_dir: str, num_records: int = 500):
    """Generate synthetic product data."""
    categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Beauty", "Food"]
    
    products_csv = Path(output_dir) / "products.csv"
    
    with open(products_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "product_name", "category", "unit_price"])
        
        for i in range(num_records):
            product_id = f"PROD_{i:05d}"
            product_name = f"Product {i}"
            category = random.choice(categories)
            unit_price = round(random.uniform(10, 500), 2)
            
            writer.writerow([product_id, product_name, category, unit_price])
    
    logger.info(f"✓ Generated {num_records} products → {products_csv}")


def generate_orders(output_dir: str, num_customers: int = 1000, num_products: int = 500, num_orders: int = 5000):
    """Generate synthetic order data."""
    orders_csv = Path(output_dir) / "orders.csv"
    base_date = datetime(2023, 1, 1)
    
    with open(orders_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "product_id", "order_date", "order_amount", "order_quantity"])
        
        for i in range(num_orders):
            order_id = f"ORD_{i:06d}"
            customer_id = f"CUST_{random.randint(0, num_customers-1):05d}"
            product_id = f"PROD_{random.randint(0, num_products-1):05d}"
            order_date = (base_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
            order_amount = round(random.uniform(50, 5000), 2)
            order_quantity = random.randint(1, 10)
            
            # Add some invalid dates (0.1%)
            if random.random() < 0.001:
                order_date = "invalid-date"
            
            # Add some negative amounts (0.1%)
            if random.random() < 0.001:
                order_amount = -100
            
            writer.writerow([order_id, customer_id, product_id, order_date, order_amount, order_quantity])
    
    logger.info(f"✓ Generated {num_orders} orders → {orders_csv}")


def generate_cdc_events(output_dir: str, num_events: int = 50):
    """Generate synthetic CDC events for customers."""
    cdc_json = Path(output_dir) / "customer_cdc_events.json"
    
    events = []
    operations = ["INSERT", "UPDATE", "DELETE"]
    
    for i in range(num_events):
        event = {
            "event_id": f"CDC_{i:05d}",
            "customer_id": f"CUST_{random.randint(0, 1000):05d}",
            "operation": random.choice(operations),
            "name": f"Updated Name {i}" if random.random() > 0.5 else None,
            "email": f"newemail{i}@example.com" if random.random() > 0.5 else None,
            "timestamp": datetime.utcnow().isoformat(),
        }
        events.append(event)
    
    # Add a duplicate event (for idempotency testing)
    if events:
        events.append(events[0])
    
    with open(cdc_json, "w") as f:
        json.dump(events, f, indent=2)
    
    logger.info(f"✓ Generated {num_events} CDC customer events → {cdc_json}")


def main():
    """Generate all synthetic data."""
    parser = argparse.ArgumentParser(description="Generate synthetic retail data")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/raw",
        help="Output directory for generated files"
    )
    parser.add_argument("--customers", type=int, default=1000, help="Number of customers")
    parser.add_argument("--products", type=int, default=500, help="Number of products")
    parser.add_argument("--orders", type=int, default=5000, help="Number of orders")
    parser.add_argument("--cdc-events", type=int, default=50, help="Number of CDC events")
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    cdc_dir = Path("data/cdc")
    cdc_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate data
    generate_customers(str(output_dir), args.customers)
    generate_products(str(output_dir), args.products)
    generate_orders(str(output_dir), args.customers, args.products, args.orders)
    generate_cdc_events(str(cdc_dir), args.cdc_events)
    
    logger.info("✓ Synthetic data generation complete!")


if __name__ == "__main__":
    main()
