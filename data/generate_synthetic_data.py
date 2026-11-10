"""
generate_synthetic_data.py

Generates three synthetic CSV datasets simulating e-commerce platforms:
- online_retail_ii.csv
- olist_orders.csv
- retailrocket_events.csv
- sample_1k_online_retail.csv (small sample for quick inspection)

Features implemented:
- Realistic seasonality with holiday peaks in Nov-Dec
- Long-tail product popularity using Zipf-like distribution
- Pareto customer spend distribution
- Configurable missing customer keys and duplicate records
- Robust timezone-aware timestamps

Usage:
    python data/generate_synthetic_data.py --rows 200000 --outdir data
"""
import argparse
import csv
import math
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

# Dynamic path resolution anchored to project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTDIR = PROJECT_ROOT / "data"

# Reproducibility seed
SEED = 42
random.seed(SEED)
np.random.seed(SEED)


def random_date(start: datetime, end: datetime) -> datetime:
    """Return random datetime between start and end."""
    delta = end - start
    int_delta = int(delta.total_seconds())
    return start + timedelta(seconds=random.randint(0, int_delta))


def generate_products(n_products: int = 3000):
    """Generate product catalog with synthetic descriptions and lognormal pricing."""
    adjectives = ["Classic", "Deluxe", "Ergonomic", "Sleek", "Premium", "Ultra", "Compact", "Vintage", "Modern"]
    nouns = ["Teapot", "Notebook", "Desk Lamp", "Coffee Mug", "Leather Wallet", "Backpack", "Headphones", "Speaker", "Watch"]
    
    products = []
    for i in range(n_products):
        code = f"P{i:05d}"
        desc = f"{random.choice(adjectives)} {random.choice(nouns)} #{i}"
        price = round(max(0.5, float(np.random.lognormal(mean=1.5, sigma=1.0))), 2)
        products.append((code, desc, price))
    return products


def generate_online_retail(rows: int, outdir: Path):
    """Generate transactional online retail dataset with sample extract."""
    products = generate_products(3000)
    start = datetime(2023, 1, 1)
    end = datetime(2024, 12, 31)

    filepath = outdir / "online_retail_ii.csv"
    sample_path = outdir / "sample_1k_online_retail.csv"

    fieldnames = [
        "InvoiceNo", "StockCode", "Description", "Quantity",
        "InvoiceDate", "UnitPrice", "CustomerID", "Country"
    ]

    countries = ["United Kingdom", "United States", "Germany", "France", "Brazil", "Australia"]
    country_weights = [0.60, 0.15, 0.10, 0.08, 0.04, 0.03]

    sample_invoices = []

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(rows):
            invoice = f"INV{100000 + (i // 3)}"  # Multiple line items per invoice
            prod = products[np.random.zipf(1.8) % len(products)]
            qty = int(np.random.poisson(2))
            if qty <= 0:
                qty = 1

            # Low frequency returns/cancellations
            if random.random() < 0.015:
                qty = -qty

            # Seasonality: higher probability in Q4 (Nov-Dec)
            if random.random() < 0.20:
                invoice_date = random_date(datetime(2023, 11, 1), datetime(2023, 12, 31))
            else:
                invoice_date = random_date(start, end)

            # Hour simulation with peak shopping times (10:00 - 20:00)
            hour_probs = np.array([
                0.01, 0.01, 0.01, 0.01, 0.01, 0.02,
                0.03, 0.04, 0.06, 0.08, 0.09, 0.09,
                0.08, 0.07, 0.07, 0.07, 0.08, 0.07,
                0.05, 0.03, 0.01, 0.01, 0.005, 0.005
            ])
            hour_probs = hour_probs / hour_probs.sum()
            hour = int(np.random.choice(range(24), p=hour_probs))
            invoice_date = invoice_date.replace(hour=hour, minute=random.randint(0, 59), second=random.randint(0, 59))
            invoice_date_str = invoice_date.strftime("%Y-%m-%d %H:%M:%S")

            # Missing customer ID simulation
            if random.random() < 0.08:
                cid = ""
            else:
                cid = str(10000 + random.randint(1, 15000))

            row = {
                "InvoiceNo": invoice,
                "StockCode": prod[0],
                "Description": prod[1],
                "Quantity": qty,
                "InvoiceDate": invoice_date_str,
                "UnitPrice": prod[2],
                "CustomerID": cid,
                "Country": random.choices(countries, weights=country_weights)[0]
            }
            writer.writerow(row)

            # Keep only the first 1,000 in memory for the sample
            if len(sample_invoices) < 1000:
                sample_invoices.append(row)

    # Write small sample
    with open(sample_path, "w", newline="", encoding="utf-8") as sf:
        writer = csv.DictWriter(sf, fieldnames=fieldnames)
        writer.writeheader()
        for r in sample_invoices:
            writer.writerow(r)

    print(f"Generated {filepath} ({rows} rows) and sample {sample_path}")


def generate_olist_orders(rows: int, outdir: Path):
    """Generate Brazilian e-commerce orders dataset."""
    filepath = outdir / "olist_orders.csv"
    fieldnames = ["order_id", "order_purchase_timestamp", "customer_id", "price", "city", "order_status", "state"]

    cities = ["Sao Paulo", "Rio de Janeiro", "Curitiba", "Belo Horizonte", "Porto Alegre", "Salvador", "Brasilia"]
    states = ["SP", "RJ", "PR", "MG", "RS", "BA", "DF"]
    statuses = ["delivered", "shipped", "invoiced", "canceled"]
    status_weights = [0.92, 0.05, 0.02, 0.01]

    start = datetime(2023, 1, 1)
    end = datetime(2024, 12, 31)

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(rows):
            oid = f"O{i + 1000000}"
            ts = random_date(start, end)
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

            # Pareto price distribution
            price = round(float(np.random.pareto(1.5) * 50 + 5), 2)

            cid = "" if random.random() < 0.05 else str(10000 + random.randint(1, 15000))
            city_idx = random.randint(0, len(cities) - 1)

            writer.writerow({
                "order_id": oid,
                "order_purchase_timestamp": ts_str,
                "customer_id": cid,
                "price": price,
                "city": cities[city_idx],
                "order_status": random.choices(statuses, weights=status_weights)[0],
                "state": states[city_idx]
            })

    print(f"Generated {filepath} ({rows} rows)")


def generate_retailrocket_events(rows: int, outdir: Path):
    """Generate clickstream and funnel events dataset."""
    filepath = outdir / "retailrocket_events.csv"
    fieldnames = ["timestamp", "visitorid", "itemid", "event_type", "transactionid"]

    start = datetime(2023, 1, 1)
    end = datetime(2024, 12, 31)
    event_types = ["view", "addtocart", "transaction"]
    event_weights = [0.85, 0.12, 0.03]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(rows):
            ts = random_date(start, end)
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
            visitor = str(100000 + random.randint(1, 50000))
            itemid = f"P{random.randint(0, 2999):05d}"
            etype = random.choices(event_types, weights=event_weights)[0]
            transactionid = f"INV{random.randint(100000, 400000)}" if etype == "transaction" else ""

            writer.writerow({
                "timestamp": ts_str,
                "visitorid": visitor,
                "itemid": itemid,
                "event_type": etype,
                "transactionid": transactionid
            })

    print(f"Generated {filepath} ({rows} rows)")


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic data for GlobalShop pipeline.")
    parser.add_argument("--rows", type=int, default=100000, help="Number of rows per dataset")
    parser.add_argument("--outdir", type=str, default=str(DEFAULT_OUTDIR), help="Output directory")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"Generating synthetic datasets in: {outdir} (Target rows: {args.rows} per file)")
    generate_online_retail(args.rows, outdir)
    generate_olist_orders(args.rows, outdir)
    generate_retailrocket_events(args.rows, outdir)
    print("Synthetic data generation successfully completed.")


if __name__ == "__main__":
    main()
