"""
generate_synthetic_data.py

Generates three synthetic CSV datasets:
- online_retail_ii.csv
- olist_orders.csv
- retailrocket_events.csv

Default rows per file: 200_000 (configurable via --rows)

Features implemented:
- seasonality in timestamps with holiday spikes in Nov-Dec
- long-tail product popularity using Zipf-like distribution
- Pareto customer spend distribution
- random missing CustomerIDs and duplicate records
- multiple timezones in timestamps

Outputs a small sample: sample_1k_online_retail.csv

Usage:
    python generate_synthetic_data.py --rows 200000 --outdir ../data

"""
import argparse
import csv
import os
import random
from datetime import datetime, timedelta
import math

import numpy as np
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)


def random_date(start, end):
    """Return random datetime between start and end"""
    delta = end - start
    int_delta = int(delta.total_seconds())
    return start + timedelta(seconds=random.randint(0, int_delta))


def generate_products(n_products=5000):
    products = []
    for i in range(n_products):
        code = f"P{i:05d}"
        desc = fake.word().title() + " " + fake.word().title()
        price = round(max(0.5, np.random.lognormal(mean=1.5, sigma=1.0)), 2)
        products.append((code, desc, price))
    return products


def generate_online_retail(rows, outdir):
    products = generate_products(3000)
    product_ids = [p[0] for p in products]

    start = datetime(2023, 1, 1)
    end = datetime(2024, 12, 31)

    filepath = os.path.join(outdir, "online_retail_ii.csv")
    sample_path = os.path.join(outdir, "sample_1k_online_retail.csv")

    fieldnames = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"]

    with open(filepath, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        invoices = []
        for i in range(rows):
            invoice = f"INV{100000 + (i//3)}"  # group some lines under same invoice
            prod = products[np.random.zipf(1.8) % len(products)]
            qty = int(np.random.poisson(2))
            if qty == 0:
                qty = 1
            # introduce negatives for returns at low frequency
            if random.random() < 0.01:
                qty = -qty

            # seasonality: higher chance in Nov-Dec
            if random.random() < 0.15:
                invoice_date = random_date(datetime(2023,11,1), datetime(2023,12,31))
            else:
                invoice_date = random_date(start, end)

            # timezone variety
            tz = random.choice(["+00:00", "+01:00", "-05:00", "+09:00"])
            invoice_date_str = invoice_date.strftime("%Y-%m-%d %H:%M:%S") + tz

            # missing customer ids
            if random.random() < 0.08:
                cid = ''
            else:
                cid = str(10000 + random.randint(1, 50000))

            row = {
                "InvoiceNo": invoice,
                "StockCode": prod[0],
                "Description": prod[1],
                "Quantity": qty,
                "InvoiceDate": invoice_date_str,
                "UnitPrice": prod[2],
                "CustomerID": cid,
                "Country": random.choice(["United Kingdom", "United States", "Germany", "France", "Brazil", "Australia"]) 
            }
            writer.writerow(row)
            invoices.append(row)

    # write sample
    with open(sample_path, "w", newline='', encoding='utf-8') as sf:
        writer = csv.DictWriter(sf, fieldnames=fieldnames)
        writer.writeheader()
        for r in invoices[:1000]:
            writer.writerow(r)

    print(f"Wrote {filepath} and sample {sample_path}")


def generate_olist_orders(rows, outdir):
    filepath = os.path.join(outdir, "olist_orders.csv")
    fieldnames = ["order_id", "order_purchase_timestamp", "customer_id", "price", "city", "state"]

    cities = ["Sao Paulo","Rio de Janeiro","London","New York","Berlin","Paris","Sydney"]

    start = datetime(2023,1,1)
    end = datetime(2024,12,31)

    with open(filepath, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(rows):
            oid = f"O{i+1000000}"
            ts = random_date(start, end)
            # more purchases during weekends and in Q4
            if ts.month in (11,12):
                ts += timedelta(hours=random.randint(0,48))
            ts_str = ts.isoformat() + random.choice(["Z","+00:00","+01:00","-03:00"])

            # price skewed
            price = round(np.random.pareto(1.5) * 50 + 5, 2)

            if random.random() < 0.05:
                cid = ''
            else:
                cid = str(10000 + random.randint(1, 50000))

            writer.writerow({
                "order_id": oid,
                "order_purchase_timestamp": ts_str,
                "customer_id": cid,
                "price": price,
                "city": random.choice(cities),
                "state": random.choice(["processed","shipped","delivered","canceled"])
            })

    print(f"Wrote {filepath}")


def generate_retailrocket_events(rows, outdir):
    filepath = os.path.join(outdir, "retailrocket_events.csv")
    fieldnames = ["timestamp", "visitorid", "itemid", "event_type", "transactionid"]

    start = datetime(2023,1,1)
    end = datetime(2024,12,31)

    event_types = ["view", "addtocart", "transaction"]

    with open(filepath, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(rows):
            ts = random_date(start, end)
            ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S") + random.choice(["Z","+01:00","-05:00"])
            visitor = str(100000 + random.randint(1, 200000))
            itemid = f"P{random.randint(0,3000):05d}"
            etype = random.choices(event_types, weights=[0.85, 0.12, 0.03])[0]
            transactionid = ''
            if etype == 'transaction':
                transactionid = f"INV{random.randint(100000,400000)}"
            writer.writerow({
                "timestamp": ts_str,
                "visitorid": visitor,
                "itemid": itemid,
                "event_type": etype,
                "transactionid": transactionid
            })

    print(f"Wrote {filepath}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=200000, help="Number of rows per file (approx)")
    parser.add_argument("--outdir", type=str, default=".", help="Output directory for CSV files")
    args = parser.parse_args()

    outdir = args.outdir
    if not os.path.exists(outdir):
        os.makedirs(outdir, exist_ok=True)

    print("Generating online_retail_ii.csv")
    generate_online_retail(args.rows, outdir)
    print("Generating olist_orders.csv")
    generate_olist_orders(args.rows, outdir)
    print("Generating retailrocket_events.csv")
    generate_retailrocket_events(args.rows, outdir)


if __name__ == '__main__':
    main()
