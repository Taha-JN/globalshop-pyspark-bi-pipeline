"""
etl_pyspark.py

ETL script that reads synthetic CSVs from data/, performs cleaning, standardization,
and writes Parquet outputs to etl/output/.

If pyspark is available, it uses Spark (local mode). Otherwise it falls back to pandas.

Outputs:
- etl/output/online_retail_clean.parquet
- etl/output/orders_clean.parquet
- etl/output/events_clean.parquet
- etl/output/summary.json

Usage:
    python etl_pyspark.py --input ../data --output etl/output

"""
import argparse
import json
import os
import sys
from datetime import datetime

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, to_timestamp, udf, when, lit, year, month, dayofmonth
    SPARK_AVAILABLE = True
except Exception:
    SPARK_AVAILABLE = False

import pandas as pd
import numpy as np

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def write_log(outdir, text):
    with open(os.path.join(outdir, 'sample_etl_log.txt'), 'a', encoding='utf-8') as f:
        f.write(text + "\n")
    print(text)


def spark_etl(input_dir, output_dir):
    spark = SparkSession.builder.master("local[*]").appName("globalshop_etl_demo").getOrCreate()

    # Read CSVs
    online = spark.read.option("header", True).csv(os.path.join(input_dir, "online_retail_ii.csv"))
    orders = spark.read.option("header", True).csv(os.path.join(input_dir, "olist_orders.csv"))
    events = spark.read.option("header", True).csv(os.path.join(input_dir, "retailrocket_events.csv"))

    initial_counts = {
        'online_rows': online.count(),
        'orders_rows': orders.count(),
        'events_rows': events.count()
    }

    # Cleaning online: cast numeric fields, handle null CustomerID, remove obviously bad rows
    online2 = online.withColumn('Quantity', col('Quantity').cast('int')) \
                    .withColumn('UnitPrice', col('UnitPrice').cast('double'))

    # Derived field TotalPrice
    online2 = online2.withColumn('TotalPrice', col('Quantity') * col('UnitPrice'))

    # OrderDate normalized
    online2 = online2.withColumn('OrderDate', to_timestamp(col('InvoiceDate')))
    online2 = online2.withColumn('OrderMonth', month(col('OrderDate')))

    # Channel synthetic assignment
    online2 = online2.withColumn('Channel', when(col('Country') == 'United Kingdom', lit('web')).otherwise(lit('marketplace')))

    # Deduplicate
    before_dedup = online2.count()
    online2 = online2.dropDuplicates(['InvoiceNo','StockCode','InvoiceDate','CustomerID'])
    after_dedup = online2.count()

    # Save parquet
    online_out = os.path.join(output_dir, 'online_retail_clean.parquet')
    online2.write.mode('overwrite').parquet(online_out)

    # Orders ETL: parse timestamp, cast price
    orders2 = orders.withColumn('order_ts', to_timestamp(col('order_purchase_timestamp')))
    orders2 = orders2.withColumn('price', col('price').cast('double'))
    orders_out = os.path.join(output_dir, 'orders_clean.parquet')
    orders2.write.mode('overwrite').parquet(orders_out)

    # Events ETL: parse timestamp
    events2 = events.withColumn('event_ts', to_timestamp(col('timestamp')))
    events_out = os.path.join(output_dir, 'events_clean.parquet')
    events2.write.mode('overwrite').parquet(events_out)

    # Summary
    summary = {
        'initial_counts': initial_counts,
        'online_before_dedup': before_dedup,
        'online_after_dedup': after_dedup,
    }

    with open(os.path.join(output_dir, 'summary.json'), 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, default=str)

    write_log(output_dir, f"Spark ETL completed. Summary: {summary}")
    spark.stop()


def pandas_etl(input_dir, output_dir):
    # Read CSVs with pandas
    online = pd.read_csv(os.path.join(input_dir, 'online_retail_ii.csv'))
    orders = pd.read_csv(os.path.join(input_dir, 'olist_orders.csv'))
    events = pd.read_csv(os.path.join(input_dir, 'retailrocket_events.csv'))

    initial_counts = {
        'online_rows': len(online),
        'orders_rows': len(orders),
        'events_rows': len(events)
    }

    # Cleaning
    online['Quantity'] = pd.to_numeric(online['Quantity'], errors='coerce')
    online['UnitPrice'] = pd.to_numeric(online['UnitPrice'], errors='coerce')
    online['TotalPrice'] = online['Quantity'] * online['UnitPrice']
    online['OrderDate'] = pd.to_datetime(online['InvoiceDate'], errors='coerce')
    online['OrderMonth'] = online['OrderDate'].dt.to_period('M')
    online['Channel'] = np.where(online['Country'] == 'United Kingdom', 'web', 'marketplace')

    before_dedup = len(online)
    online = online.drop_duplicates(subset=['InvoiceNo','StockCode','InvoiceDate','CustomerID'])
    after_dedup = len(online)

    online_out = os.path.join(output_dir, 'online_retail_clean.parquet')
    online.to_parquet(online_out, index=False)

    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'], errors='coerce')
    orders.to_parquet(os.path.join(output_dir, 'orders_clean.parquet'), index=False)

    events['timestamp'] = pd.to_datetime(events['timestamp'], errors='coerce')
    events.to_parquet(os.path.join(output_dir, 'events_clean.parquet'), index=False)

    summary = {
        'initial_counts': initial_counts,
        'online_before_dedup': before_dedup,
        'online_after_dedup': after_dedup,
        'null_customer_count': int(online['CustomerID'].isnull().sum() if 'CustomerID' in online else 0),
        'records_with_negative_quantity': int((online['Quantity'] < 0).sum())
    }

    with open(os.path.join(output_dir, 'summary.json'), 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, default=str)

    write_log(output_dir, f"Pandas ETL completed. Summary: {summary}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, default='../data', help='Input data directory')
    parser.add_argument('--output', type=str, default='etl/output', help='ETL output directory')
    args = parser.parse_args()

    ensure_dir(args.output)

    # clear sample_etl_log
    open(os.path.join(args.output, 'sample_etl_log.txt'), 'w').close()

    write_log(args.output, f"Starting ETL at {datetime.utcnow().isoformat()}Z. Spark available={SPARK_AVAILABLE}")

    if SPARK_AVAILABLE:
        try:
            spark_etl(args.input, args.output)
            return
        except Exception as e:
            write_log(args.output, f"Spark ETL failed with {e}, falling back to pandas")

    pandas_etl(args.input, args.output)


if __name__ == '__main__':
    main()
