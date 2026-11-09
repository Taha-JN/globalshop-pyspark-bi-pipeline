"""
analysis.py

Loads ETL outputs (Parquet), runs example queries using pandas, and produces charts.
Generates PNGs: monthly_sales.png, top_products.png, sales_by_region.png, customer_segments.png, kpi_card.png

Usage:
    python analysis/analysis.py --input ../etl/output --output analysis/output

"""
import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set(style='whitegrid')


def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def load_parquet_or_csv(path):
    if path.endswith('.parquet') and os.path.exists(path):
        return pd.read_parquet(path)
    # fallback: try CSV
    csv_path = path.replace('.parquet', '.csv')
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path)
    raise FileNotFoundError(path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, default='../etl/output', help='ETL output directory')
    parser.add_argument('--output', type=str, default='analysis/output', help='Analysis output directory')
    args = parser.parse_args()

    ensure_dir(args.output)

    # Load cleaned online retail
    online_path = os.path.join(args.input, 'online_retail_clean.parquet')
    orders_path = os.path.join(args.input, 'orders_clean.parquet')

    if os.path.exists(online_path):
        online = pd.read_parquet(online_path)
    else:
        # try csv fallback
        online = pd.read_csv(os.path.join(args.input, 'online_retail_ii.csv'))

    # quick aggregation: monthly sales
    if 'OrderDate' in online.columns:
        online['OrderDate'] = pd.to_datetime(online['OrderDate'], errors='coerce')
    elif 'InvoiceDate' in online.columns:
        online['OrderDate'] = pd.to_datetime(online['InvoiceDate'], errors='coerce')

    online['TotalPrice'] = pd.to_numeric(online.get('TotalPrice', online.get('Quantity',0) * online.get('UnitPrice',0)), errors='coerce')
    monthly = online.groupby(online['OrderDate'].dt.to_period('M'))['TotalPrice'].sum().reset_index()
    monthly['month'] = monthly['OrderDate'].dt.to_timestamp()

    plt.figure(figsize=(10,5))
    sns.lineplot(data=monthly, x='month', y='TotalPrice', marker='o')
    plt.title('Monthly Sales Trend')
    plt.xlabel('Month')
    plt.ylabel('Revenue')
    plt.tight_layout()
    monthly_path = os.path.join(args.output, 'monthly_sales.png')
    plt.savefig(monthly_path)

    # Top products by revenue
    prod = online.groupby(['StockCode','Description'])['TotalPrice'].sum().reset_index()
    top10 = prod.sort_values('TotalPrice', ascending=False).head(10)
    plt.figure(figsize=(10,6))
    sns.barplot(data=top10, x='TotalPrice', y='Description', palette='viridis')
    plt.title('Top 10 Products by Revenue')
    plt.xlabel('Revenue')
    plt.ylabel('Product')
    plt.tight_layout()
    top_products_path = os.path.join(args.output, 'top_products.png')
    plt.savefig(top_products_path)

    # Sales by region
    if 'Country' in online.columns:
        region = online.groupby('Country')['TotalPrice'].sum().reset_index()
        plt.figure(figsize=(8,6))
        sns.barplot(data=region.sort_values('TotalPrice', ascending=False), x='TotalPrice', y='Country')
        plt.title('Sales by Country')
        plt.xlabel('Revenue')
        plt.ylabel('Country')
        plt.tight_layout()
        region_path = os.path.join(args.output, 'sales_by_region.png')
        plt.savefig(region_path)

    # Customer segmentation chart (from sample CSV in sql outputs)
    seg_path = os.path.join('..','sql','queries','outputs','q3_customer_segmentation.csv')
    if os.path.exists(seg_path):
        seg = pd.read_csv(seg_path)
        seg_counts = seg['segment'].value_counts().reset_index()
        seg_counts.columns = ['segment','count']
        plt.figure(figsize=(6,6))
        plt.pie(seg_counts['count'], labels=seg_counts['segment'], autopct='%1.1f%%')
        plt.title('Customer Segments')
        plt.tight_layout()
        seg_path_out = os.path.join(args.output, 'customer_segments.png')
        plt.savefig(seg_path_out)

    # KPI card (simple text plot)
    total_revenue = monthly['TotalPrice'].sum()
    avg_order = online['TotalPrice'].mean()
    kpi_fig = plt.figure(figsize=(6,3))
    kpi_fig.text(0.1, 0.6, f"Total Revenue:\n${total_revenue:,.2f}", fontsize=14)
    kpi_fig.text(0.6, 0.6, f"Avg Order:\n${avg_order:,.2f}", fontsize=14)
    kpi_path = os.path.join(args.output, 'kpi_card.png')
    plt.axis('off')
    plt.savefig(kpi_path)

    # Save some CSV outputs for the queries
    top10[['StockCode','Description','TotalPrice']].rename(columns={'TotalPrice':'revenue'}).to_csv(os.path.join(args.output, 'q1_top_products_by_revenue.csv'), index=False)
    monthly[['month','TotalPrice']].rename(columns={'TotalPrice':'monthly_revenue'}).to_csv(os.path.join(args.output, 'q2_monthly_sales_trend.csv'), index=False)

    print('Analysis complete. Outputs in', args.output)


if __name__ == '__main__':
    main()
