# GlobalShop Demo — globalshop_demo_v1

This repository is a reproducible, presentation-ready demo of a Big Data project for GlobalShop Inc. It uses synthetic data and a lightweight local ETL + analytical stack to simulate a production pipeline (ETL -> Data Model -> Queries -> Dashboard).

IMPORTANT: Data and results are synthetic and produced for demonstration only. They are realistic in distribution and shape but do not represent real customers.

## Contents (top-level)

- `data/` - synthetic data generator and small sample files
- `etl/` - PySpark ETL script (with pandas fallback), ETL logs, and output examples
- `sql/` - schema DDL, load examples, and production-style query scripts + sample CSV outputs
- `analysis/` - analysis script that runs queries and creates charts (matplotlib)
- `ssas/` - SSAS tabular model notes and DAX measures
- `powerbi/` - step-by-step Power BI recreation instructions
- `report/` - demo report text to save as a Word document
- `deliverables_manifest.json` - generated manifest with checksums and sizes

## Quick setup

Requirements (recommended):
- Python 3.9+ (3.11 tested in virtualenv included in the workspace)
- pip packages: pandas, numpy, pyarrow, faker, matplotlib, seaborn, python-dateutil
- Optional for Spark path: pyspark (local mode)

Install required packages (recommended in a venv):

```powershell
python -m venv .venv
; .\.venv\Scripts\Activate.ps1
; pip install --upgrade pip
; pip install pandas numpy pyarrow faker matplotlib seaborn python-dateutil
; pip install pyspark  # optional, for PySpark path
```

## Run order (recommended)

1. Generate synthetic data (default ~200000 rows per file):
   `python data/generate_synthetic_data.py --rows 200000`

2. Run ETL (PySpark preferred if installed, otherwise the script falls back to pandas):
   `python etl/etl_pyspark.py --input data --output etl/output`

3. Run analysis to execute queries and produce charts:
   `python analysis/analysis.py --input etl/output --output analysis/output`

4. Inspect SQL in `sql/` and run against your data warehouse if desired.

## What to expect

- Parquet outputs in `etl/output/` representing cleaned facts/dimensions.
- `etl/output/summary.json` with ETL metrics (row counts, retention rates).
- CSV exports for 5 example queries in `sql/queries/outputs/` and `analysis/output/`.
- PNG chart files in `analysis/output/`.

## Files of interest (quick pointers)
- `data/generate_synthetic_data.py` — configurable data generator (seeded for reproducibility)
- `etl/etl_pyspark.py` — ETL (cleaning, standardization, derived fields, Parquet write)
- `sql/create_schema.sql` — star schema DDL and sample load commands
- `analysis/analysis.py` — loads parquet/CSV, runs SQL-like queries, and renders charts

## Reproducibility notes

- All scripts accept command-line arguments to control input/output paths and row counts.
- Generation uses a fixed seed by default to create repeatable synthetic datasets.

## Disclaimer

Data and results are synthetic and produced for demonstration; code and pipeline simulate production practices. Use with care if presenting as internal or customer-facing artifacts.

---

For full run instructions and developer notes see the detailed sections inside each folder.
