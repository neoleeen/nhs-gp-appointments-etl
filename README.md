# NHS GP Appointments ETL Pipeline

A data pipeline that extracts, cleans and loads NHS England GP appointment data,
built during the LSA Data Engineering Internship (October 2026).

Source: NHS England, Appointments in General Practice (digital.nhs.uk).
Data files are not stored in this repository.

## How to run

    pip install -r requirements.txt
    python run_pipeline.py
    python -m pytest tests

Put the three source files in a `raw/` folder first. All paths are set in `config.py`.

## Week 1: Extract

`src/extract.py` copies the three source files into `data/raw/`, converts the duration file
from Excel to CSV, and writes `manifest.json` (file name, size, checksum, row count and time).
The data profile in `docs/data_profile.md` records each file's columns, grain and the five data traps.

## Week 2: Transform

`src/transform.py` cleans the three files and writes them as Parquet, partitioned by month.

| Dataset | Rows in | Rows out | Total appointments |
|---|---:|---:|---:|
| regional | 596,821 | 295,692 | 742,804,525 |
| categories | 817,394 | 817,394 | 296,046,770 |
| duration | 137,793 | 137,793 | 167,980,692 |

| Storage | Source CSV | Parquet |
|---|---:|---:|
| Total | 157.1 MB | 4.1 MB |

13 pytest tests guard every cleaning rule. Decisions are in `docs/decision_log.md`
and the warehouse design is in `docs/star_schema.md`.

## Week 3: Load and checks (DuckDB)

`python run_pipeline.py` now runs four stages in order: extract, transform, load and checks.
The warehouse is built in `warehouse/warehouse.duckdb` (not in Git; the pipeline rebuilds it).

| Exit code | Meaning |
|---|---|
| 0 | Loaded and all checks passed |
| 2 | Stopped by a failed quality check |
| 1 | Crashed |

The star schema has three fact tables (monthly, by category, by duration) and six dimensions
(date, geography, ICB, booking wait, duration band, national category). The load replaces each
month inside a transaction, so reruns never double the data: three runs in a row gave identical totals.

Nine quality checks run after every load and are written to `docs/quality_report.json`.
All nine pass. 24 pytest tests cover the clean data and the warehouse.
