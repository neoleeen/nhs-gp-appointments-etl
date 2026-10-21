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
