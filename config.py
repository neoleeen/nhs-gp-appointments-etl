from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

SOURCE_FOLDER = PROJECT_ROOT / "raw"
DATA_FOLDER = PROJECT_ROOT / "data"
RAW_FOLDER = DATA_FOLDER / "raw"
CLEAN_FOLDER = DATA_FOLDER / "clean"
WAREHOUSE_FOLDER = PROJECT_ROOT / "warehouse"
WAREHOUSE_FILE = WAREHOUSE_FOLDER / "warehouse.duckdb"
LOG_FOLDER = PROJECT_ROOT / "logs"
DOCS_FOLDER = PROJECT_ROOT / "docs"

SOURCE_FILES = {
    "regional": "appointments_regional.csv",
    "categories": "national_categories.csv",
    "duration": "actual_duration.csv.xlsx",
}

EXPECTED = {
    "regional": {"raw_rows": 596_821, "rows": 295_692, "total": 742_804_525},
    "categories": {"raw_rows": 817_394, "rows": 817_394, "total": 296_046_770},
    "duration": {"raw_rows": 137_793, "rows": 137_793, "total": 167_980_692},
}

SPARK_SHUFFLE_PARTITIONS = 8
