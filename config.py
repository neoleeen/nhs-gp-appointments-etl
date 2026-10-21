from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

SOURCE_FOLDER = PROJECT_ROOT / "raw"
DATA_FOLDER = PROJECT_ROOT / "data"
RAW_FOLDER = DATA_FOLDER / "raw"
CLEAN_FOLDER = DATA_FOLDER / "clean"
LOG_FOLDER = PROJECT_ROOT / "logs"

SOURCE_FILES = {
    "regional": "appointments_regional.csv",
    "categories": "national_categories.csv",
    "duration": "actual_duration.csv.xlsx",
}

SPARK_SHUFFLE_PARTITIONS = 8
