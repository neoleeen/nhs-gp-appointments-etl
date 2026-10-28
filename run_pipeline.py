import json
import logging
import sys
import time
from pathlib import Path

import duckdb
from pyspark.sql import SparkSession

import config
from src.extract import extract
from src.transform import transform
from src.load import load
from src.check import run_checks, QualityCheckFailed

EXIT_OK = 0
EXIT_CRASHED = 1
EXIT_CHECKS_FAILED = 2

log = logging.getLogger("pipeline")


def setup_logging():
    config.LOG_FOLDER.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(config.LOG_FOLDER / "pipeline.log"),
        ],
    )


def build_spark():
    return (
        SparkSession.builder
        .appName("gp_appointments_pipeline")
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .getOrCreate()
    )


def run_stage(name, func, *args):
    start = time.perf_counter()
    log.info("Stage %s: started", name)
    result = func(*args)
    log.info("Stage %s: finished in %.1f seconds", name, time.perf_counter() - start)
    return result


def verify_extract(raw_paths):
    manifest = json.loads((config.RAW_FOLDER / "manifest.json").read_text())
    rows_by_file = {entry["file_name"]: entry["row_count"] for entry in manifest}
    found = []
    for name, path in raw_paths.items():
        rows = rows_by_file[Path(path).name]
        want = config.EXPECTED[name]["raw_rows"]
        if rows != want:
            raise ValueError(f"extract: {name} has {rows:,} rows, expected {want:,}")
        found.append(f"{rows:,}")
    log.info("extract OK %s", " / ".join(found))


def verify_transform(outputs):
    con = duckdb.connect()
    try:
        for name, path in outputs.items():
            rows, total = con.execute(
                f"SELECT COUNT(*), SUM(count_of_appointments) "
                f"FROM read_parquet('{path}/**/*.parquet', hive_partitioning = true)"
            ).fetchone()
            want = config.EXPECTED[name]
            if rows != want["rows"] or total != want["total"]:
                raise ValueError(
                    f"transform: {name} has {rows:,} rows, total {total:,}; "
                    f"expected {want['rows']:,} rows, total {want['total']:,}")
            log.info("transform OK %s: %s rows, %s appointments", name, f"{rows:,}", f"{total:,}")
    finally:
        con.close()


def main():
    setup_logging()
    start = time.perf_counter()
    log.info("Pipeline started")

    source_paths = {name: config.SOURCE_FOLDER / file for name, file in config.SOURCE_FILES.items()}
    report_file = config.DOCS_FOLDER / "quality_report.json"

    spark = None
    try:
        raw_paths = run_stage("extract", extract, source_paths, config.RAW_FOLDER)
        verify_extract(raw_paths)

        spark = build_spark()
        outputs = run_stage("transform", transform, spark, raw_paths, config.CLEAN_FOLDER)
        spark.stop()
        spark = None
        verify_transform(outputs)

        run_stage("load", load, config.CLEAN_FOLDER, config.WAREHOUSE_FILE, config.EXPECTED)
        log.info("load OK 3 facts, 6 dimensions")

        run_stage("checks", run_checks, config.WAREHOUSE_FILE, config.EXPECTED, report_file)
        log.info("checks OK 9/9 passed")

    except QualityCheckFailed as error:
        log.error("Stopped by failed quality check: %s", error)
        log.info("exit %s", EXIT_CHECKS_FAILED)
        return EXIT_CHECKS_FAILED
    except Exception:
        log.exception("Pipeline crashed")
        log.info("exit %s", EXIT_CRASHED)
        return EXIT_CRASHED
    finally:
        if spark is not None:
            spark.stop()

    log.info("Pipeline finished in %.1f seconds", time.perf_counter() - start)
    log.info("exit %s", EXIT_OK)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
