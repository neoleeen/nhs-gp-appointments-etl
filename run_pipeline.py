import logging
import sys
import time

from pyspark.sql import SparkSession

import config
from src.extract import extract
from src.transform import transform


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


def main():
    setup_logging()
    log = logging.getLogger("pipeline")
    start = time.perf_counter()
    log.info("Pipeline started")

    source_paths = {name: config.SOURCE_FOLDER / file for name, file in config.SOURCE_FILES.items()}

    spark = None
    try:
        log.info("Stage 1/2: extract")
        raw_paths = extract(source_paths, config.RAW_FOLDER)

        log.info("Stage 2/2: transform")
        spark = build_spark()
        outputs = transform(spark, raw_paths, config.CLEAN_FOLDER)
        log.info("Transform wrote: %s", ", ".join(str(p) for p in outputs.values()))
    except Exception:
        log.exception("Pipeline failed")
        sys.exit(1)
    finally:
        if spark is not None:
            spark.stop()

    log.info("Pipeline finished in %.1f seconds", time.perf_counter() - start)


if __name__ == "__main__":
    main()
