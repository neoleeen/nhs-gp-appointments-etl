import os
import pytest
from pyspark.sql import SparkSession

import config

CLEAN_FOLDER = os.environ.get("CLEAN_FOLDER", str(config.CLEAN_FOLDER))


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("tests")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def regional(spark):
    return spark.read.parquet(f"{CLEAN_FOLDER}/regional")


@pytest.fixture(scope="session")
def categories(spark):
    return spark.read.parquet(f"{CLEAN_FOLDER}/categories")


@pytest.fixture(scope="session")
def duration(spark):
    return spark.read.parquet(f"{CLEAN_FOLDER}/duration")
