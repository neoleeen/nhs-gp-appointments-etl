import logging
import time
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

log = logging.getLogger(__name__)

REGIONAL_SCHEMA = StructType([
    StructField("icb_ons_code", StringType()),
    StructField("appointment_month", StringType()),
    StructField("appointment_status", StringType()),
    StructField("hcp_type", StringType()),
    StructField("appointment_mode", StringType()),
    StructField("time_between_book_and_appointment", StringType()),
    StructField("count_of_appointments", LongType()),
])

CATEGORIES_SCHEMA = StructType([
    StructField("appointment_date", StringType()),
    StructField("icb_ons_code", StringType()),
    StructField("sub_icb_location_name", StringType()),
    StructField("service_setting", StringType()),
    StructField("context_type", StringType()),
    StructField("national_category", StringType()),
    StructField("count_of_appointments", LongType()),
    StructField("appointment_month", StringType()),
])

DURATION_SCHEMA = StructType([
    StructField("sub_icb_location_code", StringType()),
    StructField("sub_icb_location_ons_code", StringType()),
    StructField("sub_icb_location_name", StringType()),
    StructField("icb_ons_code", StringType()),
    StructField("region_ons_code", StringType()),
    StructField("appointment_date", StringType()),
    StructField("actual_duration", StringType()),
    StructField("count_of_appointments", LongType()),
])

def read_raw(spark, path, schema):
    return spark.read.csv(str(path), header=True, schema=schema, mode="FAILFAST")

def _tidy_text(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.regexp_replace(F.col(field.name), r"\s+", " ")))
    return df

def _rows_and_total(df):
    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum("count_of_appointments").alias("total"),
    ).first()
    return row["rows"], row["total"]

def _clean_regional(df):
    df = _tidy_text(df)
    df = (
        df
        .withColumn("hcp_type",
                    F.regexp_replace("hcp_type", "(?i)^other practice staff$", "Other Practice Staff"))
        .withColumn("month_start",
                    F.to_date(F.concat(F.col("appointment_month"), F.lit("-01")), "yyyy-MM-dd"))
    )
    keys = [c for c in df.columns if c != "count_of_appointments"]
    return df.groupBy(keys).agg(F.sum("count_of_appointments").alias("count_of_appointments"))


def _clean_categories(df):
    df = df.toDF(*[c.replace("\ufeff", "") for c in df.columns])
    df = _tidy_text(df)
    return (
        df
        .withColumn("appointment_day", F.to_date("appointment_date", "dd/MM/yyyy"))
        .withColumn("sub_icb_code",
                    F.regexp_extract("sub_icb_location_name", r"-\s*([A-Za-z0-9]+)\s*$", 1))
    )


def _clean_duration(df):
    df = _tidy_text(df)
    return (
        df
        .withColumn("appointment_day", F.to_date("appointment_date", "yyyy-MM-dd"))
        .withColumn("appointment_month", F.date_format("appointment_day", "yyyy-MM"))
    )


STEPS = {
    "regional": (REGIONAL_SCHEMA, _clean_regional),
    "categories": (CATEGORIES_SCHEMA, _clean_categories),
    "duration": (DURATION_SCHEMA, _clean_duration),
}


def transform(spark, raw_paths, clean_folder):
    """Clean the three raw CSVs and write them as Parquet partitioned by month.

    raw_paths: {"regional": path, "categories": path, "duration": path}
    Returns {name: path of the Parquet folder written}.
    """
    outputs = {}
    for name, (schema, clean) in STEPS.items():
        start = time.perf_counter()
        log.info("%s: started", name)

        raw = read_raw(spark, raw_paths[name], schema)
        rows_in, total_in = _rows_and_total(raw)
        log.info("%s: read %s rows, total %s", name, f"{rows_in:,}", f"{total_in:,}")

        out_path = Path(clean_folder) / name
        (clean(raw)
            .repartition("appointment_month")
            .write.mode("overwrite")
            .partitionBy("appointment_month")
            .parquet(str(out_path)))

        rows_out, total_out = _rows_and_total(spark.read.parquet(str(out_path)))
        log.info("%s: wrote %s rows, total %s to %s", name, f"{rows_out:,}", f"{total_out:,}", out_path)

        if total_out != total_in:
            raise ValueError(f"{name}: total changed from {total_in:,} to {total_out:,}")

        if name == "regional":
            if rows_out > rows_in:
                raise ValueError(f"{name}: rows grew from {rows_in:,} to {rows_out:,}")
        elif rows_out != rows_in:
            raise ValueError(f"{name}: rows changed from {rows_in:,} to {rows_out:,}")

        log.info("%s: finished in %.1f seconds", name, time.perf_counter() - start)
        outputs[name] = out_path
    return outputs
