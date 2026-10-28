import logging
import time
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

DIMENSIONS = {
    "dim_date": """
        SELECT
            CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
            d                                      AS full_date,
            CAST(date_trunc('month', d) AS DATE)   AS month_start,
            strftime(d, '%Y-%m')                   AS appointment_month,
            year(d)                                AS year,
            month(d)                               AS month,
            strftime(d, '%B')                      AS month_name,
            isodow(d)                              AS weekday_number,
            strftime(d, '%A')                      AS weekday_name,
            isodow(d) IN (6, 7)                    AS is_weekend
        FROM (
            SELECT CAST(range AS DATE) AS d
            FROM range(DATE '2020-01-01', DATE '2022-07-01', INTERVAL 1 DAY)
        )
    """,
    "dim_geography": """
        SELECT DISTINCT
            sub_icb_location_code     AS sub_icb_code,
            sub_icb_location_ons_code AS sub_icb_ons_code,
            sub_icb_location_name     AS sub_icb_name,
            icb_ons_code,
            region_ons_code
        FROM {duration}
        ORDER BY sub_icb_code
    """,
    "dim_icb": """
        SELECT DISTINCT
            icb_ons_code,
            regexp_replace(sub_icb_name, '\\s*-\\s*[A-Za-z0-9]+$', '') AS icb_name,
            region_ons_code
        FROM dim_geography
        ORDER BY icb_ons_code
    """,
    "dim_booking_wait": """
        SELECT * FROM (VALUES
            ('Same Day',               1, TRUE,  FALSE),
            ('1 Day',                  2, TRUE,  FALSE),
            ('2 to 7 Days',            3, TRUE,  FALSE),
            ('8 to 14 Days',           4, TRUE,  FALSE),
            ('15 to 21 Days',          5, TRUE,  TRUE),
            ('22 to 28 Days',          6, TRUE,  TRUE),
            ('More than 28 Days',      7, TRUE,  TRUE),
            ('Unknown / Data Quality', 99, FALSE, FALSE)
        ) AS t(wait_band, sort_order, is_known, more_than_14_days)
    """,
    "dim_duration_band": """
        SELECT * FROM (VALUES
            ('1-5 Minutes',            1, TRUE,  FALSE),
            ('6-10 Minutes',           2, TRUE,  FALSE),
            ('11-15 Minutes',          3, TRUE,  FALSE),
            ('16-20 Minutes',          4, TRUE,  TRUE),
            ('21-30 Minutes',          5, TRUE,  TRUE),
            ('31-60 Minutes',          6, TRUE,  TRUE),
            ('Unknown / Data Quality', 99, FALSE, FALSE)
        ) AS t(duration_band, sort_order, is_known, over_15_minutes)
    """,
    "dim_national_category": """
        SELECT DISTINCT
            national_category,
            context_type,
            (national_category IN ('Unmapped', 'Inconsistent Mapping')
             OR context_type IN ('Unmapped', 'Inconsistent Mapping')) AS is_data_quality_issue
        FROM {categories}
        ORDER BY context_type, national_category
    """,
}

FACT_TABLES = {
    "fact_appointments_monthly": """
        CREATE TABLE IF NOT EXISTS fact_appointments_monthly (
            appointment_month     VARCHAR NOT NULL,
            date_key              INTEGER NOT NULL,
            month_start           DATE    NOT NULL,
            icb_ons_code          VARCHAR,
            appointment_status    VARCHAR,
            hcp_type              VARCHAR,
            appointment_mode      VARCHAR,
            wait_band             VARCHAR,
            count_of_appointments BIGINT  NOT NULL
        )""",
    "fact_appointments_by_category": """
        CREATE TABLE IF NOT EXISTS fact_appointments_by_category (
            appointment_month     VARCHAR NOT NULL,
            date_key              INTEGER NOT NULL,
            appointment_day       DATE    NOT NULL,
            sub_icb_code          VARCHAR,
            service_setting       VARCHAR,
            national_category     VARCHAR,
            count_of_appointments BIGINT  NOT NULL
        )""",
    "fact_appointments_by_duration": """
        CREATE TABLE IF NOT EXISTS fact_appointments_by_duration (
            appointment_month     VARCHAR NOT NULL,
            date_key              INTEGER NOT NULL,
            appointment_day       DATE    NOT NULL,
            sub_icb_code          VARCHAR,
            duration_band         VARCHAR,
            count_of_appointments BIGINT  NOT NULL
        )""",
}

FACTS = {
    "fact_appointments_monthly": ("regional", """
        appointment_month,
        CAST(strftime(month_start, '%Y%m%d') AS INTEGER),
        month_start,
        icb_ons_code,
        appointment_status,
        hcp_type,
        appointment_mode,
        time_between_book_and_appointment,
        count_of_appointments
    """),
    "fact_appointments_by_category": ("categories", """
        appointment_month,
        CAST(strftime(appointment_day, '%Y%m%d') AS INTEGER),
        appointment_day,
        sub_icb_code,
        service_setting,
        national_category,
        count_of_appointments
    """),
    "fact_appointments_by_duration": ("duration", """
        appointment_month,
        CAST(strftime(appointment_day, '%Y%m%d') AS INTEGER),
        appointment_day,
        sub_icb_location_code,
        actual_duration,
        count_of_appointments
    """),
}


def _load_fact(con, table, src, columns):
    """Delete then insert each month inside a transaction, so a rerun never doubles."""
    months = [r[0] for r in con.execute(f"SELECT DISTINCT appointment_month FROM {src} ORDER BY 1").fetchall()]
    for month in months:
        con.execute("BEGIN")
        try:
            con.execute(f"DELETE FROM {table} WHERE appointment_month = ?", [month])
            con.execute(f"INSERT INTO {table} SELECT {columns} FROM {src} WHERE appointment_month = ?", [month])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return len(months)


def load(clean_folder, warehouse_file, expected):
    """Build the star schema in DuckDB from the clean Parquet.

    clean_folder: folder holding the regional, categories and duration Parquet
    warehouse_file: path of the .duckdb file to build
    expected: {"regional": {"rows": ..., "total": ...}, ...}
    Returns the warehouse file path.
    """
    start = time.perf_counter()
    warehouse_file = Path(warehouse_file)
    warehouse_file.parent.mkdir(parents=True, exist_ok=True)

    sources = {
        name: f"read_parquet('{Path(clean_folder) / name}/**/*.parquet', hive_partitioning = true)"
        for name in ("regional", "categories", "duration")
    }

    con = duckdb.connect(str(warehouse_file))
    try:
        for table, sql in DIMENSIONS.items():
            con.execute(f"CREATE OR REPLACE TABLE {table} AS {sql.format(**sources)}")
            rows = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            log.info("%s: %s rows", table, f"{rows:,}")

        for ddl in FACT_TABLES.values():
            con.execute(ddl)

        for table, (name, columns) in FACTS.items():
            months = _load_fact(con, table, sources[name], columns)
            rows, total = con.execute(
                f"SELECT COUNT(*), SUM(count_of_appointments) FROM {table}").fetchone()
            exp = expected[name]
            if rows != exp["rows"] or total != exp["total"]:
                raise ValueError(
                    f"{table}: {rows:,} rows, total {total:,} "
                    f"!= expected {exp['rows']:,} rows, total {exp['total']:,}")
            log.info("%s: %s months, %s rows, total %s", table, months, f"{rows:,}", f"{total:,}")
    finally:
        con.close()

    log.info("Load finished in %.1f seconds", time.perf_counter() - start)
    return warehouse_file
