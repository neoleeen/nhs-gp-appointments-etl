import json
import shutil

import duckdb
import pytest

import config
from src.load import load
from src.check import run_checks, QualityCheckFailed

FACT_SOURCES = {
    "fact_appointments_monthly": "regional",
    "fact_appointments_by_category": "categories",
    "fact_appointments_by_duration": "duration",
}


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    path = tmp_path_factory.mktemp("warehouse") / "warehouse.duckdb"
    load(config.CLEAN_FOLDER, path, config.EXPECTED)
    return path


@pytest.fixture
def con(warehouse):
    connection = duckdb.connect(str(warehouse))
    yield connection
    connection.close()


def fact_totals(connection):
    return {
        table: connection.execute(
            f"SELECT COUNT(*), SUM(count_of_appointments) FROM {table}").fetchone()
        for table in FACT_SOURCES
    }


# 1. Loading twice changes nothing

def test_loading_twice_changes_nothing(warehouse):
    connection = duckdb.connect(str(warehouse))
    before = fact_totals(connection)
    connection.close()

    load(config.CLEAN_FOLDER, warehouse, config.EXPECTED)

    connection = duckdb.connect(str(warehouse))
    after = fact_totals(connection)
    connection.close()
    assert after == before


# 2. Each fact total equals the EXPECTED constant

def test_fact_totals_equal_expected(con):
    for table, (rows, total) in fact_totals(con).items():
        want = config.EXPECTED[FACT_SOURCES[table]]
        assert rows == want["rows"], f"{table}: {rows:,} rows"
        assert total == want["total"], f"{table}: total {total:,}"


# 3. No orphan geography codes

def test_no_orphan_geography_codes(con):
    orphans = con.execute("""
        SELECT
            (SELECT COUNT(*) FROM fact_appointments_by_category
             WHERE sub_icb_code NOT IN (SELECT sub_icb_code FROM dim_geography))
          + (SELECT COUNT(*) FROM fact_appointments_by_duration
             WHERE sub_icb_code NOT IN (SELECT sub_icb_code FROM dim_geography))
          + (SELECT COUNT(*) FROM fact_appointments_monthly
             WHERE icb_ons_code NOT IN (SELECT icb_ons_code FROM dim_icb))
    """).fetchone()[0]
    assert orphans == 0


# 4. Every label in a fact exists in its dimension

@pytest.mark.parametrize("fact, column, dimension, key", [
    ("fact_appointments_monthly", "wait_band", "dim_booking_wait", "wait_band"),
    ("fact_appointments_by_duration", "duration_band", "dim_duration_band", "duration_band"),
    ("fact_appointments_by_category", "national_category", "dim_national_category", "national_category"),
])
def test_every_label_is_in_its_dimension(con, fact, column, dimension, key):
    unknown = con.execute(f"""
        SELECT DISTINCT {column} FROM {fact}
        WHERE {column} NOT IN (SELECT {key} FROM {dimension})
    """).fetchall()
    assert unknown == []


# 5. dim_date: one row per day, no gaps, no duplicates

def test_dim_date_has_every_day_once(con):
    rows, distinct_days, span_days = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT full_date),
               date_diff('day', MIN(full_date), MAX(full_date)) + 1
        FROM dim_date
    """).fetchone()
    assert rows == distinct_days == span_days == 912


# 6. A broken warehouse makes the checks fail

def test_broken_warehouse_fails_the_checks(warehouse, tmp_path):
    broken = tmp_path / "broken.duckdb"
    shutil.copy(warehouse, broken)

    connection = duckdb.connect(str(broken))
    connection.execute("DELETE FROM fact_appointments_monthly WHERE appointment_month = '2021-08'")
    connection.execute("UPDATE fact_appointments_by_category SET sub_icb_code = 'XXX' WHERE appointment_month = '2022-01'")
    connection.close()

    report = tmp_path / "quality_report.json"
    with pytest.raises(QualityCheckFailed):
        run_checks(broken, config.EXPECTED, report)

    status = {r["check"].split(".")[0]: r["status"]
              for r in json.loads(report.read_text())["checks"]}
    assert status["1"] == "FAIL"
    assert status["4"] == "FAIL"
    assert status["5"] == "FAIL"
