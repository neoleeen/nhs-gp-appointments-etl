import json
import logging
from datetime import datetime
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

OVERLAP = ("2021-08", "2022-06")
MIN_DURATION_COVERAGE = 0.90


class QualityCheckFailed(Exception):
    """Raised when one or more quality checks fail."""


def _checks(con, expected):
    results = []

    def add(name, passed, detail):
        results.append({"check": name, "status": "PASS" if passed else "FAIL", "detail": detail})

    for number, (table, source) in enumerate([
        ("fact_appointments_monthly", "regional"),
        ("fact_appointments_by_category", "categories"),
        ("fact_appointments_by_duration", "duration"),
    ], start=1):
        total = con.execute(f"SELECT SUM(count_of_appointments) FROM {table}").fetchone()[0]
        want = expected[source]["total"]
        add(f"{number}. {table} total matches transform",
            total == want, f"warehouse {total:,} vs expected {want:,}")

    differ, compared = con.execute("""
        WITH r AS (
            SELECT appointment_month AS m, SUM(count_of_appointments) AS total
            FROM fact_appointments_monthly
            WHERE appointment_month BETWEEN ? AND ?
            GROUP BY 1),
        c AS (
            SELECT appointment_month AS m, SUM(count_of_appointments) AS total
            FROM fact_appointments_by_category
            WHERE appointment_month BETWEEN ? AND ?
            GROUP BY 1)
        SELECT COUNT(*) FILTER (WHERE r.total IS DISTINCT FROM c.total), COUNT(*)
        FROM r FULL OUTER JOIN c USING (m)
    """, [*OVERLAP, *OVERLAP]).fetchone()
    add("4. Regional and category monthly totals reconcile, Aug 2021 - Jun 2022",
        differ == 0 and compared == 11, f"{differ} of {compared} months differ")

    orphans = con.execute("""
        SELECT
            (SELECT COUNT(*) FROM fact_appointments_by_category
             WHERE sub_icb_code NOT IN (SELECT sub_icb_code FROM dim_geography))
          + (SELECT COUNT(*) FROM fact_appointments_by_duration
             WHERE sub_icb_code NOT IN (SELECT sub_icb_code FROM dim_geography))
    """).fetchone()[0]
    add("5. Every sub-ICB code in the category and duration facts exists in dim_geography",
        orphans == 0, f"{orphans:,} orphan rows")

    unknown_wait = [r[0] for r in con.execute("""
        SELECT DISTINCT wait_band FROM fact_appointments_monthly
        WHERE wait_band NOT IN (SELECT wait_band FROM dim_booking_wait)
    """).fetchall()]
    add("6. Every booking-wait label is a known band",
        not unknown_wait, f"{len(unknown_wait)} unknown labels {unknown_wait}")

    unknown_dur = [r[0] for r in con.execute("""
        SELECT DISTINCT duration_band FROM fact_appointments_by_duration
        WHERE duration_band NOT IN (SELECT duration_band FROM dim_duration_band)
    """).fetchall()]
    add("7. Every duration label is a known band",
        not unknown_dur, f"{len(unknown_dur)} unknown labels {unknown_dur}")

    nulls = con.execute("""
        SELECT COUNT(*) FROM fact_appointments_by_category
        WHERE appointment_day IS NULL OR date_key IS NULL OR count_of_appointments IS NULL
    """).fetchone()[0]
    add("8. No null dates or counts in the category fact", nulls == 0, f"{nulls:,} nulls")

    lowest_month, lowest = con.execute("""
        WITH d AS (
            SELECT appointment_month AS m, SUM(count_of_appointments) AS total
            FROM fact_appointments_by_duration GROUP BY 1),
        c AS (
            SELECT appointment_month AS m, SUM(count_of_appointments) AS total
            FROM fact_appointments_by_category GROUP BY 1)
        SELECT d.m, d.total / c.total AS coverage
        FROM d JOIN c USING (m)
        ORDER BY coverage
        LIMIT 1
    """).fetchone()
    add("9. Duration covers at least 90% of category appointments each month",
        lowest >= MIN_DURATION_COVERAGE, f"lowest {lowest:.1%} in {lowest_month}")

    return results


def run_checks(warehouse_file, expected, report_file):
    """Run all nine checks, write the report, then fail if any check failed.

    Returns the report file path.
    """
    con = duckdb.connect(str(warehouse_file), read_only=True)
    try:
        results = _checks(con, expected)
    finally:
        con.close()

    passed = sum(r["status"] == "PASS" for r in results)
    report = {
        "run_at": datetime.now().isoformat(timespec="seconds"),
        "warehouse": str(warehouse_file),
        "passed": passed,
        "failed": len(results) - passed,
        "checks": results,
    }
    report_file = Path(report_file)
    report_file.parent.mkdir(parents=True, exist_ok=True)
    report_file.write_text(json.dumps(report, indent=2))

    for r in results:
        log.info("%s  %s  (%s)", r["status"], r["check"], r["detail"])
    log.info("Checks: %s/%s passed, report written to %s", passed, len(results), report_file)

    if passed < len(results):
        failed = [r["check"] for r in results if r["status"] == "FAIL"]
        raise QualityCheckFailed(f"{len(failed)} check(s) failed: {failed}")
    return report_file
