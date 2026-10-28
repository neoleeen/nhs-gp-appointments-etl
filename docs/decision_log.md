# Decision log

Week 2 transform (`src/transform.py`). One line per decision: what changed, why, and how many rows it affected.
Row counts and totals before and after are checked automatically by `tests/test_clean.py` (16 tests).

## Cleaning decisions

| # | What changed | Why | Rows affected |
|---|---|---|---|
| 1 | Converted `actual_duration.csv.xlsx` to CSV with pandas during extract | Trap 1: it is an Excel file despite its name, and Spark cannot read Excel | 137,793 rows; total 167,980,692 |
| 2 | Declared explicit schemas for all three files, with counts as whole numbers (`LongType`) | A guessed type can change silently when a new file arrives | All rows in all three files |
| 3 | Strip the byte-order mark (`\ufeff`) from the categories column names | Trap 2. Tested: Spark 4.2.0 already removes it from the header, so nothing changed, but it is kept as a safeguard for other tools and versions | 0 columns changed |
| 4 | Parsed categories `appointment_date` day-first (`dd/MM/yyyy`) into a new column `appointment_day` | Trap 3: `02/08/2021` is 2 August, not 8 February | 817,394 rows parsed; 0 nulls; 0 rows where the month disagrees with `appointment_month` |
| 5 | Summed regional rows that share every descriptive column, instead of dropping them | Trap 4: the sub-ICB columns were removed from this file, so look-alike rows are real appointments from different sub-ICBs | 21,604 exact duplicates found; 596,821 → 295,692 rows (301,129 combined); total unchanged at 742,804,525 |
| 6 | Collapsed repeated spaces in wait-band labels (e.g. `15  to 21 Days` → `15 to 21 Days`, with two spaces after `15`) | Trap 5: the same band must have one label | 225,119 raw rows across 3 labels |
| 7 | Changed `Other Practice staff` to `Other Practice Staff` | Trap 5: match the spelling in the metadata | 241,557 raw rows |
| 8 | Added `month_start` (a real date) beside the regional `appointment_month` | Add, don't overwrite; gives a date to join to the date dimension | 295,692 rows; 0 nulls |
| 9 | Extracted `sub_icb_code` from the end of `sub_icb_location_name` (e.g. `... ICB - 00P` → `00P`) | The categories file has no code column, and the code is needed to join to `dim_geography` | 817,394 rows; 0 empty; 106 distinct codes |
| 10 | Parsed duration `appointment_date` (`yyyy-MM-dd`, as written by pandas) into `appointment_day`, and added `appointment_month` | So all three files are partitioned by the same column | 137,793 rows; 0 nulls; 7 months |
| 11 | Left the 7 duration bands unchanged, including `Unknown / Data Quality` | The metadata says this band holds appointments under 1 or over 60 minutes; hiding it would overstate data quality | 40,284,086 appointments in that band |
| 12 | Wrote Parquet partitioned by `appointment_month`, with `mode("overwrite")` and one file per month | Reruns must not double up. The first regional write made many small files and one failed to save on Google Drive (`FAILED_READ_FILE`), so each month is now written as one file | Totals unchanged; 157.1 MB of CSV → 4.1 MB of Parquet |
| 13 | Kept counts as whole numbers and stored no rates or percentages | Rates do not add up across groups; they are calculated at query time | None |
| 14 | Did not add a sort order to the wait and duration bands in the clean layer | Alphabetical order puts `1 Day` before `Same Day`. The sort order belongs in `dim_booking_wait` and `dim_duration_band` (Week 3), so the clean layer keeps the labels only | None in Week 2; done in the Week 3 dimensions |

## KPI decisions

### What goes in the denominator

I follow the brief's KPI definitions: "Unknown" is left out of the denominator of every rate about a known category, and the number of Unknown appointments is reported beside the rate.

- KPI 3 and 4 (same-day and long-wait share): exclude the `Unknown / Data Quality` wait band.
- KPI 5 and 6 (DNA rate): DNA ÷ (Attended + DNA). Unknown status is excluded: 3–6% of appointments each month stay "Booked", and nobody knows whether they took place.
- KPI 7 (face-to-face share): exclude Unknown mode.
- KPI 8 (GP share): exclude Unknown staff type.
- KPI 11 (share longer than 15 minutes): exclude `Unknown / Data Quality` duration.
- KPI 10 and 12: the denominator is all appointments. KPI 12 measures the unknowns themselves, so they must stay in.

Why: including Unknown would pull every share down by the size of the unknown group, which changes by month and by ICB. A trend would then partly show recording quality instead of care.

### Partial years

The regional file runs from January 2020 to June 2022, so 2022 has only six months. The categories file starts in August 2021 and the duration file in December 2021. I compare a month with the same month a year earlier (KPI 2), or January–June with January–June, never a full year with a half year. Year-on-year change is only available from January 2021. Many 2020 months were affected by COVID-19, so growth in 2021 is measured against a low baseline.

### Missing population

The files have no registered-patient counts, so appointments per 1,000 patients cannot be calculated. A large ICB will always have more appointments than a small one, so raw totals are not compared between ICBs. KPI 6 ranks ICBs by DNA rate, which does not depend on size. The dashboard will state this limitation.

## Why three fact tables, not one flat table

The three files have three different grains:

- `fact_appointments_monthly`: one row per ICB × month × status × staff type × mode × booking wait
- `fact_appointments_by_category`: one row per sub-ICB location × day × service setting × national category
- `fact_appointments_by_duration`: one row per sub-ICB location × day × duration band

One flat table would mix these grains. Joining daily sub-ICB rows to monthly ICB rows repeats each row many times, which inflates totals, and any filter across files would give wrong answers. It would also repeat geography names on every row. Keeping three fact tables lets each one keep its natural grain. They share the same date and geography dimensions, so KPIs from different files line up once each is aggregated to a shared level such as month. The files also do not cover the same appointments: the duration file holds about 91–92% of the category file's appointments. Separate tables keep that gap visible instead of hiding it.


## Week 3: Load and checks

| # | Decision | Why | Evidence |
|---|---|---|---|
| 15 | Six dimensions, not the guide's five: added `dim_icb` | The regional file only has ICB codes, so its fact cannot join to the sub-ICB `dim_geography`. `dim_icb` is built from `dim_geography`, so the two levels always agree | 42 ICBs, 0 regional ICBs missing |
| 16 | NHS codes are the keys (`sub_icb_code`, `icb_ons_code`); no surrogate keys | They are real, stable identifiers, as the guide recommends | 106 sub-ICBs, 0 orphan rows (check 5) |
| 17 | `date_key` (yyyymmdd integer) plus a real date in each fact; `dim_date` covers every day from 1 Jan 2020 to 30 Jun 2022 | One row per day for weekday KPIs; covers every date in all three files | 912 days, no gaps or duplicates (test) |
| 18 | `appointment_month` kept as text (`2021-08`) in every fact | DuckDB reads the partition folder names as VARCHAR. Text is used only as the month label for the load; joins to `dim_date` use real dates (`month_start`, `appointment_day`), so each concept has one type everywhere | All dates in all facts exist in `dim_date` |
| 19 | Band sort order and KPI flags in the dimensions (closes Week 2 row 14) | Alphabetical order puts `1 Day` before `Same Day`. Flags (`is_known`, `more_than_14_days`, `over_15_minutes`, `is_data_quality_issue`) write each KPI rule once | 8 wait bands, 7 duration bands; every label known (checks 6, 7) |
| 20 | `dim_national_category` keyed by category name | Each of the 18 categories has exactly one context type | 18 rows, 18 names |
| 21 | Facts hold only keys, labels and counts; names live in the dimensions | One row per sub-ICB in `dim_geography` instead of the same name repeated 800,000 times | |
| 22 | Load is idempotent: per month, DELETE then INSERT inside one transaction; dimensions rebuilt with CREATE OR REPLACE | A retry or a second run must never double the data; ROLLBACK leaves a month untouched if the insert fails | Three full pipeline runs gave identical totals; a test loads twice and compares |
| 23 | No DISTINCT or GROUP BY in the load | Week 2 already summed the look-alike rows; re-aggregating would delete real appointments | Check 4: 0 of 11 months differ |
| 24 | Expected rows and totals are fixed constants in `config.EXPECTED`, checked after extract, transform and load | Comparing a run against its own figures only proves the arithmetic works | Pipeline stops if any total differs |
| 25 | All nine checks run, are recorded in `docs/quality_report.json`, then the run fails if any failed (exit code 2); a crash is exit code 1 | One failure must not hide the others, and a scheduler can tell a good run from a bad one by the exit code | 9/9 pass; a test proves a broken warehouse fails checks 1, 4 and 5 |
| 26 | Reconciliation (check 4) compares only Aug 2021 to Jun 2022 | Outside that window one file has no data, which would look like a failure | 11 months compared, 0 differ |

### Why duration covers about 91% of category appointments, not 100% (check 9)

Lowest coverage is 90.9%, in December 2021. The check passes at 90% or more; it is not forced to 100%.

What `metadata_nhs.txt` says:

- Actual duration is a newer field, first published in the April 2022 release with data from December 2021.
- It is recorded differently depending on the practice's system supplier (for example, EMIS times from when the patient is called in until the clinician completes the consultation; TPP can start the clock in several ways). NHS Digital is still working with suppliers on its quality.
- Appointments with a missing duration, or under 1 or over 60 minutes, are still counted, in the `Unknown / Data Quality` band.

My conclusion: because missing and out-of-range durations are already counted as Unknown, the remaining gap of about 8 to 9% is appointments with no duration record at all in the separately collected duration data. Duration depends on each supplier's own timing records, which do not cover every appointment the category file counts. The two files measure different things from different collections, so I report the gap rather than adjust either figure. Any KPI using duration (KPI 11) should say it covers about 91% of appointments.
