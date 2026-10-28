# Data Profile

Profile of the three NHS England "Appointments in General Practice" files used in this project.
Source: NHS England, Appointments in General Practice.
Profiling was done in Google Colab with pandas, before any cleaning.

## Overview

| File | Rows | Columns | Grain |
|---|---|---|---|
| `appointments_regional.csv` | 596,821 | 7 | one ICB, month, status, staff type, mode and booking-wait band |
| `national_categories.csv` | 817,394 | 8 | one sub-ICB location, day, service setting and national category |
| `actual_duration.csv` (converted from Excel) | 137,793 | 8 | one sub-ICB location, day and consultation-length band |

All three row counts match the guide and my `manifest.json`.

## File profiles

### appointments_regional.csv
- **Date range:** 2020-01 to 2022-06 (30 months)
- **Nulls:** none in any column
- **Columns:**

| Column | Type |
|---|---|
| icb_ons_code | text |
| appointment_month | text |
| appointment_status | text |
| hcp_type | text |
| appointment_mode | text |
| time_between_book_and_appointment | text |
| count_of_appointments | whole number |

- **Distinct values:**
  - `appointment_status`: Attended, DNA, Unknown
  - `hcp_type`: GP, Other Practice staff, Unknown
  - `appointment_mode`: Face-to-Face, Home Visit, Telephone, Unknown, Video/Online
  - `time_between_book_and_appointment`: 1 Day, 15  to 21 Days, 2 to 7 Days, 22  to 28 Days, 8  to 14 Days, More than 28 Days, Same Day, Unknown / Data Quality
- **Notes:** has no sub-ICB columns (see trap 4). Contains `Unknown` values in three columns.

### national_categories.csv
- **Date range:** 2021-08 to 2022-06 (11 months)
- **Nulls:** none in any column
- **Columns:**

| Column | Type |
|---|---|
| appointment_date | text, written day-first (e.g. 02/08/2021) |
| icb_ons_code | text |
| sub_icb_location_name | text |
| service_setting | text |
| context_type | text |
| national_category | text |
| count_of_appointments | whole number |
| appointment_month | text |

- **Distinct values:**
  - `service_setting`: Primary Care Network, Other, General Practice, Unmapped, Extended Access Provision
  - `context_type`: Care Related Encounter, Unmapped, Inconsistent Mapping
- **Notes:** starts with a hidden byte-order mark (trap 2). Already has an `appointment_month` column (e.g. 2021-08) that I can use to check the parsed dates.

### actual_duration.csv (converted from Excel)
- **Date range:** 2021-12-01 to 2022-06-30 (212 different dates, so every day in that range is present and the file is daily)
- **Nulls:** none in any column
- **Columns:**

| Column | Type |
|---|---|
| sub_icb_location_code | text |
| sub_icb_location_ons_code | text |
| sub_icb_location_name | text |
| icb_ons_code | text |
| region_ons_code | text |
| appointment_date | text, written year-month-day (e.g. 2021-12-01) |
| actual_duration | text |
| count_of_appointments | whole number |

- **Distinct values:**
  - `region_ons_code`: 7 values
  - `actual_duration` (7 values): 1-5 Minutes, 6-10 Minutes, 11-15 Minutes, 16-20 Minutes, 21-30 Minutes, 31-60 Minutes and Unknown / Data Quality
- **Notes:** the date format differs from the categories file. `Unknown / Data Quality` covers appointments the metadata says were under 1 minute or over 60.

## The five traps I found

### Trap 1: The "CSV" that is really an Excel file
- **Found:** `actual_duration.csv.xlsx` opened with `pd.read_excel`, so it's an Excel file even though the name says CSV. No extra install was needed.
- **Problem:** Spark can't read Excel files.
- **Fix:** my extract script converts it to `actual_duration.csv` first.

### Trap 2: A hidden character at the start of the categories file
- **Found:** the first 3 bytes of `national_categories.csv` are a byte-order mark (BOM), an invisible marker. pandas hides it, so the column names looked normal.
- **Problem:** Spark doesn't hide it, so the first column could load as `\ufeffappointment_date` and my code wouldn't find it.
- **Fix:** strip the BOM from the column names when reading the file in Spark.

### Trap 3: Dates are written day-first
- **Found:** in `appointment_date` (categories file), the biggest first number is 31 and the biggest second number is 12. So the first number is the day, and the second is the month (02/08/2021 is 2 August).
- **Problem:** a tool that guesses month-first would get dates wrong, or fail on days above 12.
- **Fix:** tell Spark the format exactly (`dd/MM/yyyy`), then check that each row's date month matches its `appointment_month`.

### Trap 4: 21,604 rows that look like duplicates
- **Found:** `regional.duplicated().sum()` gave 21,604.
- **Why they're not mistakes:** the regional file doesn't have the sub-ICB columns, so two different sub-ICBs in the same ICB can produce rows that look identical. They are real appointments.
- **Problem:** deleting them would lose real appointments and make the totals too low.
- **Fix:** add their counts together (sum) and never drop them.

### Trap 5: Messy labels
- **Found:**
  - In `time_between_book_and_appointment`, some labels have a double space: `15  to 21 Days`, `22  to 28 Days` and `8  to 14 Days`.
  - In `hcp_type`, the value is `Other Practice staff`, but the metadata says `Other Practice Staff`.
  - Alphabetical order puts the wait bands in the wrong order (for example `1 Day` comes before `Same Day`).
- **Problem:** the same label spelled two ways counts as two different labels, and charts show the wait bands in the wrong order.
- **Fix:** turn double spaces into single spaces, rename the staff label to match the metadata, and give the wait bands an explicit sort order.

## Correction (Week 2)

**Trap 2 (BOM):** testing in Week 2 showed that Spark 4.2.0 removes the byte-order mark from the
header by itself, so the first column loads as `appointment_date`. Older versions and other tools
may not, so the transform still strips `\ufeff` from the column names as a safeguard.


## Total appointments per file

| File | Rows | Total appointments |
|---|---:|---:|
| appointments_regional.csv | 596,821 | 742,804,525 |
| national_categories.csv | 817,394 | 296,046,770 |
| actual_duration.csv.xlsx | 137,793 | 167,980,692 |

These totals are the same after every stage (transform, Parquet, warehouse), which is the
evidence that no appointments were lost.
