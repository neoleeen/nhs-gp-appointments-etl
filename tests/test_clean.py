from pyspark.sql import functions as F


def total(df):
    return df.agg(F.sum("count_of_appointments")).first()[0]


def count_duplicates(df, keys):
    return df.groupBy(keys).count().filter(F.col("count") > 1).count()


# Row counts and totals preserved

def test_regional_rows_and_total(regional):
    assert regional.count() == 295_692
    assert total(regional) == 742_804_525


def test_categories_rows_and_total(categories):
    assert categories.count() == 817_394
    assert total(categories) == 296_046_770


def test_duration_rows_and_total(duration):
    assert duration.count() == 137_793
    assert total(duration) == 167_980_692


# No duplicate keys

def test_regional_no_duplicate_keys(regional):
    keys = [c for c in regional.columns if c != "count_of_appointments"]
    assert count_duplicates(regional, keys) == 0


def test_categories_no_duplicate_keys(categories):
    keys = ["appointment_day", "sub_icb_code", "service_setting",
            "context_type", "national_category"]
    assert count_duplicates(categories, keys) == 0


def test_duration_no_duplicate_keys(duration):
    keys = ["appointment_day", "sub_icb_location_code", "actual_duration"]
    assert count_duplicates(duration, keys) == 0


# Tidy labels

def test_no_double_spaces_in_wait_bands(regional):
    bad = regional.filter(F.col("time_between_book_and_appointment").contains("  ")).count()
    assert bad == 0


def test_hcp_type_labels(regional):
    labels = {r[0] for r in regional.select("hcp_type").distinct().collect()}
    assert labels == {"GP", "Other Practice Staff", "Unknown"}


def test_no_bom_in_column_names(categories):
    assert not any("\ufeff" in c for c in categories.columns)
    assert "appointment_date" in categories.columns


# Dates

def test_categories_dates_parsed_day_first(categories):
    assert categories.filter(F.col("appointment_day").isNull()).count() == 0
    wrong_month = categories.filter(
        F.date_format("appointment_day", "yyyy-MM") != F.col("appointment_month")
    ).count()
    assert wrong_month == 0


def test_no_null_dates(regional, duration):
    assert regional.filter(F.col("month_start").isNull()).count() == 0
    assert duration.filter(F.col("appointment_day").isNull()).count() == 0


# Sub-ICB codes

def test_sub_icb_codes(categories):
    assert categories.filter(F.col("sub_icb_code") == "").count() == 0
    assert categories.select("sub_icb_code").distinct().count() == 106


# Counts positive

def test_counts_positive(regional, categories, duration):
    for df in (regional, categories, duration):
        assert df.filter(F.col("count_of_appointments").isNull()).count() == 0
        assert df.agg(F.min("count_of_appointments")).first()[0] > 0
