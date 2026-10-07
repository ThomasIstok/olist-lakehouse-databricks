"""Unit tests for the pure (Spark-free) logic in src/ingestion/frankfurter.py.

Covered: resolve_window (which dates to request) and flatten_rates (JSON -> long rows).
Not covered: get_watermark and merge_into_bronze need a Spark session and a Delta table;
they are verified by the job run in olist_dev and by dbt tests on top of bronze.
"""
from datetime import date, datetime, timezone

import pytest

from src.ingestion import frankfurter
from src.ingestion.frankfurter import flatten_rates, resolve_window

# Same shape as the "fx" section of src/ingestion/config.yml (only the keys the function reads)
FX_CFG = {"start_date": "2016-09-01", "end_date": "2018-10-31"}


# ---------- resolve_window ----------


def test_first_run_starts_at_configured_start_date():
    # Empty bronze -> no watermark -> request the whole configured period
    assert resolve_window(FX_CFG, watermark=None) == (date(2016, 9, 1), date(2018, 10, 31))


def test_incremental_run_starts_day_after_watermark():
    # Bronze already has data up to 2017-03-15 -> continue with 2017-03-16
    window = resolve_window(FX_CFG, watermark=date(2017, 3, 15))
    assert window == (date(2017, 3, 16), date(2018, 10, 31))


def test_window_can_be_a_single_day():
    # Boundary: watermark is the day before end_date -> start == end is still a valid window
    window = resolve_window(FX_CFG, watermark=date(2018, 10, 30))
    assert window == (date(2018, 10, 31), date(2018, 10, 31))


# One test function, two inputs: pytest runs it once per value
@pytest.mark.parametrize("watermark", [date(2018, 10, 31), date(2019, 1, 1)])
def test_up_to_date_returns_none(watermark):
    # Watermark at or after end_date -> nothing left to request
    assert resolve_window(FX_CFG, watermark) is None


def test_end_date_today_uses_current_utc_date(monkeypatch):
    # "today" depends on the clock -> replace the clock, so the test passes on any day
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            # Design decision under test: the date must be taken in UTC, not local time
            assert tz is timezone.utc
            return datetime(2024, 5, 10, 23, 30, tzinfo=timezone.utc)

    # Patch the name inside the module that USES it (frankfurter.datetime)
    monkeypatch.setattr(frankfurter, "datetime", FrozenDatetime)

    cfg = {"start_date": "2016-09-01", "end_date": "today"}
    assert resolve_window(cfg, watermark=date(2024, 5, 8)) == (date(2024, 5, 9), date(2024, 5, 10))


# ---------- flatten_rates ----------


def test_flatten_rates_returns_one_row_per_date_and_currency():
    payload = {
        "base": "BRL",
        "rates": {
            "2017-01-02": {"EUR": 0.29, "CZK": 7.9},
            "2017-01-03": {"EUR": 0.3, "CZK": 8},
        },
    }
    # sorted(): the test checks the content, not the order the API happened to return
    assert sorted(flatten_rates(payload)) == [
        ("2017-01-02", "BRL", "CZK", 7.9),
        ("2017-01-02", "BRL", "EUR", 0.29),
        ("2017-01-03", "BRL", "CZK", 8.0),
        ("2017-01-03", "BRL", "EUR", 0.3),
    ]


def test_flatten_rates_converts_rate_to_float():
    # JSON may send 8 instead of 8.0; the bronze column is DOUBLE, so it must become a float
    rows = flatten_rates({"base": "BRL", "rates": {"2017-01-03": {"CZK": 8}}})
    assert isinstance(rows[0][3], float)


def test_flatten_rates_with_no_rates_returns_empty_list():
    # Window with only a weekend or holiday -> the API returns no dates
    assert flatten_rates({"base": "BRL", "rates": {}}) == []