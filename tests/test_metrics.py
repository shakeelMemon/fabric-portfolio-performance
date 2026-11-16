import datetime as dt

import pytest


def d(s):
    return dt.date.fromisoformat(s)


def test_xirr_matches_excel_reference(common_ns):
    # Microsoft's documented XIRR example returns 0.373362535 (37.34%)
    dates = [d("2008-01-01"), d("2008-03-01"), d("2008-10-30"), d("2009-02-15"), d("2009-04-01")]
    amounts = [-10000, 2750, 4250, 3250, 2750]
    assert common_ns["xirr"](dates, amounts) == pytest.approx(0.373362535, abs=1e-6)


def test_xirr_one_year_doubling(common_ns):
    assert common_ns["xirr"]([d("2023-01-01"), d("2024-01-01")], [-100, 200]) == pytest.approx(1.0, abs=1e-6)


def test_xirr_negative_return(common_ns):
    r = common_ns["xirr"]([d("2023-01-01"), d("2025-01-01")], [-100, 60])
    assert r == pytest.approx((60 / 100) ** (365 / 731) - 1, abs=1e-6)


def test_xirr_undefined_without_sign_change(common_ns):
    assert common_ns["xirr"]([d("2023-01-01"), d("2024-01-01")], [-100, -50]) is None
    assert common_ns["xirr"]([], []) is None


def test_xirr_order_independent(common_ns):
    dates = [d("2022-03-15"), d("2023-06-30"), d("2022-09-01"), d("2024-12-31")]
    amounts = [-50, 20, -30, 90]
    a = common_ns["xirr"](dates, amounts)
    b = common_ns["xirr"](list(reversed(dates)), list(reversed(amounts)))
    assert a == pytest.approx(b, abs=1e-9)


def test_quarter_calendar(common_ns):
    ql = common_ns["quarter_list"]("2025Q3", "2026Q2")
    assert ql == ["2025Q3", "2025Q4", "2026Q1", "2026Q2"]
    assert common_ns["quarter_end"]("2024Q1") == d("2024-03-31")
    assert common_ns["quarter_end"]("2025Q4") == d("2025-12-31")
    assert common_ns["quarter_start"]("2026Q3") == d("2026-07-01")
    assert common_ns["quarter_of"](d("2026-11-14")) == "2026Q4"
