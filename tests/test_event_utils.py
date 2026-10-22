"""Unit tests for event normalization and window overlap logic.

These tests run without Home Assistant: ``event_utils`` is loaded straight
from its file so the integration package (which imports HA) is never
imported.  Run with ``python -m pytest tests -q``.
"""
import importlib.util
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

_MOD_PATH = (
    Path(__file__).resolve().parents[1]
    / "custom_components"
    / "exchange_calendar"
    / "event_utils.py"
)
_spec = importlib.util.spec_from_file_location("event_utils", _MOD_PATH)
eu = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(eu)

TZ = ZoneInfo("Europe/Budapest")


def dt(y, m, d, hh=0, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=TZ)


def allday(start, end=None, **extra):
    ev = {
        "uid": "u",
        "summary": "All day",
        "start": start,
        "end": start if end is None else end,
        "is_all_day": True,
    }
    ev.update(extra)
    return ev


def timed(start, end, **extra):
    ev = {"uid": "t", "summary": "Timed", "start": start, "end": end, "is_all_day": False}
    ev.update(extra)
    return ev


# ── All-day events with a non-exclusive end (the reported bug) ──────────


def test_allday_nonexclusive_end_is_extended_to_next_day():
    ev = eu.normalize_event(allday(date(2026, 10, 26)), TZ)
    assert ev["start"] == date(2026, 10, 26)
    assert ev["end"] == date(2026, 10, 27)


def test_allday_returned_for_every_window_overlapping_its_day():
    # Reproduction table from the bug report: all five rows must match.
    ev = eu.normalize_event(allday(date(2026, 10, 26)), TZ)
    assert eu.event_overlaps(ev, dt(2026, 10, 25, 23, 59), dt(2026, 10, 27, 12), TZ)
    assert eu.event_overlaps(ev, dt(2026, 10, 26, 0, 0), dt(2026, 10, 27, 0, 0), TZ)
    assert eu.event_overlaps(ev, dt(2026, 10, 26, 0, 0), dt(2026, 10, 27, 12), TZ)
    assert eu.event_overlaps(ev, dt(2026, 10, 26, 1, 0), dt(2026, 10, 27, 12), TZ)


def test_allday_not_returned_for_the_day_after():
    ev = eu.normalize_event(allday(date(2026, 10, 26)), TZ)
    assert not eu.event_overlaps(ev, dt(2026, 10, 27, 0, 0), dt(2026, 10, 28, 0, 0), TZ)


def test_raw_allday_would_be_dropped_without_normalization():
    # Documents why normalization must run before the overlap test.
    raw = allday(date(2026, 10, 26))
    assert not eu.event_overlaps(raw, dt(2026, 10, 26, 0, 0), dt(2026, 10, 27, 0, 0), TZ)


def test_multiday_allday_with_exclusive_end():
    ev = eu.normalize_event(allday(date(2026, 10, 26), date(2026, 10, 29)), TZ)
    assert ev["end"] == date(2026, 10, 29)  # already exclusive: untouched
    for day in (26, 27, 28):
        assert eu.event_overlaps(ev, dt(2026, 10, day), dt(2026, 10, day + 1), TZ)
    assert not eu.event_overlaps(ev, dt(2026, 10, 29), dt(2026, 10, 30), TZ)


# ── Timed events keep exact half-open semantics ───────────────────────────


def test_timed_event_window_boundaries():
    ev = eu.normalize_event(timed(dt(2026, 11, 2, 11), dt(2026, 11, 2, 13)), TZ)
    # Unchanged bounds.
    assert ev["start"] == dt(2026, 11, 2, 11) and ev["end"] == dt(2026, 11, 2, 13)
    # Reported row: window fully covering the event.
    assert eu.event_overlaps(ev, dt(2026, 11, 2, 11), dt(2026, 11, 2, 13), TZ)
    # Event ending exactly at window start is NOT returned.
    assert not eu.event_overlaps(ev, dt(2026, 11, 2, 13), dt(2026, 11, 2, 15), TZ)
    # Event starting exactly at window start IS returned.
    assert eu.event_overlaps(ev, dt(2026, 11, 2, 11), dt(2026, 11, 2, 12), TZ)
    # Event starting exactly at window end is NOT returned.
    assert not eu.event_overlaps(ev, dt(2026, 11, 2, 9), dt(2026, 11, 2, 11), TZ)


# ── DST boundary: 2026-10-25 (CEST -> CET) in Europe/Budapest ─────────────


def test_allday_on_dst_transition_day():
    ev = eu.normalize_event(allday(date(2026, 10, 25)), TZ)
    start_dt = eu.to_comparable_datetime(ev["start"], TZ)
    end_dt = eu.to_comparable_datetime(ev["end"], TZ)
    assert start_dt.utcoffset() == timedelta(hours=2)  # still summer time at 00:00
    assert end_dt.utcoffset() == timedelta(hours=1)  # next midnight is winter time
    # Same-tzinfo subtraction is wall-clock in Python; compare real elapsed time.
    assert end_dt.timestamp() - start_dt.timestamp() == 25 * 3600  # 25h-long day
    assert eu.event_overlaps(ev, dt(2026, 10, 25), dt(2026, 10, 26), TZ)
    assert not eu.event_overlaps(ev, dt(2026, 10, 26), dt(2026, 10, 27), TZ)


# ── Normalization details ─────────────────────────────────────────────────


def test_normalize_is_idempotent():
    for ev in (
        allday(date(2026, 10, 26)),
        allday(date(2026, 10, 26), date(2026, 10, 29)),
        timed(dt(2026, 11, 2, 13), dt(2026, 11, 2, 11)),  # inverted
        timed(dt(2026, 11, 2, 11), dt(2026, 11, 2, 13), summary="x" * 500),
    ):
        once = eu.normalize_event(ev, TZ)
        twice = eu.normalize_event(once, TZ)
        assert once == twice


def test_mixed_date_and_datetime_bounds_are_promoted():
    ev = eu.normalize_event({"start": date(2026, 10, 26), "end": dt(2026, 10, 26, 10)}, TZ)
    assert ev["start"] == dt(2026, 10, 26, 0, 0)
    assert isinstance(ev["start"], datetime) and isinstance(ev["end"], datetime)


def test_naive_datetime_gets_timezone():
    ev = eu.normalize_event(
        {"start": datetime(2026, 11, 2, 11), "end": datetime(2026, 11, 2, 13)}, TZ
    )
    assert ev["start"].tzinfo is TZ and ev["end"].tzinfo is TZ


def test_inverted_timed_event_is_clamped():
    ev = eu.normalize_event(timed(dt(2026, 11, 2, 13), dt(2026, 11, 2, 11)), TZ)
    assert ev["end"] == ev["start"]


def test_oversized_text_is_truncated():
    ev = eu.normalize_event(
        timed(dt(2026, 11, 2, 11), dt(2026, 11, 2, 13), summary="s" * 300, description="d" * 5000),
        TZ,
    )
    assert len(ev["summary"]) == eu.DEFAULT_MAX_SUMMARY_LENGTH
    assert ev["summary"].endswith("...")
    assert len(ev["description"]) == eu.DEFAULT_MAX_DESCRIPTION_LENGTH
