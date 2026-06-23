"""Calendar platform for Exchange Calendar.

Patched behavior:
- The HA calendar UI is served from coordinator.data only.
- async_get_events() performs no Exchange/EWS network I/O.
- This prevents live EWS calls on every day/week/month click.
"""

from __future__ import annotations

from datetime import date, datetime, time
import logging
from typing import Any

from homeassistant.components.calendar import CalendarEntity, CalendarEvent
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util

from .const import CONF_EMAIL, DOMAIN
from .coordinator import ExchangeCalendarCoordinator

_LOGGER = logging.getLogger(__name__)

type ExchangeCalendarConfigEntry = ConfigEntry[ExchangeCalendarCoordinator]


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ExchangeCalendarConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Exchange Calendar entities."""
    coordinator = config_entry.runtime_data

    async_add_entities(
        [ExchangeCalendarEntity(coordinator, config_entry)],
        update_before_add=False,
    )


class ExchangeCalendarEntity(
    CoordinatorEntity[ExchangeCalendarCoordinator],
    CalendarEntity,
):
    """Exchange Calendar entity backed by coordinator cache.

    Important:
    - This entity does not call Exchange directly when the UI requests events.
    - All UI date-range requests are filtered from coordinator.data.
    """

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: ExchangeCalendarCoordinator,
        config_entry: ConfigEntry,
    ) -> None:
        """Initialize Exchange Calendar entity."""
        super().__init__(coordinator)

        email = config_entry.data[CONF_EMAIL]

        self._attr_unique_id = f"{DOMAIN}_{email}"
        self._attr_name = f"Exchange ({email})"

        # Deliberately do not advertise create/update/delete here.
        # This patch focuses on stable read/cache behavior.
        self._attr_supported_features = 0

    @property
    def event(self) -> CalendarEvent | None:
        """Return the current or next upcoming event from cache.

        Home Assistant uses this to determine the calendar entity state:
        - on  = there is an active event
        - off = there is no active event
        """
        now = dt_util.now()
        cached_events = self.coordinator.data or []

        candidates: list[CalendarEvent] = []

        for raw_event in cached_events:
            calendar_event = _raw_event_to_calendar_event(raw_event)
            if calendar_event is None:
                continue

            event_end = _to_compare_datetime(calendar_event.end)
            if event_end is None:
                continue

            # Keep active or future events only.
            if event_end >= now:
                candidates.append(calendar_event)

        candidates.sort(key=lambda ev: _to_compare_datetime(ev.start) or datetime.max)

        return candidates[0] if candidates else None

    async def async_get_events(
        self,
        hass: HomeAssistant,
        start_date: datetime,
        end_date: datetime,
    ) -> list[CalendarEvent]:
        """Return calendar events for requested range from cache only.

        This method is called heavily by the Home Assistant calendar UI when
        browsing days/weeks/months.

        Do NOT perform live Exchange/EWS I/O here.
        """
        cached_events = self.coordinator.data or []
        events: list[CalendarEvent] = []

        _LOGGER.debug(
            "[Exchange] Calendar UI requested events from cache: %s → %s; cached=%s",
            start_date,
            end_date,
            len(cached_events),
        )

        for raw_event in cached_events:
            calendar_event = _raw_event_to_calendar_event(raw_event)
            if calendar_event is None:
                continue

            if not _event_overlaps(
                calendar_event.start,
                calendar_event.end,
                start_date,
                end_date,
            ):
                continue

            events.append(calendar_event)

        events.sort(key=lambda ev: _to_compare_datetime(ev.start) or datetime.max)

        _LOGGER.debug(
            "[Exchange] Calendar UI served %s event(s) from cache",
            len(events),
        )

        return events

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Expose next/current event attributes for HA states."""
        event = self.event
        if event is None:
            return {
                "cached_events": len(self.coordinator.data or []),
            }

        return {
            "message": event.summary,
            "all_day": isinstance(event.start, date)
            and not isinstance(event.start, datetime),
            "start_time": event.start.isoformat()
            if hasattr(event.start, "isoformat")
            else event.start,
            "end_time": event.end.isoformat()
            if hasattr(event.end, "isoformat")
            else event.end,
            "location": event.location,
            "description": event.description,
            "categories": _get_event_categories(self.coordinator.data, event.uid),
            "legacy_free_busy_status": _get_event_free_busy_status(self.coordinator.data, event.uid),
            "sensitivity": _get_event_sensitivity(self.coordinator.data, event.uid),
            "cached_events": len(self.coordinator.data or []),
        }


def _raw_event_to_calendar_event(raw_event: dict[str, Any]) -> CalendarEvent | None:
    """Convert cached dict event to Home Assistant CalendarEvent."""
    start = (
        raw_event.get("start")
        or raw_event.get("start_time")
        or raw_event.get("begin")
    )
    end = (
        raw_event.get("end")
        or raw_event.get("end_time")
        or raw_event.get("finish")
    )

    if start is None or end is None:
        _LOGGER.debug("[Exchange] Skipping cached event without start/end: %s", raw_event)
        return None

    start = _normalize_calendar_value(start)
    end = _normalize_calendar_value(end)

    if start is None or end is None:
        _LOGGER.debug("[Exchange] Skipping cached event with invalid start/end: %s", raw_event)
        return None

    
# ✅ FIX: ensure valid duration (HA requirement)
    start_cmp = _to_compare_datetime(start)
    end_cmp = _to_compare_datetime(end)

    if start_cmp is None or end_cmp is None:
        return None

    if end_cmp < start_cmp:
        _LOGGER.debug(
            "[Exchange] Fixing invalid event duration (end < start): %s",
            raw_event,
        )
        # Option 1: fix it
        end_cmp = start_cmp

        # convert back to original type
        if isinstance(start, datetime):
            end = end_cmp
        else:
            end = end_cmp.date()


    summary = (
        raw_event.get("summary")
        or raw_event.get("message")
        or raw_event.get("subject")
        or raw_event.get("title")
        or ""
    )

    description = raw_event.get("description") or raw_event.get("body")
    location = raw_event.get("location")
    uid = (
        raw_event.get("uid")
        or raw_event.get("id")
        or raw_event.get("item_id")
        or raw_event.get("ews_id")
    )

    return CalendarEvent(
        summary=summary,
        start=start,
        end=end,
        description=description,
        location=location,
        uid=uid,
    )


def _normalize_calendar_value(value: Any) -> date | datetime | None:
    """Normalize raw cache value to date/datetime for CalendarEvent."""
    if isinstance(value, datetime):
        return _ensure_aware_datetime(value)

    if isinstance(value, date):
        return value

    if isinstance(value, str):
        parsed = dt_util.parse_datetime(value)
        if parsed is not None:
            return _ensure_aware_datetime(parsed)

        parsed_date = dt_util.parse_date(value)
        if parsed_date is not None:
            return parsed_date

    return None


def _ensure_aware_datetime(value: datetime) -> datetime:
    """Ensure datetime has timezone information."""
    if value.tzinfo is None:
        # Exchange/exchangelib can return naive datetimes in some cases.
        # Treat them as local HA timezone.
        return value.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)

    return dt_util.as_local(value)


def _to_compare_datetime(value: date | datetime) -> datetime | None:
    """Convert date/datetime to comparable timezone-aware datetime."""
    if isinstance(value, datetime):
        return _ensure_aware_datetime(value)

    if isinstance(value, date):
        return datetime.combine(
            value,
            time.min,
            tzinfo=dt_util.DEFAULT_TIME_ZONE,
        )

    return None


def _event_overlaps(
    event_start: date | datetime,
    event_end: date | datetime,
    range_start: date | datetime,
    range_end: date | datetime,
) -> bool:
    """Return True if event overlaps requested calendar range.

    Overlap rule:
    event_end > range_start and event_start < range_end
    """
    event_start_cmp = _to_compare_datetime(event_start)
    event_end_cmp = _to_compare_datetime(event_end)
    range_start_cmp = _to_compare_datetime(range_start)
    range_end_cmp = _to_compare_datetime(range_end)

    if (
        event_start_cmp is None
        or event_end_cmp is None
        or range_start_cmp is None
        or range_end_cmp is None
    ):
        return False

    return event_end_cmp > range_start_cmp and event_start_cmp < range_end_cmp

def _get_event_categories(events: list[dict], uid: str | None) -> list[str]:
    """Return categories for a given event uid."""
    if not events or not uid:
        return []

    for ev in events:
        if ev.get("uid") == uid:
            return ev.get("categories") or []

    return []

def _get_event_free_busy_status(events: list[dict], uid: str | None) -> str:
    """Return free/busy status for a given event uid."""
    if not events or not uid:
        return []

    for ev in events:
        if ev.get("uid") == uid:
            return ev.get("legacy_free_busy_status") or ""

    return []


def _get_event_sensitivity(events: list[dict], uid: str | None) -> str:
    """Return sensitivity (privacy) for a given event uid."""
    if not events or not uid:
        return []

    for ev in events:
        if ev.get("uid") == uid:
            return ev.get("sensitivity") or ""

    return []
