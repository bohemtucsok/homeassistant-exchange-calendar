"""Calendar platform for Exchange Calendar."""
from __future__ import annotations

import logging
from datetime import date, datetime
from functools import partial
from typing import Any

from homeassistant.components.calendar import (
    CalendarEntity,
    CalendarEntityFeature,
    CalendarEvent,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util

from .const import (
    DOMAIN,
    CONF_EMAIL,
    CONF_READ_ONLY,
    CONF_CALENDARS,
    DEFAULT_READ_ONLY,
    DEFAULT_CALENDAR_KEY,
    MAX_SUMMARY_LENGTH,
    MAX_DESCRIPTION_LENGTH,
)
from .coordinator import ExchangeCalendarCoordinator
from .event_utils import event_overlaps, normalize_event, to_comparable_datetime

_LOGGER = logging.getLogger(__name__)

type ExchangeCalendarConfigEntry = ConfigEntry[ExchangeCalendarCoordinator]


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ExchangeCalendarConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up one Exchange Calendar entity per selected calendar."""
    coordinator = config_entry.runtime_data

    selected = config_entry.options.get(CONF_CALENDARS) or [DEFAULT_CALENDAR_KEY]
    # The default calendar is always available, even if discovery failed.
    if DEFAULT_CALENDAR_KEY not in selected:
        selected = [DEFAULT_CALENDAR_KEY, *selected]

    entities: list[ExchangeCalendarEntity] = []
    for key in selected:
        is_default = key == DEFAULT_CALENDAR_KEY
        # Skip non-default keys that could not be resolved to a real calendar
        # (e.g. discovery failed or the calendar was deleted server-side).
        if not is_default and key not in coordinator.calendar_names:
            _LOGGER.warning("Selected calendar '%s' not found; skipping", key)
            continue
        name = coordinator.calendar_names.get(key, "")
        entities.append(
            ExchangeCalendarEntity(coordinator, config_entry, key, name, is_default)
        )

    async_add_entities(entities, update_before_add=False)


class ExchangeCalendarEntity(
    CoordinatorEntity[ExchangeCalendarCoordinator], CalendarEntity
):
    """Exchange Calendar entity with full CRUD support."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: ExchangeCalendarCoordinator,
        config_entry: ConfigEntry,
        calendar_key: str = DEFAULT_CALENDAR_KEY,
        calendar_name: str = "",
        is_default: bool = True,
    ) -> None:
        """Initialize Exchange Calendar entity."""
        super().__init__(coordinator)
        email = config_entry.data[CONF_EMAIL]
        self._config_entry = config_entry
        self._calendar_key = calendar_key
        # None for the primary calendar; the backend calendar id otherwise.
        self._calendar_id = None if is_default else calendar_key

        if is_default:
            # Preserve the original unique_id/name so existing installs and
            # their dashboards/automations keep working unchanged.
            self._attr_unique_id = f"{DOMAIN}_{config_entry.entry_id}"
            self._attr_name = f"Exchange ({email})"
        else:
            self._attr_unique_id = (
                f"{DOMAIN}_{config_entry.entry_id}_{calendar_key}"
            )
            self._attr_name = f"Exchange ({email}) {calendar_name}".strip()

        read_only = config_entry.options.get(CONF_READ_ONLY, DEFAULT_READ_ONLY)
        if read_only:
            self._attr_supported_features = CalendarEntityFeature(0)
        else:
            self._attr_supported_features = (
                CalendarEntityFeature.CREATE_EVENT
                | CalendarEntityFeature.DELETE_EVENT
                | CalendarEntityFeature.UPDATE_EVENT
            )

    def _current_event_dict(self) -> dict[str, Any] | None:
        """Return the raw dict of the current or next upcoming event."""
        data = self.coordinator.data or {}
        events = data.get(self._calendar_key, [])
        if not events:
            return None

        now = dt_util.now()
        for ev in events:
            # Normalize first: an all-day event with a non-exclusive end would
            # otherwise look as if it ended at midnight and never be "current".
            ev = self._normalize_event(ev)
            end_dt = self._to_comparable_datetime(ev["end"])
            if end_dt >= now:
                return ev

        return None

    @property
    def event(self) -> CalendarEvent | None:
        """Return the current or next upcoming event.

        Displayed on the calendar card in HA dashboard.
        """
        ev = self._current_event_dict()
        return self._to_calendar_event(ev) if ev else None

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Expose Exchange-specific fields of the current/next event.

        Handy for automations that react to categories or sensitivity.
        """
        ev = self._current_event_dict()
        if not ev:
            return None
        return {
            "free_busy_status": ev.get("free_busy") or "",
            "sensitivity": ev.get("sensitivity") or "",
            "categories": list(ev.get("categories") or []),
        }

    def _cache_covers(self, start_date: datetime, end_date: datetime) -> bool:
        """Return True if the coordinator cache fully covers the range."""
        coord = self.coordinator
        if coord.window_start is None or coord.window_end is None:
            return False
        if coord.truncated.get(self._calendar_key):
            return False
        return start_date >= coord.window_start and end_date <= coord.window_end

    async def async_get_events(
        self,
        hass: HomeAssistant,
        start_date: datetime,
        end_date: datetime,
    ) -> list[CalendarEvent]:
        """Return calendar events within a datetime range.

        Used by the calendar view and automations. Ranges that fall fully
        inside the coordinator's cached window are served from cache
        (instant, no network round-trip). Anything else - e.g. past months -
        is queried live from the server, so past and long-range browsing
        keep working.
        """
        data = self.coordinator.data or {}
        if self._cache_covers(start_date, end_date):
            raw_events = data.get(self._calendar_key, [])
        else:
            try:
                raw_events = await hass.async_add_executor_job(
                    partial(
                        self.coordinator.client.get_events_range,
                        start_date,
                        end_date,
                        calendar_id=self._calendar_id,
                    )
                )
            except Exception:
                _LOGGER.debug(
                    "Direct range query failed, falling back to coordinator cache"
                )
                raw_events = data.get(self._calendar_key, [])

        tz = dt_util.DEFAULT_TIME_ZONE
        events = []
        for ev in raw_events:
            # Normalize BEFORE the overlap test: Exchange may report an all-day
            # event with end == start, which would otherwise be filtered out of
            # every window that starts on that day.
            ev = self._normalize_event(ev)
            if event_overlaps(ev, start_date, end_date, tz):
                events.append(self._to_calendar_event(ev))

        return events

    async def async_create_event(self, **kwargs: Any) -> None:
        """Create a new event on the Exchange calendar.

        Called by the calendar.create_event service.
        """
        summary = kwargs.get("summary", "")
        dtstart = kwargs.get("dtstart")
        dtend = kwargs.get("dtend")
        description = kwargs.get("description", "")
        location = kwargs.get("location", "")

        _LOGGER.info("Creating Exchange event: %s", summary)

        await self.hass.async_add_executor_job(
            partial(
                self.coordinator.client.create_event,
                summary,
                dtstart,
                dtend,
                description,
                location,
                calendar_id=self._calendar_id,
            )
        )

        await self.coordinator.async_request_refresh()

    async def async_update_event(
        self,
        uid: str,
        event: dict[str, Any],
        recurrence_id: str | None = None,
        recurrence_range: str | None = None,
    ) -> None:
        """Update an existing event on the Exchange calendar.

        Called by the calendar.update_event service.
        """
        _LOGGER.info("Updating Exchange event: %s", uid)

        await self.hass.async_add_executor_job(
            partial(
                self.coordinator.client.update_event,
                uid,
                event.get("summary"),
                event.get("dtstart"),
                event.get("dtend"),
                event.get("description"),
                event.get("location"),
                calendar_id=self._calendar_id,
            )
        )

        await self.coordinator.async_request_refresh()

    async def async_delete_event(
        self,
        uid: str,
        recurrence_id: str | None = None,
        recurrence_range: str | None = None,
    ) -> None:
        """Delete an event from the Exchange calendar.

        Called by the calendar.delete_event service.
        """
        _LOGGER.info("Deleting Exchange event: %s", uid)

        await self.hass.async_add_executor_job(
            partial(
                self.coordinator.client.delete_event,
                uid,
                calendar_id=self._calendar_id,
            )
        )

        await self.coordinator.async_request_refresh()

    @staticmethod
    def _normalize_event(ev: dict[str, Any]) -> dict[str, Any]:
        """Return a sanitized copy of an event dict (see ``event_utils``).

        Idempotent, so it is safe to call at several layers: before the
        overlap filter, when picking the current event, and in
        ``_to_calendar_event``.
        """
        return normalize_event(
            ev,
            dt_util.DEFAULT_TIME_ZONE,
            max_summary=MAX_SUMMARY_LENGTH,
            max_description=MAX_DESCRIPTION_LENGTH,
        )

    @classmethod
    def _to_calendar_event(cls, ev: dict[str, Any]) -> CalendarEvent:
        """Convert internal dict to HA CalendarEvent."""
        ev = cls._normalize_event(ev)
        start = ev["start"]
        end = ev["end"]

        # Convert timezone-aware datetimes to HA local timezone so that
        # the Assist pipeline (voice assistant) shows correct local times
        # instead of raw UTC.
        if isinstance(start, datetime) and start.tzinfo is not None:
            start = dt_util.as_local(start)
        if isinstance(end, datetime) and end.tzinfo is not None:
            end = dt_util.as_local(end)

        return CalendarEvent(
            summary=ev.get("summary", "(No subject)"),
            start=start,
            end=end,
            description=ev.get("description", ""),
            location=ev.get("location", ""),
            uid=ev.get("uid"),
        )

    @staticmethod
    def _to_comparable_datetime(value: date | datetime) -> datetime:
        """Convert date or datetime to a tz-aware datetime for comparison."""
        return to_comparable_datetime(value, dt_util.DEFAULT_TIME_ZONE)
