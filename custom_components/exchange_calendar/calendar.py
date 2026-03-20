"""Calendar platform for Exchange Calendar."""
from __future__ import annotations

import logging
from datetime import date, datetime
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

from .const import DOMAIN, CONF_EMAIL, CONF_READ_ONLY, DEFAULT_READ_ONLY
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
    CoordinatorEntity[ExchangeCalendarCoordinator], CalendarEntity
):
    """Exchange Calendar entity with full CRUD support."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: ExchangeCalendarCoordinator,
        config_entry: ConfigEntry,
    ) -> None:
        """Initialize Exchange Calendar entity."""
        super().__init__(coordinator)
        email = config_entry.data[CONF_EMAIL]
        self._attr_unique_id = f"{DOMAIN}_{config_entry.entry_id}"
        self._attr_name = f"Exchange ({email})"
        self._config_entry = config_entry

        read_only = config_entry.options.get(CONF_READ_ONLY, DEFAULT_READ_ONLY)
        if read_only:
            self._attr_supported_features = CalendarEntityFeature(0)
        else:
            self._attr_supported_features = (
                CalendarEntityFeature.CREATE_EVENT
                | CalendarEntityFeature.DELETE_EVENT
                | CalendarEntityFeature.UPDATE_EVENT
            )

    @property
    def event(self) -> CalendarEvent | None:
        """Return the current or next upcoming event.

        Displayed on the calendar card in HA dashboard.
        """
        if not self.coordinator.data:
            return None

        now = dt_util.now()
        for ev in self.coordinator.data:
            end_dt = self._to_comparable_datetime(ev["end"])
            if end_dt >= now:
                return self._to_calendar_event(ev)

        return None

    async def async_get_events(
        self,
        hass: HomeAssistant,
        start_date: datetime,
        end_date: datetime,
    ) -> list[CalendarEvent]:
        """Return calendar events within a datetime range.

        Used by the calendar view and automations.
        Queries the Exchange server directly for the requested range,
        so both past and future events are available.
        """
        try:
            raw_events = await hass.async_add_executor_job(
                self.coordinator.client.get_events_range,
                start_date,
                end_date,
            )
        except Exception:
            _LOGGER.debug(
                "Direct range query failed, falling back to coordinator cache"
            )
            raw_events = self.coordinator.data or []

        events = []
        for ev in raw_events:
            start_dt = self._to_comparable_datetime(ev["start"])
            end_dt = self._to_comparable_datetime(ev["end"])

            if end_dt > start_date and start_dt < end_date:
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
            self.coordinator.client.create_event,
            summary,
            dtstart,
            dtend,
            description,
            location,
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
            self.coordinator.client.update_event,
            uid,
            event.get("summary"),
            event.get("dtstart"),
            event.get("dtend"),
            event.get("description"),
            event.get("location"),
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
            self.coordinator.client.delete_event,
            uid,
        )

        await self.coordinator.async_request_refresh()

    @staticmethod
    def _to_calendar_event(ev: dict[str, Any]) -> CalendarEvent:
        """Convert internal dict to HA CalendarEvent."""
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
        """Convert date or datetime to timezone-aware datetime for comparison."""
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)
            return value
        # date (all-day event) -> start of local day
        return dt_util.start_of_local_day(value)
