"""DataUpdateCoordinator for Exchange Calendar."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import (
    DataUpdateCoordinator,
    UpdateFailed,
)
from homeassistant.util import dt as dt_util

from .const import (
    DOMAIN,
    CONF_DAYS_TO_FETCH,
    CONF_MAX_EVENTS,
    CONF_UPDATE_INTERVAL,
    CONF_CALENDARS,
    DEFAULT_DAYS_TO_FETCH,
    DEFAULT_MAX_EVENTS,
    DEFAULT_UPDATE_INTERVAL,
    DEFAULT_CALENDAR_KEY,
)
from .exchange_client import ExchangeClient, ExchangeConnectionError, ExchangeAuthError

_LOGGER = logging.getLogger(__name__)


class ExchangeCalendarCoordinator(
    DataUpdateCoordinator[dict[str, list[dict[str, Any]]]]
):
    """Coordinator for periodic Exchange calendar event fetching.

    Fetches one or more calendars per refresh. ``data`` is keyed by calendar
    key (``DEFAULT_CALENDAR_KEY`` for the primary calendar, otherwise the
    backend calendar id), each holding that calendar's list of event dicts.
    """

    config_entry: ConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        client: ExchangeClient,
    ) -> None:
        """Initialize the coordinator."""
        self.client = client
        # Discovered calendars (populated lazily on first refresh).
        self.available_calendars: list[dict[str, Any]] = []
        # Calendar key -> display name, for entity naming.
        self.calendar_names: dict[str, str] = {}
        self._discovered = False
        # Forward window (tz-aware) covered by ``data`` after the last
        # successful refresh, and whether each calendar's fetch hit the
        # ``max_events`` cap (the cache may then be incomplete for the window).
        self.window_start: datetime | None = None
        self.window_end: datetime | None = None
        self.truncated: dict[str, bool] = {}
        interval = config_entry.options.get(
            CONF_UPDATE_INTERVAL, DEFAULT_UPDATE_INTERVAL
        )
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_{config_entry.entry_id}",
            update_interval=timedelta(minutes=interval),
            config_entry=config_entry,
        )

    def _selected_calendar_keys(self) -> list[str]:
        """Return the calendar keys selected in options (default: primary)."""
        selected = self.config_entry.options.get(CONF_CALENDARS)
        if not selected:
            return [DEFAULT_CALENDAR_KEY]
        return list(selected)

    async def _async_discover_calendars(self) -> None:
        """Discover the mailbox's calendars once and cache id->name mapping."""
        if self._discovered:
            return
        try:
            calendars = await self.hass.async_add_executor_job(
                self.client.list_calendars
            )
        except (ExchangeAuthError, ExchangeConnectionError) as err:
            # Non-fatal: fall back to just the default calendar.
            _LOGGER.debug("Calendar discovery failed: %s", err)
            return

        self.available_calendars = calendars
        names: dict[str, str] = {}
        for cal in calendars:
            key = DEFAULT_CALENDAR_KEY if cal.get("is_default") else cal["id"]
            names[key] = cal.get("name") or "Calendar"
        self.calendar_names = names
        self._discovered = True

    async def _async_update_data(self) -> dict[str, list[dict[str, Any]]]:
        """Fetch events for each selected calendar.

        Fetches a forward window starting at the beginning of the local day so
        the whole current day/week/month view can be served from cache.
        Uses a double-buffer: the previous data is kept for any calendar whose
        fetch fails transiently, so the entities never flicker to empty. If
        every calendar fails, ``UpdateFailed`` is raised - HA retains the last
        good data and marks the entities unavailable.

        exchangelib is synchronous, so we run it via async_add_executor_job.
        """
        await self._async_discover_calendars()

        days = self.config_entry.options.get(
            CONF_DAYS_TO_FETCH, DEFAULT_DAYS_TO_FETCH
        )
        max_events = self.config_entry.options.get(
            CONF_MAX_EVENTS, DEFAULT_MAX_EVENTS
        )

        window_start = dt_util.start_of_local_day(dt_util.now())
        window_end = window_start + timedelta(days=days)

        keys = self._selected_calendar_keys()
        # Start from the previous data so stale entries survive a transient
        # failure of an individual calendar (anti-flicker).
        new_data: dict[str, list[dict[str, Any]]] = dict(self.data or {})
        truncated: dict[str, bool] = dict(self.truncated)
        connection_errors = 0
        last_conn_err: Exception | None = None

        for key in keys:
            calendar_id = None if key == DEFAULT_CALENDAR_KEY else key
            try:
                events = await self.hass.async_add_executor_job(
                    self.client.get_events_range,
                    window_start,
                    window_end,
                    max_events,
                    calendar_id,
                )
            except ExchangeAuthError as err:
                # Trigger the reauth flow so the user can update the (likely
                # expired) password without removing the integration.
                raise ConfigEntryAuthFailed(
                    f"Exchange authentication error: {err}"
                ) from err
            except ExchangeConnectionError as err:
                # Keep whatever we had for this calendar; only seed an empty
                # list if there was never any data for it.
                _LOGGER.warning("Failed to fetch calendar '%s': %s", key, err)
                new_data.setdefault(key, [])
                connection_errors += 1
                last_conn_err = err
                continue
            except Exception as err:
                _LOGGER.exception("Unexpected error fetching Exchange events")
                raise UpdateFailed(f"Unexpected error: {err}") from err

            new_data[key] = events
            # If the fetch hit the cap, the cache may not cover the whole
            # window; the entity then falls back to a live range query.
            truncated[key] = len(events) >= max_events

        # If every selected calendar was unreachable, report a failed refresh.
        # HA keeps the previous ``data`` and marks the entities unavailable,
        # so the user sees the outage without the calendar going empty.
        if keys and connection_errors == len(keys):
            raise UpdateFailed(
                f"Exchange server unreachable: {last_conn_err}"
            )

        # Drop keys that are no longer selected so stale calendars don't linger.
        new_data = {k: v for k, v in new_data.items() if k in keys}
        self.truncated = {k: v for k, v in truncated.items() if k in keys}
        self.window_start = window_start
        self.window_end = window_end
        return new_data
