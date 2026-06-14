"""DataUpdateCoordinator for Exchange Calendar."""
from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import (
    DataUpdateCoordinator,
    UpdateFailed,
)

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

        Uses a double-buffer strategy: start from the existing cache and
        overwrite entries only when the server responds successfully.
        This prevents flickering (empty calendar) on transient errors.
        """
        await self._async_discover_calendars()

        days = self.config_entry.options.get(
            CONF_DAYS_TO_FETCH, DEFAULT_DAYS_TO_FETCH
        )
        max_events = self.config_entry.options.get(
            CONF_MAX_EVENTS, DEFAULT_MAX_EVENTS
        )

        keys = self._selected_calendar_keys()
        # Start with a copy of the previous data so stale entries survive.
        new_data: dict[str, list[dict[str, Any]]] = dict(self.data or {})
        all_failed = True

        for key in keys:
            calendar_id = None if key == DEFAULT_CALENDAR_KEY else key
            try:
                new_data[key] = await self.hass.async_add_executor_job(
                    self.client.get_events, days, max_events, calendar_id
                )
                all_failed = False
            except ExchangeAuthError as err:
                # Trigger the reauth flow so the user can update the (likely
                # expired) password without removing the integration.
                raise ConfigEntryAuthFailed(
                    f"Exchange authentication error: {err}"
                ) from err
            except ExchangeConnectionError as err:
                # Keep the stale entry in new_data; do not overwrite it.
                _LOGGER.warning("Failed to fetch calendar '%s': %s", key, err)
            except Exception as err:
                _LOGGER.exception("Unexpected error fetching Exchange events")
                raise UpdateFailed(f"Unexpected error: {err}") from err

        if keys and all_failed:
            _LOGGER.warning(
                "All selected calendars were unreachable, keeping stale data.",
            )

        return new_data
