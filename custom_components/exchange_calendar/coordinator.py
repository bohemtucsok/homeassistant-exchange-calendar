"""Data coordinator for Exchange Calendar.

Patched behavior:
- Exchange/EWS is only queried by the coordinator polling cycle.
- Events are fetched in chunks and stored in coordinator.data.
- Calendar UI browsing must use coordinator.data only, not live EWS calls.
"""

from __future__ import annotations

from datetime import timedelta
import logging
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import (
    DOMAIN,
    CONF_DAYS_TO_FETCH,
    CONF_MAX_EVENTS,
    CONF_UPDATE_INTERVAL,
    DEFAULT_DAYS_TO_FETCH,
    DEFAULT_MAX_EVENTS,
    DEFAULT_UPDATE_INTERVAL,
)
from .exchange_client import ExchangeClient, ExchangeConnectionError, ExchangeAuthError

_LOGGER = logging.getLogger(__name__)

# Extra cache tuning without requiring const.py/config_flow changes.
# You can later expose these as options in config_flow if desired.
DEFAULT_CACHE_PAST_DAYS = 31
DEFAULT_CACHE_CHUNK_DAYS = 30

OPT_CACHE_PAST_DAYS = "cache_past_days"
OPT_CACHE_CHUNK_DAYS = "cache_chunk_days"


class ExchangeCalendarCoordinator(DataUpdateCoordinator[list[dict[str, Any]]]):
    """Coordinator for periodic Exchange calendar event fetching.

    Important:
    - This coordinator is the ONLY place where periodic Exchange/EWS fetching occurs.
    - Calendar UI requests should be served from coordinator.data.
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

        interval = config_entry.options.get(
            CONF_UPDATE_INTERVAL,
            DEFAULT_UPDATE_INTERVAL,
        )

        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_{config_entry.entry_id}",
            update_interval=timedelta(minutes=interval),
            config_entry=config_entry,
        )

    async def _async_update_data(self) -> list[dict[str, Any]]:
        """Fetch events from Exchange server into cache.

        exchangelib is synchronous, so we run it via async_add_executor_job.

        This fetches a sliding window:
        - past days: cache_past_days, default 31
        - future days: existing Days to fetch option
        - chunks: cache_chunk_days, default 30

        This avoids one huge EWS request while still filling the HA calendar UI cache.
        """
        try:
            return await self.hass.async_add_executor_job(
                self._fetch_cached_window_sync
            )

        except ExchangeAuthError as err:
            raise UpdateFailed(f"Exchange authentication error: {err}") from err

        except ExchangeConnectionError as err:
            raise UpdateFailed(f"Exchange server unreachable: {err}") from err

        except Exception as err:
            _LOGGER.exception("Unexpected error fetching Exchange events")
            raise UpdateFailed(f"Unexpected error: {err}") from err

    def _fetch_cached_window_sync(self) -> list[dict[str, Any]]:
        """Synchronously fetch a chunked calendar window from Exchange.

        This intentionally uses the ExchangeClient internals because the current
        public get_events() API only supports 'now + days_to_fetch'. For a useful
        calendar UI cache we also want a small past window and chunking.
        """
        future_days = int(
            self.config_entry.options.get(
                CONF_DAYS_TO_FETCH,
                DEFAULT_DAYS_TO_FETCH,
            )
        )
        max_events_per_chunk = int(
            self.config_entry.options.get(
                CONF_MAX_EVENTS,
                DEFAULT_MAX_EVENTS,
            )
        )

        past_days = int(
            self.config_entry.options.get(
                OPT_CACHE_PAST_DAYS,
                DEFAULT_CACHE_PAST_DAYS,
            )
        )
        chunk_days = int(
            self.config_entry.options.get(
                OPT_CACHE_CHUNK_DAYS,
                DEFAULT_CACHE_CHUNK_DAYS,
            )
        )

        if future_days < 1:
            future_days = DEFAULT_DAYS_TO_FETCH
        if max_events_per_chunk < 1:
            max_events_per_chunk = DEFAULT_MAX_EVENTS
        if past_days < 0:
            past_days = 0
        if chunk_days < 1:
            chunk_days = DEFAULT_CACHE_CHUNK_DAYS

        account = self.client._ensure_connected()
        tz = account.default_timezone
        #now = account.default_timezone.localize(account.default_timezone.localize) if False else None
        # Use exchangelib-style current time via account default timezone.
        # EWSDateTime is imported in exchange_client, but not exported here.
        # datetime arithmetic against exchangelib datetimes is supported once we
        # get 'now' from the client library.
        from exchangelib import EWSDateTime

        now = EWSDateTime.now(tz)
        window_start = now - timedelta(days=past_days)
        window_end = now + timedelta(days=future_days)

        _LOGGER.debug(
            "[Exchange] Cache refresh window: %s → %s, chunk_days=%s, max_events_per_chunk=%s",
            window_start,
            window_end,
            chunk_days,
            max_events_per_chunk,
        )

        all_events: list[dict[str, Any]] = []
        cursor = window_start
        chunk_index = 0

        while cursor < window_end:
            chunk_index += 1
            chunk_end = min(cursor + timedelta(days=chunk_days), window_end)

            _LOGGER.debug(
                "[Exchange] Fetching cache chunk %s: %s → %s",
                chunk_index,
                cursor,
                chunk_end,
            )

            chunk_count = 0

            for item in account.calendar.view(
                start=cursor,
                end=chunk_end,
                max_items=max_events_per_chunk,
            ):
                converted = self.client._convert_calendar_item(item)
                all_events.append(converted)
                chunk_count += 1

            _LOGGER.debug(
                "[Exchange] Cache chunk %s returned %s event(s)",
                chunk_index,
                chunk_count,
            )

            cursor = chunk_end

        deduped = self._dedupe_events(all_events)
        deduped.sort(key=self._event_sort_key)

        _LOGGER.info(
            "[Exchange] Cache refresh complete: %s raw event(s), %s deduped event(s)",
            len(all_events),
            len(deduped),
        )

        return deduped

    @staticmethod
    def _dedupe_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Deduplicate events using strongest available key."""
        deduped: dict[Any, dict[str, Any]] = {}

        for event in events:
            key = (
                event.get("uid")
                or event.get("id")
                or event.get("item_id")
                or event.get("ews_id")
                or (
                    event.get("summary")
                    or event.get("message")
                    or event.get("subject"),
                    event.get("start")
                    or event.get("start_time"),
                    event.get("end")
                    or event.get("end_time"),
                )
            )

            deduped[key] = event

        return list(deduped.values())

    @staticmethod
    def _event_sort_key(event: dict[str, Any]) -> Any:
        """Sort events by normalized, timezone-aware datetime."""
        from datetime import date, datetime, time
        from homeassistant.util import dt as dt_util

        value = (
            event.get("start")
            or event.get("start_time")
            or event.get("begin")
        )

        if value is None:
            return datetime.max.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)

        # ✅ datetime → ensure tz-aware
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)
            return dt_util.as_local(value)

        # ✅ date → convert to datetime at midnight
        if isinstance(value, date):
            return datetime.combine(
                value,
                time.min,
                tzinfo=dt_util.DEFAULT_TIME_ZONE,
            )

        # ✅ string → try parse
        if isinstance(value, str):
            parsed = dt_util.parse_datetime(value)
            if parsed is not None:
                if parsed.tzinfo is None:
                    return parsed.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)
                return dt_util.as_local(parsed)

        # fallback
        return datetime.max.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)
