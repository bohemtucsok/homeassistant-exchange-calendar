"""The Exchange Calendar integration."""
from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import (
    DOMAIN,
    PLATFORMS,
    CONF_AUTH_TYPE,
    CONF_SERVER,
    CONF_EMAIL,
    CONF_USERNAME,
    CONF_PASSWORD,
    CONF_DOMAIN,
    CONF_CLIENT_ID,
    CONF_CLIENT_SECRET,
    CONF_TENANT_ID,
    CONF_ALLOW_INSECURE_SSL,
    DEFAULT_ALLOW_INSECURE_SSL,
)
from .coordinator import ExchangeCalendarCoordinator
from .exchange_client import create_client

_LOGGER = logging.getLogger(__name__)

type ExchangeCalendarConfigEntry = ConfigEntry[ExchangeCalendarCoordinator]


async def async_setup_entry(
    hass: HomeAssistant, entry: ExchangeCalendarConfigEntry
) -> bool:
    """Set up Exchange Calendar from a config entry."""
    client = create_client(
        auth_type=entry.data[CONF_AUTH_TYPE],
        email=entry.data[CONF_EMAIL],
        server=entry.data.get(CONF_SERVER),
        username=entry.data.get(CONF_USERNAME),
        password=entry.data.get(CONF_PASSWORD),
        domain=entry.data.get(CONF_DOMAIN),
        client_id=entry.data.get(CONF_CLIENT_ID),
        client_secret=entry.data.get(CONF_CLIENT_SECRET),
        tenant_id=entry.data.get(CONF_TENANT_ID),
        allow_insecure_ssl=entry.data.get(
            CONF_ALLOW_INSECURE_SSL, DEFAULT_ALLOW_INSECURE_SSL
        ),
    )

    # Test connection in executor (exchangelib is synchronous)
    await hass.async_add_executor_job(client.connect)

    # Create coordinator and perform first data fetch
    coordinator = ExchangeCalendarCoordinator(hass, entry, client)
    await coordinator.async_config_entry_first_refresh()

    # Store coordinator on config entry
    entry.runtime_data = coordinator

    # Listen for options changes
    entry.async_on_unload(entry.add_update_listener(_async_update_options))

    # Forward setup to calendar platform
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    return True


async def async_unload_entry(
    hass: HomeAssistant, entry: ExchangeCalendarConfigEntry
) -> bool:
    """Unload a config entry."""
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)


async def _async_update_options(
    hass: HomeAssistant, entry: ExchangeCalendarConfigEntry
) -> None:
    """Reload integration when options change."""
    await hass.config_entries.async_reload(entry.entry_id)
