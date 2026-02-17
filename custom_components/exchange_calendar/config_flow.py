"""Config flow for Exchange Calendar integration."""
from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.core import callback

from .const import (
    DOMAIN,
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
    CONF_DAYS_TO_FETCH,
    CONF_MAX_EVENTS,
    CONF_UPDATE_INTERVAL,
    CONF_READ_ONLY,
    AUTH_TYPE_BASIC,
    AUTH_TYPE_NTLM,
    AUTH_TYPE_OAUTH2,
    DEFAULT_DAYS_TO_FETCH,
    DEFAULT_MAX_EVENTS,
    DEFAULT_ALLOW_INSECURE_SSL,
    DEFAULT_UPDATE_INTERVAL,
    DEFAULT_READ_ONLY,
)
from homeassistant.components import persistent_notification

from .exchange_client import ExchangeClient, ExchangeAuthError, ExchangeConnectionError

_LOGGER = logging.getLogger(__name__)


class ExchangeCalendarConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Exchange Calendar."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the config flow."""
        self._auth_data: dict[str, Any] = {}
        self._last_error_detail: str = ""

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Step 1: Choose authentication type."""
        if user_input is not None:
            auth_type = user_input[CONF_AUTH_TYPE]
            if auth_type == AUTH_TYPE_NTLM:
                return await self.async_step_ntlm()
            if auth_type == AUTH_TYPE_BASIC:
                return await self.async_step_basic()
            return await self.async_step_oauth2()

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_AUTH_TYPE, default=AUTH_TYPE_NTLM): vol.In(
                        {
                            AUTH_TYPE_NTLM: "On-premise (NTLM)",
                            AUTH_TYPE_BASIC: "Basic (EWS)",
                            AUTH_TYPE_OAUTH2: "Office 365 (OAuth2)",
                        }
                    ),
                }
            ),
        )

    def _send_debug_notification(self, title: str, err: Exception) -> None:
        """Send a persistent notification with error details for debugging."""
        error_type = type(err).__name__
        error_msg = str(err)
        cause = str(err.__cause__) if err.__cause__ else "N/A"
        cause_type = type(err.__cause__).__name__ if err.__cause__ else "N/A"

        message = (
            f"**{title}**\n\n"
            f"- **Error type:** `{error_type}`\n"
            f"- **Message:** {error_msg}\n"
            f"- **Cause type:** `{cause_type}`\n"
            f"- **Cause:** {cause}\n\n"
            f"Check HA logs for full stack trace."
        )
        persistent_notification.async_create(
            self.hass,
            message=message,
            title=f"Exchange Calendar Debug: {title}",
            notification_id=f"exchange_calendar_debug_{id(err)}",
        )

    async def async_step_ntlm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Step 2a: NTLM credentials."""
        errors: dict[str, str] = {}

        if user_input is not None:
            try:
                client = ExchangeClient(
                    auth_type=AUTH_TYPE_NTLM,
                    server=user_input[CONF_SERVER],
                    email=user_input[CONF_EMAIL],
                    username=user_input.get(CONF_USERNAME, user_input[CONF_EMAIL]),
                    password=user_input[CONF_PASSWORD],
                    domain=user_input.get(CONF_DOMAIN, ""),
                    allow_insecure_ssl=user_input.get(
                        CONF_ALLOW_INSECURE_SSL, DEFAULT_ALLOW_INSECURE_SSL
                    ),
                )
                await self.hass.async_add_executor_job(client.validate_connection)
            except ExchangeAuthError as err:
                self._last_error_detail = str(err)
                _LOGGER.error("NTLM auth failed: %s", err)
                errors["base"] = "invalid_auth"
                self._send_debug_notification("NTLM Auth Error", err)
            except ExchangeConnectionError as err:
                self._last_error_detail = str(err)
                _LOGGER.error("NTLM connection failed: %s", err)
                errors["base"] = "cannot_connect"
                self._send_debug_notification("NTLM Connection Error", err)
            except Exception as err:
                self._last_error_detail = str(err)
                _LOGGER.exception("Unexpected error during NTLM validation: %s", err)
                errors["base"] = "unknown"
                self._send_debug_notification("NTLM Unexpected Error", err)
            else:
                await self.async_set_unique_id(user_input[CONF_EMAIL].lower())
                self._abort_if_unique_id_configured()

                self._auth_data = {
                    CONF_AUTH_TYPE: AUTH_TYPE_NTLM,
                    CONF_SERVER: user_input[CONF_SERVER],
                    CONF_EMAIL: user_input[CONF_EMAIL],
                    CONF_USERNAME: user_input.get(
                        CONF_USERNAME, user_input[CONF_EMAIL]
                    ),
                    CONF_PASSWORD: user_input[CONF_PASSWORD],
                    CONF_DOMAIN: user_input.get(CONF_DOMAIN, ""),
                    CONF_ALLOW_INSECURE_SSL: user_input.get(
                        CONF_ALLOW_INSECURE_SSL, DEFAULT_ALLOW_INSECURE_SSL
                    ),
                }
                return await self.async_step_options()

        return self.async_show_form(
            step_id="ntlm",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_SERVER): str,
                    vol.Required(CONF_EMAIL): str,
                    vol.Optional(CONF_USERNAME): str,
                    vol.Required(CONF_PASSWORD): str,
                    vol.Optional(CONF_DOMAIN, default=""): str,
                    vol.Optional(
                        CONF_ALLOW_INSECURE_SSL, default=DEFAULT_ALLOW_INSECURE_SSL
                    ): bool,
                }
            ),
            errors=errors,
            description_placeholders={"error_detail": self._last_error_detail},
        )

    async def async_step_basic(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Step 2c: Basic (EWS) credentials for AWS WorkMail and similar."""
        errors: dict[str, str] = {}

        if user_input is not None:
            try:
                client = ExchangeClient(
                    auth_type=AUTH_TYPE_BASIC,
                    server=user_input[CONF_SERVER],
                    email=user_input[CONF_EMAIL],
                    username=user_input.get(CONF_USERNAME, user_input[CONF_EMAIL]),
                    password=user_input[CONF_PASSWORD],
                )
                await self.hass.async_add_executor_job(client.validate_connection)
            except ExchangeAuthError as err:
                self._last_error_detail = str(err)
                _LOGGER.error("Basic auth failed: %s", err)
                errors["base"] = "invalid_auth"
                self._send_debug_notification("Basic Auth Error", err)
            except ExchangeConnectionError as err:
                self._last_error_detail = str(err)
                _LOGGER.error("Basic connection failed: %s", err)
                errors["base"] = "cannot_connect"
                self._send_debug_notification("Basic Connection Error", err)
            except Exception as err:
                self._last_error_detail = str(err)
                _LOGGER.exception("Unexpected error during Basic validation: %s", err)
                errors["base"] = "unknown"
                self._send_debug_notification("Basic Unexpected Error", err)
            else:
                await self.async_set_unique_id(user_input[CONF_EMAIL].lower())
                self._abort_if_unique_id_configured()

                self._auth_data = {
                    CONF_AUTH_TYPE: AUTH_TYPE_BASIC,
                    CONF_SERVER: user_input[CONF_SERVER],
                    CONF_EMAIL: user_input[CONF_EMAIL],
                    CONF_USERNAME: user_input.get(
                        CONF_USERNAME, user_input[CONF_EMAIL]
                    ),
                    CONF_PASSWORD: user_input[CONF_PASSWORD],
                }
                return await self.async_step_options()

        return self.async_show_form(
            step_id="basic",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_SERVER): str,
                    vol.Required(CONF_EMAIL): str,
                    vol.Optional(CONF_USERNAME): str,
                    vol.Required(CONF_PASSWORD): str,
                }
            ),
            errors=errors,
            description_placeholders={"error_detail": self._last_error_detail},
        )

    async def async_step_oauth2(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Step 2b: OAuth2 credentials."""
        errors: dict[str, str] = {}

        if user_input is not None:
            try:
                client = ExchangeClient(
                    auth_type=AUTH_TYPE_OAUTH2,
                    email=user_input[CONF_EMAIL],
                    client_id=user_input[CONF_CLIENT_ID],
                    client_secret=user_input[CONF_CLIENT_SECRET],
                    tenant_id=user_input[CONF_TENANT_ID],
                )
                await self.hass.async_add_executor_job(client.validate_connection)
            except ExchangeAuthError as err:
                self._last_error_detail = str(err)
                _LOGGER.error("OAuth2 auth failed: %s", err)
                errors["base"] = "invalid_auth"
                self._send_debug_notification("OAuth2 Auth Error", err)
            except ExchangeConnectionError as err:
                self._last_error_detail = str(err)
                _LOGGER.error("OAuth2 connection failed: %s", err)
                errors["base"] = "cannot_connect"
                self._send_debug_notification("OAuth2 Connection Error", err)
            except Exception as err:
                self._last_error_detail = str(err)
                _LOGGER.exception("Unexpected error during OAuth2 validation: %s", err)
                errors["base"] = "unknown"
                self._send_debug_notification("OAuth2 Unexpected Error", err)
            else:
                await self.async_set_unique_id(user_input[CONF_EMAIL].lower())
                self._abort_if_unique_id_configured()

                self._auth_data = {
                    CONF_AUTH_TYPE: AUTH_TYPE_OAUTH2,
                    CONF_EMAIL: user_input[CONF_EMAIL],
                    CONF_CLIENT_ID: user_input[CONF_CLIENT_ID],
                    CONF_CLIENT_SECRET: user_input[CONF_CLIENT_SECRET],
                    CONF_TENANT_ID: user_input[CONF_TENANT_ID],
                }
                return await self.async_step_options()

        return self.async_show_form(
            step_id="oauth2",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_EMAIL): str,
                    vol.Required(CONF_TENANT_ID): str,
                    vol.Required(CONF_CLIENT_ID): str,
                    vol.Required(CONF_CLIENT_SECRET): str,
                }
            ),
            errors=errors,
            description_placeholders={"error_detail": self._last_error_detail},
        )

    async def async_step_options(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Step 3: Calendar options."""
        if user_input is not None:
            return self.async_create_entry(
                title=f"Exchange ({self._auth_data[CONF_EMAIL]})",
                data=self._auth_data,
                options=user_input,
            )

        return self.async_show_form(
            step_id="options",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        CONF_DAYS_TO_FETCH, default=DEFAULT_DAYS_TO_FETCH
                    ): vol.All(int, vol.Range(min=1, max=90)),
                    vol.Optional(
                        CONF_MAX_EVENTS, default=DEFAULT_MAX_EVENTS
                    ): vol.All(int, vol.Range(min=1, max=500)),
                    vol.Optional(
                        CONF_UPDATE_INTERVAL, default=DEFAULT_UPDATE_INTERVAL
                    ): vol.All(int, vol.Range(min=1, max=60)),
                    vol.Optional(
                        CONF_READ_ONLY, default=DEFAULT_READ_ONLY
                    ): bool,
                }
            ),
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        """Get the options flow handler."""
        return ExchangeCalendarOptionsFlow(config_entry)


class ExchangeCalendarOptionsFlow(OptionsFlow):
    """Handle options flow for Exchange Calendar."""

    def __init__(self, config_entry) -> None:
        """Initialize options flow."""
        self.config_entry = config_entry

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle options."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        CONF_DAYS_TO_FETCH,
                        default=self.config_entry.options.get(
                            CONF_DAYS_TO_FETCH, DEFAULT_DAYS_TO_FETCH
                        ),
                    ): vol.All(int, vol.Range(min=1, max=90)),
                    vol.Optional(
                        CONF_MAX_EVENTS,
                        default=self.config_entry.options.get(
                            CONF_MAX_EVENTS, DEFAULT_MAX_EVENTS
                        ),
                    ): vol.All(int, vol.Range(min=1, max=500)),
                    vol.Optional(
                        CONF_UPDATE_INTERVAL,
                        default=self.config_entry.options.get(
                            CONF_UPDATE_INTERVAL, DEFAULT_UPDATE_INTERVAL
                        ),
                    ): vol.All(int, vol.Range(min=1, max=60)),
                    vol.Optional(
                        CONF_READ_ONLY,
                        default=self.config_entry.options.get(
                            CONF_READ_ONLY, DEFAULT_READ_ONLY
                        ),
                    ): bool,
                }
            ),
        )
