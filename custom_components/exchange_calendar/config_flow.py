"""Config flow for Exchange Calendar integration."""
from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.core import callback
import homeassistant.helpers.config_validation as cv

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
    CONF_CERT_PATH,
    CONF_KEY_PATH,
    CONF_USERAGENT,
    CONF_ALLOW_INSECURE_SSL,
    CONF_DAYS_TO_FETCH,
    CONF_MAX_EVENTS,
    CONF_UPDATE_INTERVAL,
    CONF_READ_ONLY,
    CONF_CALENDARS,
    AUTH_TYPE_BASIC,
    AUTH_TYPE_NTLM,
    AUTH_TYPE_OAUTH2,
    AUTH_TYPE_CBA,
    DEFAULT_DAYS_TO_FETCH,
    DEFAULT_MAX_EVENTS,
    DEFAULT_ALLOW_INSECURE_SSL,
    DEFAULT_UPDATE_INTERVAL,
    DEFAULT_READ_ONLY,
    DEFAULT_CALENDAR_KEY,
)
from homeassistant.components import persistent_notification

from .exchange_client import create_client, ExchangeAuthError, ExchangeConnectionError

_LOGGER = logging.getLogger(__name__)


class ExchangeCalendarConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Exchange Calendar."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the config flow."""
        self._auth_data: dict[str, Any] = {}
        self._last_error_detail: str = ""

    async def _async_validate_auth(self, auth_data: dict[str, Any]) -> dict[str, str]:
        """Validate credentials against the server.

        Returns an empty dict on success, or an ``errors`` dict (keyed for
        ``async_show_form``) describing what went wrong. On failure it also
        records the detail in ``self._last_error_detail`` and raises a debug
        notification, mirroring the behaviour of the individual setup steps.
        """
        errors: dict[str, str] = {}
        auth_type = auth_data[CONF_AUTH_TYPE]
        try:
            client = create_client(
                auth_type=auth_type,
                email=auth_data[CONF_EMAIL],
                server=auth_data.get(CONF_SERVER),
                username=auth_data.get(CONF_USERNAME),
                password=auth_data.get(CONF_PASSWORD),
                domain=auth_data.get(CONF_DOMAIN, ""),
                client_id=auth_data.get(CONF_CLIENT_ID),
                client_secret=auth_data.get(CONF_CLIENT_SECRET),
                tenant_id=auth_data.get(CONF_TENANT_ID),
                cert_path=auth_data.get(CONF_CERT_PATH),
                key_path=auth_data.get(CONF_KEY_PATH),
                useragent=auth_data.get(CONF_USERAGENT),
                allow_insecure_ssl=auth_data.get(
                    CONF_ALLOW_INSECURE_SSL, DEFAULT_ALLOW_INSECURE_SSL
                ),
            )
            await self.hass.async_add_executor_job(client.validate_connection)
        except ExchangeAuthError as err:
            self._last_error_detail = str(err)
            _LOGGER.error("%s auth failed: %s", auth_type, err)
            errors["base"] = "invalid_auth"
            self._send_debug_notification(f"{auth_type} Auth Error", err)
        except ExchangeConnectionError as err:
            self._last_error_detail = str(err)
            _LOGGER.error("%s connection failed: %s", auth_type, err)
            errors["base"] = "cannot_connect"
            self._send_debug_notification(f"{auth_type} Connection Error", err)
        except Exception as err:  # noqa: BLE001
            self._last_error_detail = str(err)
            _LOGGER.exception("Unexpected error during %s validation: %s", auth_type, err)
            errors["base"] = "unknown"
            self._send_debug_notification(f"{auth_type} Unexpected Error", err)
        return errors

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
            if auth_type == AUTH_TYPE_CBA:
                return await self.async_step_cba()
            return await self.async_step_oauth2()

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_AUTH_TYPE, default=AUTH_TYPE_NTLM): vol.In(
                        {
                            AUTH_TYPE_NTLM: "On-premise (NTLM)",
                            AUTH_TYPE_BASIC: "Basic (EWS)",
                            AUTH_TYPE_CBA: "On-premise (Certificate)",
                            AUTH_TYPE_OAUTH2: "Office 365 (Graph API)",
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
            auth_data = {
                CONF_AUTH_TYPE: AUTH_TYPE_NTLM,
                CONF_SERVER: user_input[CONF_SERVER],
                CONF_EMAIL: user_input[CONF_EMAIL],
                CONF_USERNAME: user_input.get(CONF_USERNAME, user_input[CONF_EMAIL]),
                CONF_PASSWORD: user_input[CONF_PASSWORD],
                CONF_DOMAIN: user_input.get(CONF_DOMAIN, ""),
                CONF_USERAGENT: user_input.get(CONF_USERAGENT),
                CONF_ALLOW_INSECURE_SSL: user_input.get(
                    CONF_ALLOW_INSECURE_SSL, DEFAULT_ALLOW_INSECURE_SSL
                ),
            }
            errors = await self._async_validate_auth(auth_data)
            if not errors:
                await self.async_set_unique_id(user_input[CONF_EMAIL].lower())
                self._abort_if_unique_id_configured()
                self._auth_data = auth_data
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
                    vol.Optional(CONF_USERAGENT): str,
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
            auth_data = {
                CONF_AUTH_TYPE: AUTH_TYPE_BASIC,
                CONF_SERVER: user_input[CONF_SERVER],
                CONF_EMAIL: user_input[CONF_EMAIL],
                CONF_USERNAME: user_input.get(CONF_USERNAME, user_input[CONF_EMAIL]),
                CONF_PASSWORD: user_input[CONF_PASSWORD],
                CONF_USERAGENT: user_input.get(CONF_USERAGENT),
            }
            errors = await self._async_validate_auth(auth_data)
            if not errors:
                await self.async_set_unique_id(user_input[CONF_EMAIL].lower())
                self._abort_if_unique_id_configured()
                self._auth_data = auth_data
                return await self.async_step_options()

        return self.async_show_form(
            step_id="basic",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_SERVER): str,
                    vol.Required(CONF_EMAIL): str,
                    vol.Optional(CONF_USERNAME): str,
                    vol.Required(CONF_PASSWORD): str,
                    vol.Optional(CONF_USERAGENT): str,
                }
            ),
            errors=errors,
            description_placeholders={"error_detail": self._last_error_detail},
        )

    async def async_step_cba(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Step 2b: Certificate-Based Authentication (CBA)."""
        errors: dict[str, str] = {}

        if user_input is not None:
            auth_data = {
                CONF_AUTH_TYPE: AUTH_TYPE_CBA,
                CONF_SERVER: user_input[CONF_SERVER],
                CONF_EMAIL: user_input[CONF_EMAIL],
                CONF_CERT_PATH: user_input[CONF_CERT_PATH],
                CONF_KEY_PATH: user_input.get(CONF_KEY_PATH),
                CONF_USERAGENT: user_input.get(CONF_USERAGENT),
                CONF_ALLOW_INSECURE_SSL: user_input.get(
                    CONF_ALLOW_INSECURE_SSL, DEFAULT_ALLOW_INSECURE_SSL
                ),
            }
            errors = await self._async_validate_auth(auth_data)
            if not errors:
                await self.async_set_unique_id(user_input[CONF_EMAIL].lower())
                self._abort_if_unique_id_configured()
                self._auth_data = auth_data
                return await self.async_step_options()

        return self.async_show_form(
            step_id="cba",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_SERVER): str,
                    vol.Required(CONF_EMAIL): str,
                    vol.Required(CONF_CERT_PATH): str,
                    vol.Optional(CONF_KEY_PATH): str,
                    vol.Optional(CONF_USERAGENT): str,
                    vol.Optional(
                        CONF_ALLOW_INSECURE_SSL, default=DEFAULT_ALLOW_INSECURE_SSL
                    ): bool,
                }
            ),
            errors=errors,
            description_placeholders={"error_detail": self._last_error_detail},
        )

    async def async_step_oauth2(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Step 2c: OAuth2 credentials."""
        errors: dict[str, str] = {}

        if user_input is not None:
            auth_data = {
                CONF_AUTH_TYPE: AUTH_TYPE_OAUTH2,
                CONF_EMAIL: user_input[CONF_EMAIL],
                CONF_CLIENT_ID: user_input[CONF_CLIENT_ID],
                CONF_CLIENT_SECRET: user_input[CONF_CLIENT_SECRET],
                CONF_TENANT_ID: user_input[CONF_TENANT_ID],
            }
            errors = await self._async_validate_auth(auth_data)
            if not errors:
                await self.async_set_unique_id(user_input[CONF_EMAIL].lower())
                self._abort_if_unique_id_configured()
                self._auth_data = auth_data
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
                    ): vol.All(int, vol.Range(min=30, max=90)),
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

    async def async_step_reauth(
        self, entry_data: dict[str, Any]
    ) -> ConfigFlowResult:
        """Handle re-authentication when the stored credentials stop working.

        Triggered automatically by Home Assistant once the integration raises
        ``ConfigEntryAuthFailed`` (e.g. the password has expired).
        """
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Ask for the new password / client secret and re-validate."""
        entry = self.hass.config_entries.async_get_entry(
            self.context["entry_id"]
        )
        return await self._async_update_credentials(
            entry=entry,
            step_id="reauth_confirm",
            reason="reauth_successful",
            user_input=user_input,
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Let the user proactively change the password before it expires."""
        entry = self.hass.config_entries.async_get_entry(
            self.context["entry_id"]
        )
        return await self._async_update_credentials(
            entry=entry,
            step_id="reconfigure",
            reason="reconfigure_successful",
            user_input=user_input,
        )

    async def _async_update_credentials(
        self,
        *,
        entry,
        step_id: str,
        reason: str,
        user_input: dict[str, Any] | None,
    ) -> ConfigFlowResult:
        """Shared handler for the reauth/reconfigure credential update.

        Only the secret field is editable: the password for NTLM/Basic, the
        client secret for OAuth2, or the certificate path for CBA.
        All other connection details are preserved.
        """
        auth_type = entry.data[CONF_AUTH_TYPE]
        if auth_type == AUTH_TYPE_OAUTH2:
            cred_key = CONF_CLIENT_SECRET
        elif auth_type == AUTH_TYPE_CBA:
            cred_key = CONF_CERT_PATH
        else:
            cred_key = CONF_PASSWORD
        errors: dict[str, str] = {}

        if user_input is not None:
            new_data = {**entry.data, cred_key: user_input[cred_key]}
            # Also update key path if provided during CBA reconfigure
            if auth_type == AUTH_TYPE_CBA and CONF_KEY_PATH in user_input:
                new_data[CONF_KEY_PATH] = user_input[CONF_KEY_PATH]
            errors = await self._async_validate_auth(new_data)
            if not errors:
                return self.async_update_reload_and_abort(
                    entry,
                    data=new_data,
                    reason=reason,
                )

        schema_fields = {vol.Required(cred_key): str}
        if auth_type == AUTH_TYPE_CBA:
            schema_fields[vol.Optional(CONF_KEY_PATH)] = str

        return self.async_show_form(
            step_id=step_id,
            data_schema=vol.Schema(schema_fields),
            errors=errors,
            description_placeholders={
                "email": entry.data[CONF_EMAIL],
                "error_detail": self._last_error_detail,
            },
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        """Get the options flow handler."""
        return ExchangeCalendarOptionsFlow()


class ExchangeCalendarOptionsFlow(OptionsFlow):
    """Handle options flow for Exchange Calendar."""

    async def _async_discover_calendars(self) -> dict[str, str]:
        """Return {calendar_key: label} for the multi-select, or {} on failure.

        The mailbox's default calendar is represented by DEFAULT_CALENDAR_KEY so
        that its entity keeps the original unique_id.
        """
        data = self.config_entry.data
        try:
            client = create_client(
                auth_type=data[CONF_AUTH_TYPE],
                email=data[CONF_EMAIL],
                server=data.get(CONF_SERVER),
                username=data.get(CONF_USERNAME),
                password=data.get(CONF_PASSWORD),
                domain=data.get(CONF_DOMAIN, ""),
                client_id=data.get(CONF_CLIENT_ID),
                client_secret=data.get(CONF_CLIENT_SECRET),
                tenant_id=data.get(CONF_TENANT_ID),
                cert_path=data.get(CONF_CERT_PATH),
                key_path=data.get(CONF_KEY_PATH),
                useragent=data.get(CONF_USERAGENT),
                allow_insecure_ssl=data.get(
                    CONF_ALLOW_INSECURE_SSL, DEFAULT_ALLOW_INSECURE_SSL
                ),
            )
            calendars = await self.hass.async_add_executor_job(
                client.list_calendars
            )
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("Could not list calendars for options: %s", err)
            return {}

        choices: dict[str, str] = {}
        for cal in calendars:
            if cal.get("is_default"):
                choices[DEFAULT_CALENDAR_KEY] = f"{cal['name']} (default)"
            else:
                choices[cal["id"]] = cal["name"]
        return choices

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle options."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        calendar_choices = await self._async_discover_calendars()

        current_calendars = self.config_entry.options.get(
            CONF_CALENDARS
        ) or [DEFAULT_CALENDAR_KEY]
        # Drop any stored keys that no longer exist server-side.
        if calendar_choices:
            current_calendars = [
                k for k in current_calendars if k in calendar_choices
            ] or [DEFAULT_CALENDAR_KEY]

        schema: dict[Any, Any] = {}
        if calendar_choices:
            schema[
                vol.Optional(CONF_CALENDARS, default=current_calendars)
            ] = cv.multi_select(calendar_choices)

        schema.update(
            {
                vol.Optional(
                    CONF_DAYS_TO_FETCH,
                    default=self.config_entry.options.get(
                        CONF_DAYS_TO_FETCH, DEFAULT_DAYS_TO_FETCH
                    ),
                ): vol.All(int, vol.Range(min=30, max=90)),
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
        )

        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(schema),
        )
