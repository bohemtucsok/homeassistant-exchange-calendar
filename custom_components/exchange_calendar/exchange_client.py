"""Exchange client wrapper for exchangelib.

This is the Python equivalent of the MMM-Exchange node_helper.js,
using exchangelib instead of httpntlm + raw SOAP XML.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, date
from typing import Any

from exchangelib import (
    Account,
    BASIC,
    CalendarItem,
    Configuration,
    Credentials,
    DELEGATE,
    EWSDateTime,
    EWSTimeZone,
    NTLM,
    OAuth2Credentials,
)
from exchangelib.winzone import MS_TIMEZONE_TO_IANA_MAP
from exchangelib.errors import (
    AutoDiscoverFailed,
    ErrorAccessDenied,
    ErrorFolderNotFound,
    ErrorItemNotFound,
    ErrorMailboxMoveInProgress,
    ErrorMailboxStoreUnavailable,
    ErrorNonExistentMailbox,
    TransportError,
    UnauthorizedError,
)
from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter
from exchangelib.items import SEND_TO_NONE, SEND_TO_ALL_AND_SAVE_COPY

from .const import AUTH_TYPE_BASIC, AUTH_TYPE_NTLM, AUTH_TYPE_OAUTH2

_LOGGER = logging.getLogger(__name__)

# Fix for Exchange servers that report "Customized Time Zone" instead of a
# standard Windows timezone name.  Map it to Europe/Budapest (CET/CEST).
if "Customized Time Zone" not in MS_TIMEZONE_TO_IANA_MAP:
    MS_TIMEZONE_TO_IANA_MAP["Customized Time Zone"] = "Europe/Budapest"


class ExchangeConnectionError(Exception):
    """Exchange connection error."""


class ExchangeAuthError(Exception):
    """Exchange authentication error."""


class ExchangeClient:
    """Wrapper around exchangelib for Exchange calendar access.

    Supports:
    - NTLM authentication (on-premise Exchange)
    - OAuth2 authentication (Office 365)
    - Self-signed SSL certificates
    - Full CRUD operations on calendar events
    """

    def __init__(
        self,
        auth_type: str,
        email: str,
        server: str | None = None,
        username: str | None = None,
        password: str | None = None,
        domain: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        tenant_id: str | None = None,
        allow_insecure_ssl: bool = False,
    ) -> None:
        self._auth_type = auth_type
        self._email = email
        # Strip protocol prefix - exchangelib expects bare hostname
        # (MMM-Exchange: this.config.host.replace(/\/+$/, "") + "/EWS/Exchange.asmx")
        self._server = self._clean_server(server) if server else None
        self._username = username or email
        self._password = password
        self._domain = domain
        self._client_id = client_id
        self._client_secret = client_secret
        self._tenant_id = tenant_id
        self._allow_insecure_ssl = allow_insecure_ssl
        self._account: Account | None = None
        self._original_adapter_cls = None

    @staticmethod
    def _clean_server(server: str) -> str:
        """Strip protocol prefix and trailing slashes from server hostname.

        Accepts: "https://ex.example.com", "http://ex.example.com",
                 "https://ex.example.com/", "ex.example.com"
        Returns: "ex.example.com"
        """
        s = server.strip()
        for prefix in ("https://", "http://"):
            if s.lower().startswith(prefix):
                s = s[len(prefix):]
                break
        return s.rstrip("/")

    def _setup_ssl(self) -> None:
        """Disable SSL verification if needed.

        Equivalent to MMM-Exchange: process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"
        """
        if self._allow_insecure_ssl:
            self._original_adapter_cls = BaseProtocol.HTTP_ADAPTER_CLS
            BaseProtocol.HTTP_ADAPTER_CLS = NoVerifyHTTPAdapter
            _LOGGER.warning(
                "SSL certificate verification DISABLED for Exchange connection. "
                "Only use this with self-signed certificates."
            )

    def _restore_ssl(self) -> None:
        """Restore SSL settings."""
        if self._original_adapter_cls is not None:
            BaseProtocol.HTTP_ADAPTER_CLS = self._original_adapter_cls
            self._original_adapter_cls = None

    def _build_credentials(self):
        """Build credentials based on auth type."""
        if self._auth_type in (AUTH_TYPE_NTLM, AUTH_TYPE_BASIC):
            username = self._username
            if self._domain and "\\" not in username and "@" not in username:
                username = f"{self._domain}\\{username}"
            return Credentials(username=username, password=self._password)

        if self._auth_type == AUTH_TYPE_OAUTH2:
            return OAuth2Credentials(
                client_id=self._client_id,
                client_secret=self._client_secret,
                tenant_id=self._tenant_id,
            )

        raise ValueError(f"Unknown auth type: {self._auth_type}")

    def _build_config(self, credentials) -> Configuration:
        """Build Exchange configuration."""
        if self._auth_type == AUTH_TYPE_NTLM:
            return Configuration(
                server=self._server,
                credentials=credentials,
                auth_type=NTLM,
            )

        if self._auth_type == AUTH_TYPE_BASIC:
            return Configuration(
                server=self._server,
                credentials=credentials,
                auth_type=BASIC,
            )

        # OAuth2 - Office 365
        return Configuration(
            server="outlook.office365.com",
            credentials=credentials,
        )

    def connect(self) -> Account:
        """Connect to Exchange server. SYNCHRONOUS - must run in executor."""
        _LOGGER.debug(
            "[Exchange] Connecting: auth=%s, server=%s, email=%s, username=%s, "
            "domain=%s, insecure_ssl=%s",
            self._auth_type, self._server, self._email, self._username,
            self._domain, self._allow_insecure_ssl,
        )
        self._setup_ssl()
        try:
            credentials = self._build_credentials()
            _LOGGER.debug("[Exchange] Credentials built OK")

            config = self._build_config(credentials)
            _LOGGER.debug("[Exchange] Configuration built OK (server=%s)", self._server)

            _LOGGER.debug("[Exchange] Creating Account object for %s...", self._email)
            self._account = Account(
                primary_smtp_address=self._email,
                config=config,
                autodiscover=False,
                access_type=DELEGATE,
            )
            _LOGGER.info("[Exchange] Connected successfully to %s as %s", self._server, self._email)
            return self._account
        except (UnauthorizedError, ErrorAccessDenied) as err:
            _LOGGER.error("[Exchange] AUTH FAILED: %s (type: %s)", err, type(err).__name__)
            raise ExchangeAuthError(f"Authentication failed: {err}") from err
        except (TransportError, AutoDiscoverFailed, ConnectionError) as err:
            _LOGGER.error("[Exchange] CONNECTION FAILED: %s (type: %s)", err, type(err).__name__)
            raise ExchangeConnectionError(f"Connection failed: {err}") from err
        except Exception as err:
            _LOGGER.error("[Exchange] UNEXPECTED ERROR during connect: %s (type: %s)", err, type(err).__name__)
            raise ExchangeConnectionError(f"Unexpected error: {err}") from err

    def validate_connection(self) -> bool:
        """Test connection (used by config flow)."""
        _LOGGER.debug("[Exchange] Starting connection validation...")
        try:
            account = self.connect()
            tz = account.default_timezone
            _LOGGER.debug("[Exchange] Timezone: %s", tz)

            now = EWSDateTime.now(tz)
            _LOGGER.debug("[Exchange] Fetching test events (1 day, 1 item)...")
            items = list(account.calendar.view(start=now, end=now + timedelta(days=1), max_items=1))
            _LOGGER.info("[Exchange] Validation OK - found %d event(s)", len(items))
            return True
        except (ExchangeAuthError, ExchangeConnectionError):
            raise
        except Exception as err:
            _LOGGER.error("[Exchange] Validation FAILED: %s (type: %s)", err, type(err).__name__)
            raise ExchangeConnectionError(f"Validation failed: {err}") from err

    def _ensure_connected(self) -> Account:
        """Ensure we have an active connection."""
        if self._account is None:
            self.connect()
        return self._account

    def get_events(
        self, days_to_fetch: int = 14, max_events: int = 50
    ) -> list[dict[str, Any]]:
        """Fetch calendar events.

        Python equivalent of MMM-Exchange fetchEvents() + buildSoapXml().
        calendar.view() automatically expands recurring events.
        """
        account = self._ensure_connected()
        tz = account.default_timezone
        now = EWSDateTime.now(tz)
        end = now + timedelta(days=days_to_fetch)

        events = []
        try:
            for item in account.calendar.view(
                start=now, end=end, max_items=max_events
            ):
                events.append(self._convert_calendar_item(item))
        except (
            TransportError,
            ErrorMailboxStoreUnavailable,
            ErrorMailboxMoveInProgress,
        ) as err:
            _LOGGER.error("Failed to fetch Exchange events: %s", err)
            self._account = None
            raise ExchangeConnectionError(f"Event fetch failed: {err}") from err

        events.sort(key=lambda e: self._sort_key(e["start"]))
        return events[:max_events]

    @staticmethod
    def _sort_key(dt_or_date) -> datetime:
        """Normalize date/datetime for sorting (all-day date vs timed datetime)."""
        if isinstance(dt_or_date, datetime):
            # Already a datetime (or EWSDateTime which is a datetime subclass)
            if dt_or_date.tzinfo is None:
                return dt_or_date
            return dt_or_date.replace(tzinfo=None)
        # It's a date (or EWSDate) - convert to midnight datetime for comparison
        return datetime(dt_or_date.year, dt_or_date.month, dt_or_date.day)

    def create_event(
        self,
        summary: str,
        start: datetime,
        end: datetime,
        description: str | None = None,
        location: str | None = None,
    ) -> str:
        """Create a new calendar event. Returns the item UID."""
        account = self._ensure_connected()
        tz = account.default_timezone

        ews_start = self._to_ews_datetime(start, tz)
        ews_end = self._to_ews_datetime(end, tz)

        item = CalendarItem(
            account=account,
            folder=account.calendar,
            subject=summary,
            start=ews_start,
            end=ews_end,
            body=description or "",
            location=location or "",
        )
        item.save(send_meeting_invitations=SEND_TO_NONE)
        _LOGGER.info("Created Exchange event: %s", summary)
        return item.uid

    def update_event(
        self,
        uid: str,
        summary: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        description: str | None = None,
        location: str | None = None,
    ) -> None:
        """Update an existing calendar event by UID."""
        account = self._ensure_connected()
        tz = account.default_timezone

        item = self._get_item_by_uid(uid)
        if item is None:
            raise ExchangeConnectionError(f"Event not found: {uid}")

        update_fields = []
        if summary is not None:
            item.subject = summary
            update_fields.append("subject")
        if start is not None:
            item.start = self._to_ews_datetime(start, tz)
            update_fields.append("start")
        if end is not None:
            item.end = self._to_ews_datetime(end, tz)
            update_fields.append("end")
        if description is not None:
            item.body = description
            update_fields.append("body")
        if location is not None:
            item.location = location
            update_fields.append("location")

        if update_fields:
            item.save(
                update_fields=update_fields,
                send_meeting_invitations=SEND_TO_NONE,
            )
            _LOGGER.info("Updated Exchange event: %s (fields: %s)", uid, update_fields)

    def delete_event(self, uid: str) -> None:
        """Delete a calendar event by UID."""
        item = self._get_item_by_uid(uid)
        if item is None:
            raise ExchangeConnectionError(f"Event not found: {uid}")

        item.delete(send_meeting_cancellations=SEND_TO_NONE)
        _LOGGER.info("Deleted Exchange event: %s", uid)

    def _get_item_by_uid(self, uid: str) -> CalendarItem | None:
        """Find a calendar item by its UID."""
        account = self._ensure_connected()
        try:
            items = list(
                account.calendar.filter(uid=uid)
            )
            if items:
                return items[0]
        except (ErrorItemNotFound, IndexError):
            pass
        return None

    @staticmethod
    def _to_ews_datetime(dt: datetime | date, tz: EWSTimeZone) -> EWSDateTime:
        """Convert a Python datetime to EWSDateTime."""
        if isinstance(dt, date) and not isinstance(dt, datetime):
            return EWSDateTime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=tz)
        if dt.tzinfo is None:
            return EWSDateTime.from_datetime(dt.replace(tzinfo=tz))
        return EWSDateTime.from_datetime(dt)

    @staticmethod
    def _to_python_dt(ews_dt) -> datetime | date:
        """Convert EWSDateTime/EWSDate to plain Python datetime/date.

        EWSDateTime.astimezone() only accepts EWSTimeZone, which crashes
        when Home Assistant tries to call .astimezone(datetime.timezone.utc).
        Converting to plain Python types avoids this.
        """
        if ews_dt is None:
            return None
        if isinstance(ews_dt, datetime):
            # EWSDateTime → plain datetime
            # EWSTimeZone inherits from zoneinfo.ZoneInfo, so we can use it
            # directly as tzinfo, but we must construct a plain datetime
            # (not EWSDateTime) to avoid EWSDateTime.__new__ type checking.
            tz = ews_dt.tzinfo
            return datetime(
                ews_dt.year, ews_dt.month, ews_dt.day,
                ews_dt.hour, ews_dt.minute, ews_dt.second,
                ews_dt.microsecond, tzinfo=tz,
            )
        if isinstance(ews_dt, date):
            return date(ews_dt.year, ews_dt.month, ews_dt.day)
        return ews_dt

    @staticmethod
    def _convert_calendar_item(item: CalendarItem) -> dict[str, Any]:
        """Convert exchangelib CalendarItem to dict.

        Field mapping from MMM-Exchange parseXmlResponse():
          subject -> summary
          start -> start
          end -> end
          location -> location
          organizer -> organizer (stored separately)
        """
        if item.is_all_day:
            start = item.start
            end = item.end
            if hasattr(start, "date"):
                start = start.date()
            elif isinstance(start, date):
                pass
            if hasattr(end, "date"):
                end = end.date()
            elif isinstance(end, date):
                pass
            # Ensure plain Python date
            if isinstance(start, date) and not isinstance(start, datetime):
                start = date(start.year, start.month, start.day)
            if isinstance(end, date) and not isinstance(end, datetime):
                end = date(end.year, end.month, end.day)
        else:
            start = ExchangeClient._to_python_dt(item.start)
            end = ExchangeClient._to_python_dt(item.end)

        organizer_name = ""
        if item.organizer:
            organizer_name = (
                item.organizer.name
                or getattr(item.organizer, "email_address", "")
                or ""
            )

        return {
            "uid": item.uid or (str(item.id) if item.id else None),
            "summary": item.subject or "(No subject)",
            "start": start,
            "end": end,
            "location": item.location or "",
            "description": item.text_body or "",
            "organizer": organizer_name,
            "is_all_day": item.is_all_day or False,
        }
