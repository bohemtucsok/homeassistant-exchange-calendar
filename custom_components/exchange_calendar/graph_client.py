"""Microsoft Graph API client for Office 365 calendar access.

Replaces exchangelib OAuth2 for Office 365 users.
On-premise (NTLM/Basic) continues to use ExchangeClient with exchangelib.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

from .exchange_client import ExchangeAuthError, ExchangeConnectionError

_LOGGER = logging.getLogger(__name__)

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
TOKEN_URL = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"

# Token refresh buffer - renew 5 minutes before expiry
TOKEN_REFRESH_BUFFER = timedelta(minutes=5)


class GraphCalendarClient:
    """Microsoft Graph API client for Office 365 calendars.

    Provides the same interface as ExchangeClient so that coordinator.py,
    calendar.py and __init__.py can use either backend transparently.
    """

    def __init__(
        self,
        email: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
    ) -> None:
        self._email = email
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._access_token: str | None = None
        self._token_expiry: datetime | None = None
        self._session: requests.Session | None = None

    # ── Token management ─────────────────────────────────────────────

    def _ensure_token(self) -> None:
        """Acquire or refresh OAuth2 token via client credentials flow."""
        now = datetime.now(timezone.utc)
        if (
            self._access_token is not None
            and self._token_expiry is not None
            and now < self._token_expiry - TOKEN_REFRESH_BUFFER
        ):
            return

        url = TOKEN_URL.format(tenant=self._tenant_id)
        data = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "scope": GRAPH_SCOPE,
        }

        try:
            resp = self._get_session().post(url, data=data, timeout=30)
        except requests.RequestException as err:
            _LOGGER.error("[Graph] Token request failed: %s", err)
            raise ExchangeConnectionError(
                f"Token request failed: {err}"
            ) from err

        if resp.status_code != 200:
            body = resp.text
            _LOGGER.error(
                "[Graph] Token request returned %s: %s", resp.status_code, body
            )
            raise ExchangeAuthError(
                f"Token request failed ({resp.status_code}): {body}"
            )

        token_data = resp.json()
        self._access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 3600)
        self._token_expiry = now + timedelta(seconds=int(expires_in))
        _LOGGER.debug("[Graph] Token acquired, expires in %s seconds", expires_in)

    def _get_session(self) -> requests.Session:
        """Return or create a requests session."""
        if self._session is None:
            self._session = requests.Session()
        return self._session

    def _headers(self) -> dict[str, str]:
        """Return request headers with bearer token."""
        self._ensure_token()
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }

    def _graph_request(
        self,
        method: str,
        path: str,
        json_body: dict | None = None,
        params: dict | None = None,
    ) -> requests.Response:
        """Make an authenticated Graph API request with error handling."""
        url = f"{GRAPH_BASE_URL}{path}"
        try:
            resp = self._get_session().request(
                method,
                url,
                headers=self._headers(),
                json=json_body,
                params=params,
                timeout=30,
            )
        except requests.RequestException as err:
            _LOGGER.error("[Graph] Request failed: %s %s - %s", method, path, err)
            self._access_token = None
            raise ExchangeConnectionError(
                f"Graph API request failed: {err}"
            ) from err

        if resp.status_code in (401, 403):
            _LOGGER.error(
                "[Graph] Auth error %s on %s %s: %s",
                resp.status_code, method, path, resp.text,
            )
            self._access_token = None
            raise ExchangeAuthError(
                f"Graph API auth error ({resp.status_code}): {resp.text}"
            )

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After", "60")
            _LOGGER.warning(
                "[Graph] Rate limited (429), Retry-After: %s", retry_after
            )
            raise ExchangeConnectionError(
                f"Graph API rate limited, retry after {retry_after}s"
            )

        if resp.status_code >= 400:
            _LOGGER.error(
                "[Graph] Error %s on %s %s: %s",
                resp.status_code, method, path, resp.text,
            )
            raise ExchangeConnectionError(
                f"Graph API error ({resp.status_code}): {resp.text}"
            )

        return resp

    # ── Connection ───────────────────────────────────────────────────

    def connect(self) -> None:
        """Acquire token and verify API access. SYNCHRONOUS."""
        _LOGGER.debug(
            "[Graph] Connecting: email=%s, tenant=%s, client_id=%s",
            self._email, self._tenant_id, self._client_id,
        )
        self._ensure_token()
        _LOGGER.info("[Graph] Connected successfully for %s", self._email)

    def validate_connection(self) -> bool:
        """Test connection by fetching 1 event from calendarView."""
        _LOGGER.debug("[Graph] Starting connection validation...")
        self.connect()

        now = datetime.now(timezone.utc)
        end = now + timedelta(days=1)
        params = {
            "startDateTime": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endDateTime": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "$top": "1",
            "$select": "id,subject",
        }
        self._graph_request(
            "GET",
            f"/users/{self._email}/calendarView",
            params=params,
        )
        _LOGGER.info("[Graph] Validation OK for %s", self._email)
        return True

    # ── Read events ──────────────────────────────────────────────────

    def get_events(
        self, days_to_fetch: int = 30, max_events: int = 50
    ) -> list[dict[str, Any]]:
        """Fetch calendar events using calendarView (expands recurrences)."""
        self._ensure_token()
        now = datetime.now(timezone.utc)
        end = now + timedelta(days=days_to_fetch)

        params = {
            "startDateTime": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endDateTime": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "$top": str(min(max_events, 1000)),
            "$orderby": "start/dateTime",
            "$select": "id,subject,start,end,location,body,organizer,isAllDay",
        }

        events: list[dict[str, Any]] = []
        path = f"/users/{self._email}/calendarView"

        try:
            while path and len(events) < max_events:
                resp = self._graph_request("GET", path, params=params)
                data = resp.json()

                for item in data.get("value", []):
                    if len(events) >= max_events:
                        break
                    events.append(self._convert_graph_event(item))

                # Pagination: follow @odata.nextLink
                next_link = data.get("@odata.nextLink")
                if next_link:
                    # nextLink is a full URL; extract path + query
                    path = next_link.replace(GRAPH_BASE_URL, "")
                    params = None  # nextLink already includes query params
                else:
                    break

        except (ExchangeAuthError, ExchangeConnectionError):
            self._access_token = None
            raise
        except Exception as err:
            _LOGGER.error("[Graph] Failed to fetch events: %s", err)
            self._access_token = None
            raise ExchangeConnectionError(
                f"Event fetch failed: {err}"
            ) from err

        events.sort(key=lambda e: self._sort_key(e["start"]))
        return events[:max_events]

    def get_events_range(
        self, start_dt: datetime, end_dt: datetime, max_events: int = 200
    ) -> list[dict[str, Any]]:
        """Fetch calendar events for an arbitrary date range.

        Used by async_get_events() to serve the HA calendar frontend,
        including past events that the coordinator cache does not hold.
        """
        self._ensure_token()

        params = {
            "startDateTime": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            if start_dt.tzinfo
            else start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endDateTime": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            if end_dt.tzinfo
            else end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "$top": str(min(max_events, 1000)),
            "$orderby": "start/dateTime",
            "$select": "id,subject,start,end,location,body,organizer,isAllDay",
        }

        events: list[dict[str, Any]] = []
        path = f"/users/{self._email}/calendarView"

        try:
            while path and len(events) < max_events:
                resp = self._graph_request("GET", path, params=params)
                data = resp.json()
                for item in data.get("value", []):
                    if len(events) >= max_events:
                        break
                    events.append(self._convert_graph_event(item))
                next_link = data.get("@odata.nextLink")
                if next_link:
                    path = next_link.replace(GRAPH_BASE_URL, "")
                    params = None
                else:
                    break
        except (ExchangeAuthError, ExchangeConnectionError):
            self._access_token = None
            raise
        except Exception as err:
            _LOGGER.error("[Graph] Failed to fetch events range: %s", err)
            self._access_token = None
            raise ExchangeConnectionError(
                f"Event range fetch failed: {err}"
            ) from err

        events.sort(key=lambda e: self._sort_key(e["start"]))
        return events[:max_events]

    # ── Create event ─────────────────────────────────────────────────

    def create_event(
        self,
        summary: str,
        start: datetime,
        end: datetime,
        description: str | None = None,
        location: str | None = None,
    ) -> str:
        """Create a new calendar event. Returns the event ID."""
        body = self._build_event_body(summary, start, end, description, location)

        resp = self._graph_request(
            "POST",
            f"/users/{self._email}/events",
            json_body=body,
        )
        event_id = resp.json()["id"]
        _LOGGER.info("[Graph] Created event: %s", summary)
        return event_id

    # ── Update event ─────────────────────────────────────────────────

    def update_event(
        self,
        uid: str,
        summary: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        description: str | None = None,
        location: str | None = None,
    ) -> None:
        """Update an existing calendar event by ID."""
        body: dict[str, Any] = {}
        if summary is not None:
            body["subject"] = summary
        if start is not None:
            body["start"] = self._to_graph_datetime(start)
            body["isAllDay"] = isinstance(start, date) and not isinstance(
                start, datetime
            )
        if end is not None:
            body["end"] = self._to_graph_datetime(end)
        if description is not None:
            body["body"] = {"contentType": "Text", "content": description}
        if location is not None:
            body["location"] = {"displayName": location}

        if body:
            self._graph_request(
                "PATCH",
                f"/users/{self._email}/events/{uid}",
                json_body=body,
            )
            _LOGGER.info(
                "[Graph] Updated event: %s (fields: %s)",
                uid,
                list(body.keys()),
            )

    # ── Delete event ─────────────────────────────────────────────────

    def delete_event(self, uid: str) -> None:
        """Delete a calendar event by ID."""
        self._graph_request(
            "DELETE",
            f"/users/{self._email}/events/{uid}",
        )
        _LOGGER.info("[Graph] Deleted event: %s", uid)

    # ── Helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _to_graph_datetime(dt: datetime | date) -> dict[str, str]:
        """Convert Python datetime/date to Graph API dateTimeTimeZone."""
        if isinstance(dt, date) and not isinstance(dt, datetime):
            return {
                "dateTime": f"{dt.isoformat()}T00:00:00",
                "timeZone": "UTC",
            }
        if dt.tzinfo is not None:
            dt_utc = dt.astimezone(timezone.utc)
        else:
            dt_utc = dt
        return {
            "dateTime": dt_utc.strftime("%Y-%m-%dT%H:%M:%S"),
            "timeZone": "UTC",
        }

    @staticmethod
    def _build_event_body(
        summary: str,
        start: datetime,
        end: datetime,
        description: str | None,
        location: str | None,
    ) -> dict[str, Any]:
        """Build Graph API event JSON body."""
        is_all_day = isinstance(start, date) and not isinstance(start, datetime)
        body: dict[str, Any] = {
            "subject": summary,
            "start": GraphCalendarClient._to_graph_datetime(start),
            "end": GraphCalendarClient._to_graph_datetime(end),
            "isAllDay": is_all_day,
        }
        if description:
            body["body"] = {"contentType": "Text", "content": description}
        if location:
            body["location"] = {"displayName": location}
        return body

    @staticmethod
    def _convert_graph_event(event: dict[str, Any]) -> dict[str, Any]:
        """Convert Graph API event JSON to our standard event dict.

        Must return the exact same structure as ExchangeClient._convert_calendar_item().
        """
        is_all_day = event.get("isAllDay", False)

        start_raw = event.get("start", {})
        end_raw = event.get("end", {})

        if is_all_day:
            start = GraphCalendarClient._parse_graph_date(start_raw)
            end = GraphCalendarClient._parse_graph_date(end_raw)
        else:
            start = GraphCalendarClient._parse_graph_datetime(start_raw)
            end = GraphCalendarClient._parse_graph_datetime(end_raw)

        organizer_name = ""
        organizer_data = event.get("organizer", {})
        if organizer_data:
            email_data = organizer_data.get("emailAddress", {})
            organizer_name = email_data.get("name") or email_data.get("address", "")

        location_name = ""
        location_data = event.get("location", {})
        if location_data:
            location_name = location_data.get("displayName", "")

        description = ""
        body_data = event.get("body", {})
        if body_data:
            description = body_data.get("content", "")

        return {
            "uid": event.get("id"),
            "summary": event.get("subject") or "(No subject)",
            "start": start,
            "end": end,
            "location": location_name,
            "description": description,
            "organizer": organizer_name,
            "is_all_day": is_all_day,
        }

    @staticmethod
    def _parse_graph_datetime(dt_data: dict[str, str]) -> datetime:
        """Parse Graph API dateTimeTimeZone to timezone-aware Python datetime."""
        dt_str = dt_data.get("dateTime", "")
        tz_str = dt_data.get("timeZone", "UTC")

        # Graph returns fractional seconds sometimes: "2024-01-15T10:00:00.0000000"
        # Truncate to seconds
        if "." in dt_str:
            dt_str = dt_str.split(".")[0]

        try:
            dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            _LOGGER.warning("[Graph] Could not parse datetime: %s", dt_str)
            dt = datetime.now()

        if tz_str == "UTC":
            return dt.replace(tzinfo=timezone.utc)

        # Try to resolve Windows timezone name to IANA
        try:
            from zoneinfo import ZoneInfo

            # Graph may return Windows timezone names like "Central European Standard Time"
            # or IANA names like "Europe/Budapest"
            try:
                tz = ZoneInfo(tz_str)
            except KeyError:
                # Try mapping from Windows timezone name
                from exchangelib.winzone import MS_TIMEZONE_TO_IANA_MAP

                iana_name = MS_TIMEZONE_TO_IANA_MAP.get(tz_str)
                if iana_name:
                    tz = ZoneInfo(iana_name)
                else:
                    _LOGGER.warning(
                        "[Graph] Unknown timezone '%s', falling back to UTC",
                        tz_str,
                    )
                    tz = timezone.utc
            return dt.replace(tzinfo=tz)
        except ImportError:
            return dt.replace(tzinfo=timezone.utc)

    @staticmethod
    def _parse_graph_date(dt_data: dict[str, str]) -> date:
        """Parse Graph API dateTimeTimeZone to plain Python date (for all-day events)."""
        dt_str = dt_data.get("dateTime", "")

        # Extract just the date portion from "2024-01-15T00:00:00.0000000"
        date_str = dt_str.split("T")[0] if "T" in dt_str else dt_str

        try:
            parts = date_str.split("-")
            return date(int(parts[0]), int(parts[1]), int(parts[2]))
        except (ValueError, IndexError):
            _LOGGER.warning("[Graph] Could not parse date: %s", dt_str)
            return date.today()

    @staticmethod
    def _sort_key(dt_or_date) -> datetime:
        """Normalize date/datetime for sorting (same as ExchangeClient)."""
        if isinstance(dt_or_date, datetime):
            if dt_or_date.tzinfo is None:
                return dt_or_date
            return dt_or_date.replace(tzinfo=None)
        return datetime(dt_or_date.year, dt_or_date.month, dt_or_date.day)
