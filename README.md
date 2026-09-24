# Exchange Calendar for Home Assistant

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-41BDF5.svg)](https://github.com/hacs/integration)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Home Assistant custom integration for Microsoft Exchange calendars.

Supports **on-premise Exchange** (NTLM / Basic / client-certificate via EWS) and **Office 365** (via Microsoft Graph API) with full CRUD operations, multiple calendars per account and past-event browsing.

> Based on the [MMM-Exchange](https://github.com/bohemtucsok/MMM-Exchange) MagicMirror module, ported to Python/Home Assistant.

## Features

- **Read** calendar events with automatic recurring event expansion
- **Create**, **update** and **delete** events from Home Assistant (optional read-only mode)
- **Multiple calendars per account** — each selected calendar becomes its own `calendar.*` entity
- **Past-event browsing** — the calendar panel can go back in time, not just forward
- On-premise Exchange (NTLM authentication)
- Basic EWS authentication (AWS WorkMail and similar)
- **Certificate-Based Authentication** (client-certificate TLS) for corporate on-premise Exchange
- Office 365 / Microsoft 365 (Microsoft Graph API)
- Self-signed SSL certificate support
- **Change credentials without re-adding** — automatic re-authentication when a password, secret or certificate expires, plus proactive Reconfigure
- Cache-first calendar view, no flicker on transient errors, robust handling of malformed events
- Extra event attributes (`free_busy_status`, `sensitivity`, `categories`) for automations
- Configurable polling interval, date range, event limits and custom User-Agent
- **Voice assistant support** (Home Assistant Voice PE / Assist pipeline)
- Hungarian and English UI translations
- HACS compatible

## Installation

### HACS (Recommended)

1. Open HACS in Home Assistant
2. Click the three dots menu (top right) > **Custom repositories**
3. Add this repository URL: `https://github.com/bohemtucsok/homeassistant-exchange-calendar`
4. Category: **Integration**
5. Click **Add**, then find "Exchange Calendar" and install
6. Restart Home Assistant

### Manual

1. Copy the `custom_components/exchange_calendar/` folder to your Home Assistant `config/custom_components/` directory
2. Restart Home Assistant

## Configuration

### On-premise Exchange (NTLM)

1. Go to **Settings** > **Devices & Services** > **Add Integration**
2. Search for "Exchange Calendar"
3. Select **On-premise (NTLM)**
4. Fill in:
   - **Exchange server hostname**: e.g., `mail.example.com`
   - **Email address**: Your email (e.g., `user@example.com`)
   - **Username**: (Optional) If different from email
   - **Password**: Your password
   - **Windows domain**: (Optional) e.g., `MYDOMAIN`
   - **Allow insecure SSL**: Enable for self-signed certificates
5. Configure calendar options (days to fetch, max events, update interval)

> **Note for MMM-Exchange users**: The configuration fields map directly:
> - `host` -> Exchange server hostname
> - `username` -> Email / Username
> - `password` -> Password
> - `domain` -> Windows domain
> - `allowInsecureSSL` -> Allow insecure SSL

### On-premise Exchange (Certificate-Based Authentication)

For Exchange servers that require client certificate authentication instead of passwords (common in some corporate environments).

#### Prerequisites

1. Export your client certificate as a PEM file containing **both the certificate and private key**.
   - If your certificate is in PFX format, convert it first:
     ```bash
     openssl pkcs12 -in certificate.pfx -out client.pem -nodes
     ```
2. Make sure the PEM file **does not have a password** on the private key. If it does, remove it:
   ```bash
   openssl rsa -in client.pem -out client_unencrypted.pem
   ```
   Then use `client_unencrypted.pem` as the certificate file.
3. Place the PEM file somewhere accessible by your Home Assistant instance (e.g. `/config/ssl/exchange.pem`).

#### Home Assistant Setup

1. Go to **Settings** > **Devices & Services** > **Add Integration**
2. Search for "Exchange Calendar"
3. Select **On-premise (Certificate)**
4. Fill in:
   - **Exchange server hostname**: e.g., `mail.example.com`
   - **Email address**: Your email (e.g., `user@example.com`)
   - **Path to client certificate**: Absolute path to the PEM file (e.g., `/config/ssl/exchange.pem`)
   - **Path to private key** (Optional): **Leave empty** in most cases. Only fill this if your private key is stored in a separate file from the certificate.
   - **Allow insecure SSL**: Enable for self-signed certificates
5. Configure calendar options

> **Re-authentication**: When your certificate expires, use the integration's **Reconfigure** or **Re-authenticate** menu to update the certificate path without removing the integration.

Notes:
- **Allow insecure SSL** works with certificate auth too: server-certificate verification is skipped while your client certificate is still presented.
- Certificate connections honour the custom **User-Agent** set in Options.
- Protect the PEM file — it contains your private key: `chmod 600 /config/ssl/exchange.pem`. If you use a separate key file, the integration combines certificate and key into a private temporary file (owner-only permissions) for the lifetime of the config entry.

### Office 365 (Graph API)

Uses the Microsoft Graph API for Office 365 / Microsoft 365 mailboxes.

#### Prerequisites: Azure AD App Registration

1. Go to [Azure Portal](https://portal.azure.com) > **Azure Active Directory** > **App registrations**
2. Click **New registration**
   - Name: `Home Assistant Exchange Calendar`
   - Supported account types: **Single tenant**
3. After creation, note the **Application (Client) ID** and **Directory (Tenant) ID**
4. Go to **Certificates & secrets** > **New client secret**
   - Note the **Value** (this is your Client Secret)
5. Go to **API permissions** > **Add a permission**
   - Select **Microsoft Graph** > **Application permissions**
   - Add: `Calendars.ReadWrite` and `User.Read.All`
   - Click **Grant admin consent** for both permissions

#### Home Assistant Setup

1. Go to **Settings** > **Devices & Services** > **Add Integration**
2. Search for "Exchange Calendar"
3. Select **Office 365 (Graph API)**
4. Fill in:
   - **Email address**: The mailbox email
   - **Azure AD Tenant ID**: From app registration
   - **Application (Client) ID**: From app registration
   - **Client Secret**: From app registration
5. Configure calendar options

> **Upgrading from v1.x (EWS/OAuth2)?** You need to add the `User.Read.All` Application permission to your Azure AD app and grant admin consent. Your existing configuration will continue to work.

## Usage

### Calendar Card

Add a calendar card to your dashboard:

```yaml
type: calendar
entities:
  - calendar.exchange_your_email_example_com
```

### Services

#### Create Event
```yaml
service: calendar.create_event
target:
  entity_id: calendar.exchange_your_email_example_com
data:
  summary: "Team Meeting"
  start_date_time: "2025-03-01 10:00:00"
  end_date_time: "2025-03-01 11:00:00"
  description: "Weekly sync"
  location: "Conference Room A"
```

#### Automations

Use calendar events as triggers:

```yaml
automation:
  - alias: "Meeting reminder"
    trigger:
      - platform: calendar
        event: start
        entity_id: calendar.exchange_your_email_example_com
        offset: "-00:15:00"
    action:
      - service: notify.mobile_app
        data:
          message: "Meeting starts in 15 minutes!"
```

### Voice Assistant (Voice PE / Assist)

The integration is compatible with the Home Assistant Assist pipeline, allowing you to query calendar events using voice commands:

- **"What's on my calendar tomorrow?"** - Query events using natural language
- **"What do I have next week?"** - Supports relative date expressions

Event times are automatically converted to the local timezone, so the voice assistant always reports the correct time.

> **Tip**: For best results, use the OpenAI Conversation integration with `gpt-4o`. The `gpt-4o-mini` model can sometimes be inaccurate with date calculations.

## Options

After initial setup, you can modify these options via **Settings** > **Devices & Services** > **Exchange Calendar** > **Configure**:

| Option | Default | Description |
|--------|---------|-------------|
| Calendars to show | primary only | Which of the mailbox's calendars are exposed as entities (multi-select; each becomes its own `calendar.*` entity) |
| Days to fetch ahead | 30 | Size of the cached forward window, counted from the start of the current day (30–90). Requests inside it are served from cache |
| Maximum number of events | 50 | Per-calendar cap for the cached window; a calendar with more events falls back to live queries |
| Update interval | 5 min | How often to poll the Exchange server |
| Read-only mode | off | Disables create/update/delete on the entities |
| Custom User-Agent | empty | Overrides the `exchangelib` User-Agent for servers that block it (applies to all EWS connections of this HA instance) |

## Troubleshooting

### Cannot connect to Exchange server
- Verify the server hostname is correct and reachable from your HA instance
- For on-premise: ensure EWS endpoint is accessible (`https://server/EWS/Exchange.asmx`)
- For self-signed certificates: enable "Allow insecure SSL"
- Check HA logs for detailed error messages

### Authentication failed
- NTLM: Try both `user@domain.com` and `DOMAIN\user` formats
- OAuth2: Verify admin consent was granted for `Calendars.ReadWrite`
- OAuth2: Ensure the client secret hasn't expired
- Certificate: the PEM must contain the certificate **and** the unencrypted private key (or point "Path to private key" to a separate key file); the path must be absolute and readable by Home Assistant
- Certificate: if the certificate expired, use **Re-authenticate** / **Reconfigure** to point to the renewed file
- `401 Unauthorized` behind IIS/NTLM: try a custom **User-Agent** in Options (see below)

### No events showing
- Check that the mailbox has calendar events within the configured date range
- Increase "Days to fetch ahead" in options
- Verify the email address matches the mailbox
- Looking for a secondary calendar? Select it under **Configure** → **Calendars to show**
- Past months are fetched live from the server; if that fails the panel shows only cached (upcoming) events — check the HA log for connection errors

### Calendar panel is slow or "cannot load events"
- Requests inside the cached window are instant; only ranges outside it (e.g. past months) hit the server live
- Raise "Maximum number of events" if a busy calendar exceeds it — otherwise that calendar always queries live
- Some servers throttle or block the default `exchangelib` User-Agent — set a custom one in Options

## Security Considerations

- Always use HTTPS when connecting to your Exchange server
- For on-premise NTLM connections, it is strongly recommended to access Exchange over a trusted internal network or VPN
- Use a dedicated service account with minimal permissions where possible
- Keep client-certificate PEM files private (`chmod 600`) — they contain your private key. With a separate key file the integration keeps a combined copy in a private temporary file for the lifetime of the config entry
- "Allow insecure SSL" disables server-certificate verification for that connection — use it only for self-signed certificates on trusted networks

## Performance, robustness & extra attributes

- **Cache-first calendar view** — the integration keeps a forward window of events (from the start of the current day, for `Days to fetch ahead` days). Calendar panel requests that fall inside that window are served instantly from the cache, with no live Exchange call. Ranges outside it (e.g. past months) are still fetched live, so past browsing keeps working. If a calendar holds more events than `Maximum number of events`, the integration falls back to live queries for it — raise the limit for full cache coverage.
- **No flicker on transient errors** — if a calendar fails to refresh, its last known events are kept. If the server is completely unreachable, the entities become `unavailable` while retaining the last data.
- **Robust event handling** — malformed Exchange events (`end` before `start`, all-day events without an exclusive end, mixed date/datetime bounds, naive timestamps, oversized subjects) are normalized so they no longer break the calendar UI.
- **Extra attributes** — each calendar entity exposes `free_busy_status`, `sensitivity` and `categories` for its current/next event, handy for notification automations.

### Custom User-Agent

Some on-premise Exchange servers block or throttle the default `exchangelib` User-Agent (e.g. a `401 Unauthorized` behind IIS/NTLM). Set a custom string under the integration's **Configure** (Options) menu, e.g. `Microsoft Outlook/16.0 (Android; en-US)`. Note: exchangelib applies the User-Agent process-wide, so it affects every EWS connection of this Home Assistant instance. Leave it empty to use the default.

## Requirements

- Home Assistant 2024.1.0 or later
- Network access to your Exchange server (on-premise) or Office 365
- Python library: `exchangelib` (automatically installed)

## Roadmap

- [x] HACS integration
- [x] On-premise Exchange support (NTLM)
- [x] Office 365 support (OAuth2) via EWS
- [x] Read-only mode option
- [x] Basic EWS authentication (AWS WorkMail)
- [x] Voice assistant (Assist pipeline) support
- [x] **Microsoft Graph API migration for Office 365** — Office 365 now uses Graph API instead of EWS. On-premise (NTLM/Basic) continues to use EWS. See [#3](https://github.com/bohemtucsok/homeassistant-exchange-calendar/issues/3).
- [x] Past events browsing — Calendar view now supports browsing past events
- [x] **Multiple calendar support per account** — Expose your additional mailbox calendars as separate entities. Pick them under the integration's **Configure** (Options) menu; each selected calendar becomes its own `calendar.*` entity.
- [x] **Change credentials without re-adding** — automatic reauth when a password, secret or certificate expires, plus proactive Reconfigure. See [#12](https://github.com/bohemtucsok/homeassistant-exchange-calendar/issues/12).
- [x] **Performance & robustness** — cache-first calendar view, no-flicker refresh, normalization of malformed events, extra event attributes.
- [x] **Certificate-Based Authentication (CBA)** for on-premise Exchange — contributed in [#16](https://github.com/bohemtucsok/homeassistant-exchange-calendar/pull/16); currently in community testing.
- [x] Custom User-Agent option
- [ ] Exchange Tasks as Home Assistant to-do list entities
- [ ] Shared / room calendar support
- [ ] Personal Microsoft account support

## Supporters

<p align="center">
  <a href="https://infotipp.hu"><img src="docs/images/infotipp-logo.png" height="40" alt="Infotipp Rendszerház Kft." /></a>
  &nbsp;&nbsp;&nbsp;&nbsp;
  <a href="https://brutefence.com"><img src="docs/images/brutefence.png" height="40" alt="BruteFence" /></a>
</p>

## License

MIT License - see [LICENSE](LICENSE) for details.

---

*Magyar nyelvű [README_hu.md](README_hu.md) is elérhető.*
