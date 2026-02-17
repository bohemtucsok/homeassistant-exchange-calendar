# Exchange Calendar for Home Assistant

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-41BDF5.svg)](https://github.com/hacs/integration)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Home Assistant custom integration for Microsoft Exchange calendars via EWS (Exchange Web Services).

Supports both **on-premise Exchange** (NTLM) and **Office 365** (OAuth2) with full CRUD operations.

> Based on the [MMM-Exchange](https://github.com/bohemtucsok/MMM-Exchange) MagicMirror module, ported to Python/Home Assistant.

## Features

- **Read** calendar events with automatic recurring event expansion
- **Create** new events from Home Assistant
- **Update** existing events
- **Delete** events
- On-premise Exchange (NTLM authentication)
- Basic EWS authentication (AWS WorkMail and similar)
- Office 365 / Microsoft 365 (OAuth2 authentication)
- Self-signed SSL certificate support
- Configurable polling interval, date range, and event limits
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

### Office 365 (OAuth2)

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
   - Add: `Calendars.ReadWrite`
   - Click **Grant admin consent**

#### Home Assistant Setup

1. Go to **Settings** > **Devices & Services** > **Add Integration**
2. Search for "Exchange Calendar"
3. Select **Office 365 (OAuth2)**
4. Fill in:
   - **Email address**: The mailbox email
   - **Azure AD Tenant ID**: From app registration
   - **Application (Client) ID**: From app registration
   - **Client Secret**: From app registration
5. Configure calendar options

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
| Days to fetch | 14 | How many days ahead to fetch events |
| Max events | 50 | Maximum number of events to display |
| Update interval | 5 min | How often to poll the Exchange server |

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

### No events showing
- Check that the mailbox has calendar events within the configured date range
- Increase "Days to fetch" in options
- Verify the email address matches the mailbox

## Security Considerations

- Always use HTTPS when connecting to your Exchange server
- For on-premise NTLM connections, it is strongly recommended to access Exchange over a trusted internal network or VPN
- Use a dedicated service account with minimal permissions where possible

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
- [ ] **Microsoft Graph API migration for Office 365** — EWS will be [deprecated for Exchange Online in October 2026](https://learn.microsoft.com/en-us/exchange/clients-and-mobile-in-exchange-online/deprecation-of-ews-exchange-online). On-premise (NTLM) is not affected. See [#3](https://github.com/bohemtucsok/homeassistant-exchange-calendar/issues/3).
- [ ] Exchange Tasks as Home Assistant to-do list entities
- [ ] Shared / room calendar support
- [ ] Multiple calendar support per account
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
