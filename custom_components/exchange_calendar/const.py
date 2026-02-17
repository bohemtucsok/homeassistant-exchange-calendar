"""Constants for the Exchange Calendar integration."""
from datetime import timedelta

DOMAIN = "exchange_calendar"

# Authentication types
AUTH_TYPE_NTLM = "ntlm"
AUTH_TYPE_BASIC = "basic"
AUTH_TYPE_OAUTH2 = "oauth2"

# Configuration keys (stored in config_entry.data)
CONF_AUTH_TYPE = "auth_type"
CONF_SERVER = "server"
CONF_EMAIL = "email"
CONF_USERNAME = "username"
CONF_PASSWORD = "password"
CONF_DOMAIN = "domain"
CONF_CLIENT_ID = "client_id"
CONF_CLIENT_SECRET = "client_secret"
CONF_TENANT_ID = "tenant_id"
CONF_ALLOW_INSECURE_SSL = "allow_insecure_ssl"

# Options keys (stored in config_entry.options)
CONF_DAYS_TO_FETCH = "days_to_fetch"
CONF_MAX_EVENTS = "max_events"
CONF_UPDATE_INTERVAL = "update_interval"
CONF_READ_ONLY = "read_only"

# Defaults (aligned with MMM-Exchange where applicable)
DEFAULT_DAYS_TO_FETCH = 14
DEFAULT_MAX_EVENTS = 50
DEFAULT_ALLOW_INSECURE_SSL = False
DEFAULT_UPDATE_INTERVAL = 5  # minutes
DEFAULT_READ_ONLY = False

# Platforms
PLATFORMS = ["calendar"]

# Minimum update interval
MIN_UPDATE_INTERVAL = timedelta(minutes=1)
