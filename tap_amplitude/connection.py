#!/usr/bin/env python3
from __future__ import annotations

import sys

import backoff
import snowflake.connector
import snowflake.connector.network as _sf_network
from snowflake.connector.auth._auth import Auth
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.errorcode import ER_FAILED_TO_REQUEST
from snowflake.connector.errors import Error, OperationalError
from snowflake.connector.network import (
    HEADER_AUTHORIZATION_KEY,
    HEADER_EXTERNAL_SESSION_KEY,
    HTTP_HEADER_USER_AGENT,
    SnowflakeRestful,
)
from snowflake.connector.secret_detector import SecretDetector
from snowflake.connector.telemetry_oob import TelemetryService

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_REDACTED = "****"

_SENSITIVE_HEADERS: frozenset[str] = frozenset([
    HEADER_AUTHORIZATION_KEY,     # "Authorization" — session / bearer token
    HEADER_EXTERNAL_SESSION_KEY,  # "X-Snowflake-External-Session-ID"
    HTTP_HEADER_USER_AGENT,       # "User-Agent" — contains OS / runtime info
])

# ---------------------------------------------------------------------------
# Patch 1 – strip CLIENT_ENVIRONMENT from login request body
# ---------------------------------------------------------------------------

_original_base_auth_data = Auth.base_auth_data  # plain function in Python 3


def _patched_base_auth_data(*args, **kwargs):
    body = _original_base_auth_data(*args, **kwargs)
    body.get("data", {}).pop("CLIENT_ENVIRONMENT", None)
    return body


# ---------------------------------------------------------------------------
# Patch 2 – replace system-identifying User-Agent string in all sync modules
# ---------------------------------------------------------------------------

_SAFE_USER_AGENT = f"PythonConnector/{_sf_network.SNOWFLAKE_CONNECTOR_VERSION}"

_USER_AGENT_MODULE_PATHS = [
    "snowflake.connector.network",
    "snowflake.connector.auth._auth",
    "snowflake.connector.auth.okta",
    "snowflake.connector.auth.webbrowser",
    "snowflake.connector.ocsp_snowflake",
]


def _patch_user_agent() -> None:
    for mod_path in _USER_AGENT_MODULE_PATHS:
        mod = sys.modules.get(mod_path)
        if mod is not None and hasattr(mod, "PYTHON_CONNECTOR_USER_AGENT"):
            setattr(mod, "PYTHON_CONNECTOR_USER_AGENT", _SAFE_USER_AGENT)


# ---------------------------------------------------------------------------
# Patch 3 – redact sensitive headers in _handle_unknown_error log output
# ---------------------------------------------------------------------------

def _redact_headers(headers: dict) -> dict:
    return {k: (_REDACTED if k in _SENSITIVE_HEADERS else v) for k, v in headers.items()}


def _patched_handle_unknown_error(self, method, full_url, headers, data, conn) -> None:
    if data:
        _, masked_data, err_str = SecretDetector.mask_secrets(data)
        if err_str is None:
            data = masked_data

    _sf_network.logger.error(
        "Failed to get the response. Hanging? "
        "method: %s, url: %s, headers: %s, data: %s",
        method, full_url, _redact_headers(headers), data,
    )

    Error.errorhandler_wrapper(
        conn, None, OperationalError,
        {
            "msg": f"Failed to get the response. Hanging? method: {method}, url: {full_url}",
            "errno": ER_FAILED_TO_REQUEST,
        },
    )


# ---------------------------------------------------------------------------
# Patch 4 – suppress exception telemetry (error msgs + stack traces)
# ---------------------------------------------------------------------------

def _noop_send_exception_telemetry(self, connection, telemetry_data) -> None:
    pass


# ---------------------------------------------------------------------------
# Patch 5 – suppress imported-package list telemetry
# ---------------------------------------------------------------------------

def _noop_log_telemetry_imported_packages(self) -> None:
    pass


# ---------------------------------------------------------------------------
# Patch 6 – remove exc_info=True from HTTP-error log line
# ---------------------------------------------------------------------------

def _patched_log_and_handle_http_error_with_cause(
    self, e, full_url, method, retry_timeout, retry_count, conn, timed_out=True
) -> None:
    cause = e.args[0]
    _sf_network.logger.error(cause)  # exc_info removed: no file-path traceback
    if isinstance(cause, Error):
        Error.errorhandler_wrapper_from_cause(conn, cause)
    else:
        self.handle_invalid_certificate_error(conn, full_url, cause)


# ---------------------------------------------------------------------------
# Patch 7 – sanitise raw cause from certificate-error caller-facing message
# ---------------------------------------------------------------------------

def _patched_handle_invalid_certificate_error(self, conn, full_url, cause) -> None:
    Error.errorhandler_wrapper(
        conn, None, OperationalError,
        {
            "msg": "Failed to execute request: certificate or SSL error (details suppressed)",
            "errno": ER_FAILED_TO_REQUEST,
        },
    )


# ---------------------------------------------------------------------------
# Patch 8 – neuter OOB telemetry context (account, host, user, warehouse …)
# ---------------------------------------------------------------------------

def _noop_update_context(self, connection_params) -> None:
    """Prevent connection parameters from being stored as OOB telemetry tags."""
    self.configure_deployment(connection_params)
    self.context = {}


def _noop_get_connection_string(self) -> str:
    """Prevent host:port from appearing in OOB telemetry tags."""
    return ""


# ---------------------------------------------------------------------------
# Apply all patches at import time, before any connection is created
# ---------------------------------------------------------------------------

Auth.base_auth_data = staticmethod(_patched_base_auth_data)             # 1
_patch_user_agent()                                                     # 2
SnowflakeRestful._handle_unknown_error = _patched_handle_unknown_error  # 3
Error.send_exception_telemetry = _noop_send_exception_telemetry         # 4
SnowflakeConnection._log_telemetry_imported_packages = (                # 5
    _noop_log_telemetry_imported_packages
)
SnowflakeRestful.log_and_handle_http_error_with_cause = (               # 6
    _patched_log_and_handle_http_error_with_cause
)
SnowflakeRestful.handle_invalid_certificate_error = (                   # 7
    _patched_handle_invalid_certificate_error
)
TelemetryService.update_context = _noop_update_context                  # 8
TelemetryService.get_connection_string = _noop_get_connection_string    # 8

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


@backoff.on_exception(backoff.expo,
                      (snowflake.connector.Error),
                      max_tries=5,
                      factor=2)
def connect_with_backoff(config):
    return snowflake.connector.connect(
        user=config['username'],
        password=config['password'],
        account=config['account'],
        database=config['database'],
        warehouse=config['warehouse'],
        # Prevent the full sys.modules list from being sent in telemetry even
        # if _log_telemetry_imported_packages is somehow invoked.
        log_imported_packages_in_telemetry=False,
    )
