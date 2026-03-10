"""
Secrets abstraction layer.

When running inside a Streamlit app, reads from st.secrets.
When running as a standalone Python service (monitor_engine), reads from os.environ.

Usage:
    from shared.config import cfg
    api_key = cfg("MARKETDATA_TOKEN")
    service_json = cfg("SERVICE_ACCOUNT_JSON")
"""

from __future__ import annotations
import os
import logging

logger = logging.getLogger(__name__)


def cfg(key: str, default: str = "") -> str:
    """
    Read a secret/config value.

    Priority order:
      1. st.secrets[key]  — when running inside Streamlit (tried on every call,
                            never cached, so background threads can't poison it)
      2. os.environ[key]  — when running as a standalone service
      3. default          — fallback
    """
    # Always try st.secrets fresh — do NOT cache the result.
    # Caching caused background threads (APScheduler) to poison the singleton:
    # if a thread called cfg() before Streamlit was ready, _st=None got locked
    # in and st.secrets was never used again, even inside the UI.
    try:
        import streamlit as st
        val = st.secrets[key]
        if hasattr(val, "to_dict"):
            return dict(val)  # type: ignore[return-value]
        return str(val) if not isinstance(val, dict) else val  # type: ignore[return-value]
    except Exception:
        pass

    return os.environ.get(key, default)


def cfg_dict(key: str) -> dict:
    """
    Read a secret that is a TOML table / JSON dict (e.g. SERVICE_ACCOUNT_JSON).
    Returns a plain dict.
    """
    try:
        import streamlit as st
        val = st.secrets[key]
        if hasattr(val, "to_dict"):
            return dict(val)
        if isinstance(val, dict):
            return val
        import json
        return json.loads(str(val))
    except Exception:
        pass

    raw = os.environ.get(key, "")
    if raw:
        import json
        try:
            return json.loads(raw)
        except Exception:
            logger.warning(f"cfg_dict: could not parse {key} as JSON")
    return {}
