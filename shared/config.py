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


def _read_toml_secrets_file() -> dict:
    """
    Parse .streamlit/secrets.toml directly and return its contents as a dict.
    Tries tomllib (Python 3.11+) first, then tomli, then falls back to a
    minimal hand-rolled parser that handles the common patterns in this file.
    Result is cached in-process so the file is only read once per run.
    """
    if hasattr(_read_toml_secrets_file, "_cache"):
        return _read_toml_secrets_file._cache  # type: ignore[attr-defined]

    import pathlib
    candidates = [
        pathlib.Path(__file__).parent.parent / ".streamlit" / "secrets.toml",
        pathlib.Path.home() / ".streamlit" / "secrets.toml",
    ]
    toml_path = next((p for p in candidates if p.exists()), None)
    if toml_path is None:
        _read_toml_secrets_file._cache = {}  # type: ignore[attr-defined]
        return {}

    result: dict = {}
    try:
        # Python 3.11+
        import tomllib  # type: ignore[import]
        with open(toml_path, "rb") as f:
            result = tomllib.load(f)
    except ImportError:
        try:
            import tomli  # type: ignore[import]
            with open(toml_path, "rb") as f:
                result = tomli.load(f)
        except ImportError:
            # Minimal fallback: handles KEY = "value", KEY = 'value',
            # and [TABLE] sections that this project actually uses.
            import re as _re
            with open(toml_path) as f:
                raw = f.read()
            current_table: dict | None = None
            current_table_key: str = ""
            for line in raw.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # TOML table header  [section]
                tm = _re.match(r'^\[([^\]]+)\]$', line)
                if tm:
                    current_table_key = tm.group(1).strip()
                    current_table = {}
                    result[current_table_key] = current_table
                    continue
                # KEY = "value" or KEY = 'value'
                km = _re.match(r'^([A-Za-z_]\w*)\s*=\s*"(.*?)"(?:\s*#.*)?$', line) or \
                     _re.match(r"^([A-Za-z_]\w*)\s*=\s*'(.*?)'(?:\s*#.*)?$", line)
                if km:
                    k, v = km.group(1), km.group(2)
                    if current_table is not None:
                        current_table[k] = v
                    else:
                        result[k] = v

    _read_toml_secrets_file._cache = result  # type: ignore[attr-defined]
    return result


def cfg_dict(key: str) -> dict:
    """
    Read a secret that is a TOML table / JSON dict (e.g. SERVICE_ACCOUNT_JSON).
    Returns a plain dict.

    Priority:
      1. st.secrets[key]           — inside Streamlit (TOML table or JSON string)
      2. os.environ[key]           — env var set by start_monitor.sh (JSON string)
      3. .streamlit/secrets.toml   — direct file read (works in monitor process)
    """
    import json

    # 1. Streamlit secrets
    try:
        import streamlit as st
        val = st.secrets[key]
        if hasattr(val, "to_dict"):
            return dict(val)
        if isinstance(val, dict):
            return val
        return json.loads(str(val))
    except Exception:
        pass

    # 2. Environment variable (JSON string)
    raw = os.environ.get(key, "")
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass  # fall through to TOML file read

    # 3. Direct TOML file read — primary path for the monitor process
    toml = _read_toml_secrets_file()
    val = toml.get(key)
    if val is None:
        if raw:
            logger.warning(f"cfg_dict: could not parse {key} as JSON")
        return {}
    if isinstance(val, dict):
        # TOML table — e.g. [SERVICE_ACCOUNT_JSON] with sub-keys
        return val
    if isinstance(val, str):
        # Inline JSON string — e.g. SERVICE_ACCOUNT_JSON = '{"type":...}'
        # Strip surrounding whitespace/quotes that may survive the parser
        s = val.strip()
        if s in ('{"type": "service_account", ...}', ''):
            logger.warning(f"cfg_dict: {key} still contains placeholder value — update secrets.toml")
            return {}
        try:
            return json.loads(s)
        except Exception:
            logger.warning(f"cfg_dict: could not parse {key} from secrets.toml as JSON")
    return {}
