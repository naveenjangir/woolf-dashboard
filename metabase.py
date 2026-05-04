"""Metabase API client - read-only SQL execution."""

import os
import requests
import pandas as pd

# Secrets are lazy-loaded inside run_query so this module is safe to import
# before Streamlit's runtime context is fully initialised.
_DATABASE_ID_DEFAULT = 3


def _get_secret(key: str, default: str | None = None) -> str | None:
    """Read secret from env var (Railway) or st.secrets (local/Streamlit Cloud)."""
    # Environment variable takes priority (Railway, Docker, etc.)
    val = os.environ.get(key)
    if val:
        return val
    # Fallback to st.secrets for local development
    try:
        import streamlit as st
        return st.secrets.get(key, default)
    except Exception:
        return default


def run_query(sql: str) -> pd.DataFrame:
    """Execute a read-only SQL query and return a DataFrame."""
    url     = _get_secret("METABASE_URL")
    api_key = _get_secret("METABASE_API_KEY")
    db_id   = int(_get_secret("METABASE_DATABASE_ID", str(_DATABASE_ID_DEFAULT)))

    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json",
    }
    payload = {
        "database": db_id,
        "type": "native",
        "native": {"query": sql},
    }
    resp = requests.post(
        f"{url}/api/dataset",
        json=payload,
        headers=headers,
        timeout=120,
    )
    resp.raise_for_status()
    result = resp.json()

    if "error" in result:
        raise ValueError(f"Query error: {result['error']}")

    cols = [col["name"] for col in result["data"]["cols"]]
    rows = result["data"]["rows"]
    return pd.DataFrame(rows, columns=cols)
