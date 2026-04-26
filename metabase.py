"""Metabase API client - read-only SQL execution."""

import requests
import pandas as pd

# Secrets are lazy-loaded inside run_query so this module is safe to import
# before Streamlit's runtime context is fully initialised.
_DATABASE_ID_DEFAULT = 3


def run_query(sql: str) -> pd.DataFrame:
    """Execute a read-only SQL query and return a DataFrame."""
    import streamlit as st

    url     = st.secrets["METABASE_URL"]
    api_key = st.secrets["METABASE_API_KEY"]
    db_id   = int(st.secrets.get("METABASE_DATABASE_ID", _DATABASE_ID_DEFAULT))

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
