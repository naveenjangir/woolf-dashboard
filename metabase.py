"""Metabase API client - read-only SQL execution."""

import requests
import pandas as pd
import streamlit as st

METABASE_URL = st.secrets["METABASE_URL"]
API_KEY      = st.secrets["METABASE_API_KEY"]
DATABASE_ID  = int(st.secrets.get("METABASE_DATABASE_ID", 3))

HEADERS = {
    "x-api-key": API_KEY,
    "Content-Type": "application/json",
}


def run_query(sql: str) -> pd.DataFrame:
    """Execute a read-only SQL query and return a DataFrame."""
    payload = {
        "database": DATABASE_ID,
        "type": "native",
        "native": {"query": sql},
    }
    resp = requests.post(
        f"{METABASE_URL}/api/dataset",
        json=payload,
        headers=HEADERS,
        timeout=120,
    )
    resp.raise_for_status()
    result = resp.json()

    if "error" in result:
        raise ValueError(f"Query error: {result['error']}")

    cols = [col["name"] for col in result["data"]["cols"]]
    rows = result["data"]["rows"]
    return pd.DataFrame(rows, columns=cols)
