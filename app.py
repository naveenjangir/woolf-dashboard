import streamlit as st

st.title("Woolf Dashboard — Deploy Test")
st.write("✅ Streamlit is running on Streamlit Cloud.")

try:
    url = st.secrets["METABASE_URL"]
    st.success(f"✅ Secrets loaded OK: {url}")
except Exception as e:
    st.error(f"❌ Secret error: {type(e).__name__}: {e}")
