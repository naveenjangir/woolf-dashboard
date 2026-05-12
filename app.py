"""
Woolf Business Dashboard — streamlit run app.py
"""
import base64
import calendar
from datetime import date

import json
from pathlib import Path

import pandas as pd
import streamlit as st
import streamlit.components.v1 as st_components
from PIL import Image

from queries import (
    load_all_colleges, get_college_trend, get_st_trend,
    get_graduation_data, prev_month, year_ago, _april_invoices,
    get_funnel_extras,
)

# ── Asset paths ───────────────────────────────────────────────────────────────
_ASSETS        = Path(__file__).parent / "assets"
_FAVICON_PATH  = _ASSETS / "woolf_favicon.png"       # black mark — used for browser tab
_MARK_PATH     = _ASSETS / "woolf_mark_white.png"    # white mark — for dark backgrounds
_MARK_DARK_PATH= _ASSETS / "woolf_favicon.png"       # black mark — for light header background
_WORDMARK_PATH = _ASSETS / "woolf_wordmark_white.png"

def _img_b64(path: Path) -> str:
    """Read an image file and return a base64 data URI (for inline HTML use)."""
    mime = "image/png" if path.suffix == ".png" else "image/svg+xml"
    return f"data:{mime};base64,{base64.b64encode(path.read_bytes()).decode()}"

# ── External rates (SBS fees, projections) ────────────────────────────────────
_RATES_PATH = Path(__file__).parent / "rates.json"
try:
    RATES = json.loads(_RATES_PATH.read_text())
except Exception:
    RATES = {}
SBS_FEES     = RATES.get("oxford_sbs_fee", {})       # college name → $ per SBS enrolment
PROJECTIONS  = RATES.get("projections", {})

# ── Squad groupings — assign colleges to their squad ─────────────────────────
# Keys = squad display names, values = exact DB college names.
# Colleges not listed in any squad appear under "All Colleges" at the bottom.
SQUAD_MAP: dict[str, list[str]] = {
    "India Squad": [
        "AlmaBetter Innovarsity",
        "Exeed College",
        "Kingsford College of Business and Technology",
        "MSM Grad",
        "UGNXT",
        "Directors' Institute - World Council Of Directors",
        "MATRIX - Maven Academy for Technology, Research, Innovation & eXcellence",
        "NxtWave School of Technology",
        "Scaler Neovarsity",
    ],
    "US Squad": [
        "ALTIS Higher Education",
        "Breathe For Change",
        "Chegg Skills Institute of Applied Learning",
        "Clarke College",
        "Data Science Institute",
        "GoIT Neoversity",
        "Oneday",
    ],
    "Udacity Squad": [
        "Udacity Institute of AI & Technology",
    ],
    "Async Squad": [
        "Global School of Entrepreneurship",
        "Learnbay",
        "Pickering Global Campus",
        "Africa Digital Media Institute",
        "Authstone College",
        "Deep Science Ventures",
        "Digital Scholar",
        "GMC School of Technology",
        "Global Center for Advanced Studies",
        "Inner Institute",
        "Mentogram",
        "Retro Biosciences",
        "Studienzentrum Hohe Warte",
        "The Global Leaders Institute",
        "WeStride Institute of Technology",
    ],
}
# ── Short display names (full DB name → short label used across all pages) ────
SHORT_NAMES: dict[str, str] = {
    "Udacity Institute of AI & Technology":                         "Udacity",
    "AlmaBetter Innovarsity":                                       "AlmaBetter",
    "Pickering Global Campus":                                      "Pickering",
    "Exeed College":                                                "Exeed",
    "UGNXT":                                                        "UGNXT",
    "Chegg Skills Institute of Applied Learning":                   "Chegg Skills",
    "Learnbay":                                                     "Learnbay",
    "GMC School of Technology":                                     "GMC",
    "Global School of Entrepreneurship":                            "GSE",
    "Inner Institute":                                              "Inner Institute",
    "Kingsford College of Business and Technology":                 "Kingsford",
    "MSM Grad":                                                     "MSM Grad",
    "Scaler Neovarsity":                                            "Scaler",
    "Directors' Institute - World Council Of Directors":            "Directors' Institute",
    "Africa Digital Media Institute":                               "ADMI",
    "GoIT Neoversity":                                              "GoIT",
    "Oneday":                                                       "Oneday",
    "Mentogram":                                                    "Mentogram",
    "Global Center for Advanced Studies":                           "GCAS",
    "Data Science Institute":                                       "DSI",
    "Breathe For Change":                                           "B4C",
    "WeStride Institute of Technology":                             "WeStride",
    "Studienzentrum Hohe Warte":                                    "SHW",
    "NxtWave School of Technology":                                 "NxtWave",
    "The Global Leaders Institute":                                 "GLI",
    "ALTIS Higher Education":                                       "ALTIS",
    "Digital Scholar":                                              "Digital Scholar",
    "Deep Science Ventures":                                        "DSV",
    "Clarke College":                                               "Clarke College",
    "Retro Biosciences":                                            "Retro Bioscience",
    "Authstone College":                                            "Authstone",
    "MATRIX - Maven Academy for Technology, Research, Innovation & eXcellence": "MATRIX",
}

def sn(name: str) -> str:
    """Return the short display name for a college, falling back to full name."""
    return SHORT_NAMES.get(name, name)


SQUAD_ICONS: dict[str, str] = {
    "India Squad":   "🇮🇳",
    "US Squad":      "🇺🇸",
    "Udacity Squad": "⚡",
    "Async Squad":   "🌐",
}
# Internal page IDs for squad pages
_SQUAD_PAGE_IDS = {
    "India Squad":   "india_squad",
    "US Squad":      "us_squad",
    "Udacity Squad": "udacity_squad",
    "Async Squad":   "async_squad",
}
_PAGE_ID_TO_SQUAD = {v: k for k, v in _SQUAD_PAGE_IDS.items()}

# ── Page config ───────────────────────────────────────────────────────────────
_favicon_img = Image.open(_FAVICON_PATH) if _FAVICON_PATH.exists() else "🎓"
st.set_page_config(
    page_title="Woolf Business Dashboard",
    page_icon=_favicon_img,
    layout="wide",
    initial_sidebar_state="expanded",
)

PENDING = "—"

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* ── Source Serif 4 + Material Symbols (for sidebar icon buttons) ── */
@import url('https://fonts.googleapis.com/css2?family=Source+Serif+4:ital,opsz,wght@0,8..60,300;0,8..60,400;0,8..60,500;0,8..60,600;1,8..60,400&display=swap');
@import url('https://fonts.googleapis.com/css2?family=Material+Symbols+Rounded:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200&display=swap');

:root {
    --font-serif: 'Source Serif 4', 'Georgia', serif;
}

/* ── Apply serif globally ── */
html, body, .stApp, [data-testid="stAppViewContainer"],
[data-testid="stHeader"],
.stMarkdown, .stCaption,
h1, h2, h3, h4, h5, h6,
p, span, label, div, li,
[data-testid="stMetricLabel"] p,
[data-testid="stMetricValue"] div,
section[data-testid="stSidebar"] {
    font-family: var(--font-serif) !important;
}
/* ── Restore icon fonts overridden by the rule above ── */
/* Streamlit uses Material Symbols for expander chevrons, button icons, etc. */
span.material-symbols-rounded,
span.material-symbols-outlined,
span.material-symbols,
span.material-icons,
span.material-icons-outlined,
[data-testid="stExpander"] summary span:first-child,
[data-baseweb="icon"] span,
button span[class*="material"] {
    font-family: 'Material Symbols Rounded', 'Material Icons', sans-serif !important;
}

/* ── Table data: keep tabular numerics crisp — only numbers / amounts ── */
table.wt td {
    font-family: var(--font-serif) !important;
    font-variant-numeric: tabular-nums;
}
table.wt th {
    font-family: var(--font-serif) !important;
}

/* ── Force light mode regardless of Streamlit theme toggle ── */
html, body, .stApp, [data-testid="stAppViewContainer"], [data-testid="stHeader"] {
    background-color: #f0f3f9 !important;
    color: #1f2937 !important;
}
/* ── Remove Streamlit's blank top bar ── */
header[data-testid="stHeader"] {
    height: 0 !important;
    min-height: 0 !important;
    padding: 0 !important;
    overflow: hidden !important;
}
[data-testid="stToolbar"], [data-testid="stDecoration"] {
    display: none !important;
}
/* ── Tighten top padding now the header bar is gone ── */
section.main .block-container {
    padding-top: 0.8rem; padding-bottom: 2rem;
    background: transparent !important;
}
/* Reset any dark-mode text overrides on standard elements */
p, span, label, div, h1, h2, h3, h4, li { color: inherit; }

/* ── Sidebar — dark navy ── */
section[data-testid="stSidebar"],
section[data-testid="stSidebar"] > div:first-child,
section[data-testid="stSidebar"] > div:first-child > div {
    background: #1b2637 !important;
    background-color: #1b2637 !important;
}
section[data-testid="stSidebar"] h3 {
    color: #f1f5f9 !important; font-size:16px !important; margin-bottom:0 !important; }
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span,
section[data-testid="stSidebar"] small {
    color: #94a3b8 !important; font-size:12.5px !important; }
section[data-testid="stSidebar"] label {
    color: #94a3b8 !important; font-size:12.5px !important; }
section[data-testid="stSidebar"] hr { border-color: #2d3f57 !important; }
section[data-testid="stSidebar"] .stButton > button {
    background: #243447 !important; color: #c0cbd8 !important;
    border: 1px solid #334a64 !important; font-size:12px !important; }
section[data-testid="stSidebar"] .stButton > button:hover {
    background: #2d4060 !important; color: #e2e8f0 !important; }
/* Selectbox / multiselect inside sidebar */
section[data-testid="stSidebar"] [data-baseweb="select"] > div {
    background: #243447 !important; border-color: #334a64 !important; color: #c0cbd8 !important; }
section[data-testid="stSidebar"] .stSelectbox > label,
section[data-testid="stSidebar"] .stMultiSelect > label {
    color: #64748b !important; font-size: 11px !important;
    text-transform: uppercase !important; letter-spacing: 0.4px !important; }
/* Radio nav */
section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label {
    padding: 4px 10px !important; border-radius: 6px !important;
    font-size: 12.5px !important; cursor: pointer; color: #94a3b8 !important; }
section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label:hover {
    background: #243447 !important; }
section[data-testid="stSidebar"] .stRadio input[type="radio"]:checked + div {
    color: #60a5fa !important; }
/* Checkbox */
section[data-testid="stSidebar"] .stCheckbox label {
    color: #94a3b8 !important; }
/* Section nav label */
.nav-section-label {
    font-size: 10px; text-transform: uppercase; letter-spacing: 0.8px;
    color: #4a6080; font-weight: 700; padding: 6px 0 2px; }

/* ── KPI metric cards — force light regardless of theme ── */
[data-testid="metric-container"] {
    background: #ffffff !important;
    background-color: #ffffff !important;
    border: 1px solid #e2e8f0 !important;
    border-radius: 12px !important;
    padding: 14px 18px !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06) !important; }
[data-testid="metric-container"] [data-testid="stMetricLabel"] p,
[data-testid="metric-container"] [data-testid="stMetricLabel"] span {
    color: #6b7280 !important; font-size: 10.5px !important;
    font-weight: 700 !important; text-transform: uppercase !important;
    letter-spacing: 0.5px !important; }
[data-testid="metric-container"] [data-testid="stMetricValue"],
[data-testid="metric-container"] [data-testid="stMetricValue"] div {
    color: #111827 !important; font-size: 26px !important; font-weight: 700 !important; }
[data-testid="metric-container"] [data-testid="stMetricDelta"],
[data-testid="metric-container"] [data-testid="stMetricDelta"] p {
    font-size: 11px !important; }

/* ── Section banners ── */
.sec {
    border-radius: 10px; padding: 10px 18px;
    margin: 8px 0 14px; display: flex; align-items: center; gap: 10px; }
.sec h2  { margin:0; font-size:16px; color:white; display:inline; }
.sec .sub { font-size:11.5px; color:rgba(255,255,255,0.78); margin-left:8px; }
.sec-seat  { background: linear-gradient(135deg,#1e3a8a 0%,#2563eb 100%); }
.sec-revsh { background: linear-gradient(135deg,#4c1d95 0%,#7c3aed 100%); }

/* ── Data table ── */
.wt-wrap {
    background: white; border: 1px solid #e2e8f0; border-radius: 10px;
    overflow: hidden; box-shadow: 0 1px 4px rgba(0,0,0,0.05); margin-bottom: 6px; }
/* Scrollable container — needed for both axes of sticky to work */
.wt-scroll { overflow: auto; max-height: 68vh; }
table.wt { border-collapse:collapse; font-size:12px; table-layout:fixed; width:100%; }
table.wt th {
    background:#f8fafc; padding:5px 5px; text-align:center;
    font-weight:700; color:#4b5563; border-bottom:2px solid #e5e7eb;
    white-space:normal; word-break:break-word; line-height:1.2;
    font-size:9.5px; text-transform:uppercase; letter-spacing:0.3px;
    width:68px; overflow:hidden;
    /* ── Sticky header row ── */
    position: sticky; top: 0; z-index: 2; }
table.wt th:first-child {
    text-align:left; width:148px; white-space:nowrap; word-break:normal;
    /* ── Corner cell: sticky on both axes ── */
    position: sticky; left: 0; top: 0; z-index: 3;
    background: #f8fafc; box-shadow: 2px 0 4px rgba(0,0,0,0.06); }
table.wt td {
    padding:4px 5px; border-bottom:1px solid #f1f5f9; text-align:center;
    color:#374151; white-space:nowrap; font-variant-numeric:tabular-nums;
    width:68px; overflow:hidden; text-overflow:ellipsis; font-size:12px; }
table.wt tr:last-child td { border-bottom:none; }
table.wt tr:hover td    { background:#fafbff; }
table.wt td.cn {
    text-align:left !important; font-weight:500; color:#111827;
    width:148px; max-width:148px; overflow:hidden; text-overflow:ellipsis;
    white-space:nowrap;
    /* ── Sticky first column ── */
    position: sticky; left: 0; z-index: 1;
    background: white; box-shadow: 2px 0 4px rgba(0,0,0,0.04); }
/* Deltas */
table.wt td.dp { color:#16a34a; font-weight:700; }   /* positive ▲ */
table.wt td.dn { color:#dc2626; font-weight:700; }   /* negative ▼ */
table.wt td.dz { color:#9ca3af; }                     /* zero → */
/* Inline negative number (net additions) */
table.wt td.neg { color:#dc2626; font-weight:600; }
/* Pending — compact, unobtrusive */
table.wt td.pd { color:#d1d5db; font-style:normal; font-size:11px; }

.wt-caption { font-size:10.5px; color:#9ca3af; margin:2px 0 14px; }

/* ── College detail ── */
.college-hdr {
    background:white; border:1px solid #e2e8f0; border-radius:12px;
    padding:18px 22px; margin-bottom:18px;
    box-shadow:0 1px 4px rgba(0,0,0,0.05); }
.badge { display:inline-block; padding:3px 10px; border-radius:10px;
         font-size:11px; font-weight:700; }
.badge-seat  { background:#dbeafe; color:#1e40af; }
.badge-revsh { background:#ede9fe; color:#5b21b6; }
.growth-chip {
    display:inline-block; background:#fef9c3; color:#854d0e;
    border:1px solid #fde047; border-radius:8px; padding:4px 10px;
    font-size:12px; font-weight:600; margin:3px; }
</style>
""", unsafe_allow_html=True)

# ── Force sidebar open on every load ─────────────────────────────────────────
# Streamlit persists sidebar state in localStorage and that overrides
# initial_sidebar_state="expanded". Clearing it via JS ensures the sidebar
# is always visible on refresh, while still allowing the user to collapse
# it during a session.
st_components.html("""
<script>
(function() {
    try {
        // Remove all Streamlit sidebar-related localStorage keys
        var toRemove = [];
        for (var i = 0; i < localStorage.length; i++) {
            var k = localStorage.key(i);
            if (k && (k.toLowerCase().includes('sidebar') ||
                      k.toLowerCase().includes('stSidebar'))) {
                toRemove.push(k);
            }
        }
        toRemove.forEach(function(k) { localStorage.removeItem(k); });
    } catch(e) {}
})();
</script>
""", height=0)


# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_int(val):
    """Convert to int, return PENDING if None/NaN."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return PENDING
    return int(val)


def delta(cur, prior):
    """Return trend string with ▲/▼/→ symbol and % change."""
    cur, prior = int(cur or 0), int(prior or 0)
    diff = cur - prior
    if diff == 0:
        return "→ 0"
    sym  = "▲" if diff > 0 else "▼"
    sign = "+" if diff > 0 else ""
    if prior == 0:
        return f"{sym} {sign}{diff}"
    pct = round(diff / prior * 100, 1)
    return f"{sym} {sign}{diff} ({pct}%)"


def delta_detail(cur_mtd, prior_mtd):
    """
    MTD vs MTD comparison cell — shows delta/% on line 1, raw 'X vs Y' on line 2.
    e.g.  ▲ +1 (6.3%)
          17 vs 16
    cell_class() still works since ▲/▼/→ appear in the string.
    """
    cur_i, prior_i = int(cur_mtd or 0), int(prior_mtd or 0)
    diff = cur_i - prior_i
    if diff == 0:
        top = "→ 0"
    else:
        sym  = "▲" if diff > 0 else "▼"
        sign = "+" if diff > 0 else ""
        if prior_i == 0:
            top = f"{sym} {sign}{diff}"
        else:
            pct = round(diff / prior_i * 100, 1)
            top = f"{sym} {sign}{diff} ({pct}%)"
    sub = (f"<br><span style='font-size:10px;font-weight:400;"
           f"color:#9ca3af'>{cur_i} vs {prior_i}</span>")
    return top + sub


def cell_class(val):
    """Determine CSS class for a table cell based on its value."""
    s = str(val)
    if "▲" in s:                           return "dp"
    if "▼" in s:                           return "dn"
    if "→" in s:                           return "dz"
    if val == PENDING:                     return "pd"
    if isinstance(val, int) and val < 0:   return "neg"
    return ""


# Minimum seasonal completion rate for a reliable projection extrapolation.
# If the college's historical rate is below this (e.g. they batch-import at
# month-end so <25 % of enrolments arrive by today's day-of-month on average),
# dividing by such a small number produces nonsense — show PENDING instead.
_MIN_COMPLETION_RATE = 0.25


def fmt_variance(variance: int) -> str:
    """Format projected variance with ▲/▼/→ so cell_class() colours it correctly."""
    if variance > 0:
        return f"▲ +{variance}"
    elif variance < 0:
        return f"▼ {variance}"     # variance is already negative
    return "→ 0"


def calc_variance(cur_enrol: int, proj, comp_rate) -> str:
    """
    Estimate full-month enrolments and compare to projection.

    Normal path (completion_rate ≥ 25%):
      estimated_final = cur_enrol / completion_rate
      variance        = estimated_final − projected

    Fallback path (rate missing or < 25% — e.g. end-of-month batch importers):
      Extrapolation is unreliable, but we still know the MTD position.
      variance = cur_enrol − projected   (minimum variance already locked in)
      This is prefixed with * to signal it's MTD-based, not a full-month estimate.
      e.g. Pickering projected 15, has 47 MTD → shows "▲ +32*" not "—"

    Returns PENDING only if no projection is set for this college/month.
    """
    if proj is None:
        return PENDING
    cr = _fee(comp_rate)
    if cr is None or cr < _MIN_COMPLETION_RATE:
        # Can't extrapolate reliably — show MTD vs projection with * marker
        mtd_var = cur_enrol - int(proj)
        base = fmt_variance(mtd_var)
        return base + "*"
    est_final = round(cur_enrol / cr)
    return fmt_variance(est_final - proj)


def calc_eom_display(cur_enrol: int, proj, comp_rate, avg_full_month=None) -> str:
    """
    Est. EOM column for Enrolment Overview:
    - Shows absolute estimated end-of-month number (no +/-)
    - Green (#16a34a) if est >= projection, red (#dc2626) if below
    - Subtext: 'X% of proj'
    - When completion rate is reliable (≥25%): est = MTD / completion_rate
    - When rate is unreliable (batch-importers with back-loaded enrolments):
        uses historical avg full-month count as estimate (marked *),
        never falls back to MTD alone (which would be misleadingly low)
    - Returns PENDING if no projection set
    """
    if proj is None:
        return PENDING
    proj_i = int(proj)
    cr = _fee(comp_rate)
    if cr is None or cr < _MIN_COMPLETION_RATE:
        # Batch-importer fallback: use historical avg full month if available
        afm = _fee(avg_full_month)
        est  = round(afm) if (afm and afm > cur_enrol) else cur_enrol
        star = "*"
    else:
        est  = round(cur_enrol / cr)
        star = ""
    color = "#16a34a" if est >= proj_i else "#dc2626"
    sub = ""
    if proj_i > 0:
        pct = round(est / proj_i * 100)
        sub = f"<br><span style='font-size:10px;font-weight:400;color:#9ca3af'>{pct}% of proj</span>"
    return f"<span style='color:{color};font-weight:600'>{est}{star}</span>{sub}"


# ── Revenue helpers ───────────────────────────────────────────────────────────

def _fee(val):
    """Return float if val is a valid non-NaN number, else None."""
    if val is None:
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(f) else f


def fmt_usd(amount: float) -> str:
    return f"${int(round(amount)):,}"


def fmt_seat_fee(seat_fee_min, seat_fee_max) -> str:
    """'$N' for single-tier, '$N/$M' for dual-tier (e.g. Exeed)."""
    mn = _fee(seat_fee_min)
    mx = _fee(seat_fee_max)
    if mn is None:
        return PENDING
    return f"${int(mn)}" if mn == mx else f"${int(mn)}/${int(mx)}"


def fmt_enrollment_pct(enrollment_pct) -> str:
    """Revenue-share % — stored as decimal (0.10 → '10.0%')."""
    pct = _fee(enrollment_pct)
    if pct is None:
        return PENDING
    return f"{pct * 100:.1f}%"


# ── Numeric revenue components (return float or None) ─────────────────────────

def _seat_exp_num(seat_exp_rev):
    """Pre-computed per-college expected seat revenue (SUM active × per-degree fee)."""
    v = _fee(seat_exp_rev)
    return v if (v is not None and v > 0) else None


def _revsh_exp_num(exp_rev_usd):
    """Pre-computed per-college expected revenue from queries._revsh_exp_revenue."""
    v = _fee(exp_rev_usd)
    return v if (v is not None and v > 0) else None


def disp_revsh(exp_rev_usd, est_from_min_rev) -> str:
    """
    Format rev-share expected revenue.
    Prepends '~' if some students were estimated from the min-rev-share floor only
    (i.e. no actual tuition data available — e.g. non-USD degrees with no purchase history).
    """
    n = _revsh_exp_num(exp_rev_usd)
    if n is None:
        return PENDING
    approx = int(est_from_min_rev or 0) > 0
    prefix = "~" if approx else ""
    return f"{prefix}{fmt_usd(n)}"


def _st_rev_num(st_converted, airlock_fee):
    """ST→Degree conversions × airlock fee."""
    af = _fee(airlock_fee)
    if af is None:
        return None
    return int(st_converted or 0) * af


def _rpl_pba_num(rpl_low, rpl_high, pba, rpl_admissions, airlock_fee):
    """
    RPL/PBA revenue using Airlock fee:
      rpl_low        × airlock_fee        (< 20 credits → 1× fee)
      rpl_high       × 2 × airlock_fee    (≥ 20 credits → 2× fee)
      pba            × airlock_fee        (PBA admission → 1× fee)
      rpl_admissions × $200 flat          (RPL admission fee)
    """
    af = _fee(airlock_fee)
    if af is None:
        return None
    return (int(rpl_low or 0) * af +
            int(rpl_high or 0) * 2 * af +
            int(pba or 0) * af +
            int(rpl_admissions or 0) * 200)


def _sbs_rev_num(oxford_sbs, college_name):
    """Oxford SBS enrolments × SBS fee from rates.json (varies $400–$750 by college)."""
    sbs_fee = SBS_FEES.get(college_name)
    if sbs_fee is None:
        return None
    return int(oxford_sbs or 0) * sbs_fee


# ── Display wrappers (return formatted string) ────────────────────────────────

def disp(num) -> str:
    """Format a numeric revenue component, or show PENDING."""
    return fmt_usd(num) if num is not None else PENDING


def total_rev(*components) -> str:
    """Sum all non-None revenue components; PENDING only if ALL are unknown."""
    known = [c for c in components if c is not None]
    if not known:
        return PENDING
    return fmt_usd(sum(known))


def html_table(rows: list, cols: list,
               labels: dict | None = None,
               wide_cols: set | None = None,
               narrow_cols: set | None = None,
               total_row: bool = False) -> str:
    """
    Render data as a styled HTML table.

    wide_cols:   set of column keys that should be wider than the default 68px.
                 Used for vs-MTD columns that contain 2-line delta+raw content.
    narrow_cols: set of column keys that should be constrained to 100px.
    """
    lbl = labels or {}
    wc  = set(wide_cols or [])
    nc  = set(narrow_cols or [])

    # Wide/narrow columns get explicit inline width to override table-layout:fixed default
    def th_style(c):
        if c in wc: return ' style="width:108px;min-width:108px"'
        if c in nc: return ' style="width:100px;min-width:100px;max-width:100px"'
        return ""

    def td_style(c):
        # white-space:normal allows the <br> sub-line to wrap properly in wide cells
        if c in wc: return ' style="width:108px;min-width:108px;white-space:normal;line-height:1.35"'
        if c in nc: return ' style="width:100px;min-width:100px;max-width:100px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"'
        return ""

    ths  = "".join(f"<th{th_style(c)}>{lbl.get(c, c)}</th>" for c in cols)
    body = ""
    last_i = len(rows) - 1
    for ri, row in enumerate(rows):
        is_total = total_row and ri == last_i
        tr_style = ' style="background:#f0f4ff;font-weight:700;border-top:2px solid #c7d2fe"' if is_total else ""
        tds = ""
        for i, col in enumerate(cols):
            v   = row.get(col, "")
            if is_total:
                cls = "cn" if i == 0 else ("dp" if "▲" in str(v) else ("dn" if "▼" in str(v) else ""))
            else:
                cls = "cn" if i == 0 else cell_class(v)
            tds += f'<td class="{cls}"{td_style(col)}>{v}</td>'
        body += f"<tr{tr_style}>{tds}</tr>"
    return (
        '<div class="wt-wrap"><div class="wt-scroll">'
        f'<table class="wt"><thead><tr>{ths}</tr></thead>'
        f'<tbody>{body}</tbody></table></div></div>'
    )


# ── Data loaders (cached) ─────────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner="📡 Fetching data from Metabase…")
def get_data(year: int, month: int) -> pd.DataFrame:
    return load_all_colleges(year, month)


@st.cache_data(ttl=3600, show_spinner=False)
def get_trend(college_id: str) -> pd.DataFrame:
    return get_college_trend(college_id, months=14)


@st.cache_data(ttl=3600, show_spinner=False)
def get_st_data(college_id: str) -> pd.DataFrame:
    return get_st_trend(college_id, months=14)


@st.cache_data(ttl=3600, show_spinner=False)
def get_funnel_extras_cached(year: int, month: int) -> dict:
    """Cached wrapper for get_funnel_extras — returns {} on any error."""
    try:
        return get_funnel_extras(year, month)
    except Exception:
        return {}


@st.cache_data(ttl=3600, show_spinner="📡 Loading April 2026 invoice data…")
def get_april_invoices() -> pd.DataFrame:
    """Cached wrapper for _april_invoices() — returns empty DF on any error."""
    try:
        return _april_invoices()
    except Exception:
        return pd.DataFrame(columns=["saas_fee", "monthly_invoice_total"])


# ── Page state (session_state needed for two-radio nav) ───────────────────────
if "_page" not in st.session_state:
    st.session_state["_page"] = "overview"

def _set_page(pid: str):
    st.session_state["_page"] = pid

# ── Sidebar — part 1: wordmark + controls ─────────────────────────────────────
today = date.today()
with st.sidebar:
    # Woolf wordmark (white version — on dark navy sidebar)
    if _WORDMARK_PATH.exists():
        wm_b64 = _img_b64(_WORDMARK_PATH)
        st.markdown(
            f'<div style="padding:12px 10px 4px">'
            f'<img src="{wm_b64}" style="width:130px;opacity:0.92"/>'
            f'<div style="font-size:10px;color:#f1f5f9;letter-spacing:0.8px;'
            f'text-transform:uppercase;margin-top:3px">Business Dashboard</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown("### 🎓 Woolf Dashboard")
    st.divider()
    # ── Date selector — hard min: April 2026 ─────────────────────────────────
    _MIN_YEAR, _MIN_MONTH = 2026, 4   # April 2026 hard minimum

    c1, c2 = st.columns(2)
    with c2:
        _years    = list(range(_MIN_YEAR, today.year + 1))
        sel_year  = st.selectbox("Year", _years, index=_years.index(today.year))

    with c1:
        # Month options depend on selected year:
        # Min year (2026) → start from April; max year (today) → end at today.month
        _month_min = _MIN_MONTH if sel_year == _MIN_YEAR else 1
        _month_max = today.month if sel_year == today.year else 12
        _valid_months = list(range(_month_min, _month_max + 1))
        _default_month = today.month if sel_year == today.year else _month_max
        _default_month = min(_default_month, _month_max)
        sel_month = st.selectbox(
            "Month", _valid_months,
            index=_valid_months.index(_default_month),
            format_func=lambda m: calendar.month_abbr[m])
    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()

py,  pm  = prev_month(sel_year, sel_month)
yy1, my1 = year_ago(sel_year, sel_month)
period    = f"{calendar.month_name[sel_month]} {sel_year}"
is_current_month = (sel_year == today.year and sel_month == today.month)
_last_day_sel = calendar.monthrange(sel_year, sel_month)[1]   # last day of selected month

if is_current_month:
    cur_lbl  = f"{calendar.month_abbr[sel_month]} {sel_year} MTD"   # e.g. "May 2026 MTD"
    vsm1_lbl = f"vs {calendar.month_abbr[pm]} {py} MTD"             # e.g. "vs Apr 2026 MTD"
    vsy1_lbl = f"vs {calendar.month_abbr[my1]} {yy1} MTD"           # e.g. "vs May 2025 MTD"
    active_lbl = "Active (today)"
else:
    cur_lbl  = f"{calendar.month_abbr[sel_month]} {sel_year}"        # e.g. "Apr 2026" (full month)
    vsm1_lbl = f"vs {calendar.month_abbr[pm]} {py} (1–{_last_day_sel})"   # e.g. "vs Mar 2026 (1–30)"
    vsy1_lbl = f"vs {calendar.month_abbr[my1]} {yy1}"                # e.g. "vs Apr 2025"
    active_lbl = f"Active ({calendar.month_abbr[sel_month]} {_last_day_sel})"  # e.g. "Active (Apr 30)"

m1_lbl    = f"{calendar.month_abbr[pm]} {py}"                       # e.g. "Mar 2026" (full month)
y1_lbl    = f"{calendar.month_abbr[my1]} {yy1}"                     # e.g. "Apr 2025" (full month)
proj_lbl  = f"Projected {calendar.month_abbr[sel_month]} {sel_year}"
proj_key  = f"{sel_year}-{sel_month:02d}"
proj_month = PROJECTIONS.get(proj_key, {})

# ── Load main dataset ─────────────────────────────────────────────────────────
df_all = get_data(sel_year, sel_month)

# Always show all columns and both revenue models
show_st = show_rpl = show_oxford = True
model_filter = ["SEAT_BASED", "REVENUE_SHARE"]

def _squad_sorted_colleges(squad_name: str) -> list[str]:
    """Return colleges for a squad sorted by: active seats → MTD enrols →
    ST till → ST this month → alpha. Rev-share colleges have active_base=0
    so they naturally rank by enrolments first."""
    names = SQUAD_MAP.get(squad_name, [])
    sub = df_all[df_all["name"].isin(names)].copy()
    sub["_s1"] = pd.to_numeric(sub["active_base"],        errors="coerce").fillna(0)
    sub["_s2"] = pd.to_numeric(sub["new_enrol"],          errors="coerce").fillna(0)
    sub["_s3"] = pd.to_numeric(sub["st_till_last_month"], errors="coerce").fillna(0)
    sub["_s4"] = pd.to_numeric(sub["st_new_this_month"],  errors="coerce").fillna(0)
    sub = sub.sort_values(["_s1","_s2","_s3","_s4","name"],
                           ascending=[False,False,False,False,True])
    return sub["name"].tolist()

# ── Sidebar — part 2: navigation ─────────────────────────────────────────────
_cur_page = st.session_state["_page"]

# Single flat nav list: Enrolment Overview → Revenue Overview → Overview → 4 squads → all colleges A-Z
_nav_squad_labels  = [f"{SQUAD_ICONS[sq]} {sq}" for sq in SQUAD_MAP]
_nav_college_names = sorted(df_all["name"].tolist())          # full names (used as page IDs)
_nav_college_labels= [sn(n) for n in _nav_college_names]     # short display labels

_nav_options = (
    ["📈 Enrolment Overview", "💰 Revenue Overview", "📊 Overview"]
    + _nav_squad_labels
    + _nav_college_labels   # show short names in sidebar
)

# Map display label → page id  (colleges: short label → full name as page ID)
_nav_id_map = (
    {"📈 Enrolment Overview": "enrolment_overview",
     "💰 Revenue Overview":   "revenue_overview",
     "📊 Overview":           "overview"}
    | {f"{SQUAD_ICONS[sq]} {sq}": _SQUAD_PAGE_IDS[sq] for sq in SQUAD_MAP}
    | {sn(name): name for name in _nav_college_names}   # short label → full page ID
)
# Reverse: page id → display label
_nav_label_map = (
    {pid: lbl for lbl, pid in _nav_id_map.items()}
    | {name: sn(name) for name in _nav_college_names}   # full name → short label
)

_nav_idx = _nav_options.index(_nav_label_map[_cur_page]) if _cur_page in _nav_label_map else 0

def _on_nav_change():
    choice = st.session_state.get("radio_nav")
    if choice is not None:
        _set_page(_nav_id_map[choice])

with st.sidebar:
    st.divider()
    st.markdown('<div class="nav-section-label">Navigate</div>', unsafe_allow_html=True)
    st.radio(
        "_nav", _nav_options, index=_nav_idx,
        key="radio_nav", label_visibility="collapsed",
        on_change=_on_nav_change,
    )
    st.divider()
    st.markdown(
        '<p style="font-size:11px;color:#475569">Source: Woolf Metabase · BigQuery'
        '<br>Read-only · Cached 1 hr</p>',
        unsafe_allow_html=True,
    )

# Resolve legacy `page` variable used by router below
page = _cur_page


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: WORK IN PROGRESS
# ══════════════════════════════════════════════════════════════════════════════

def show_wip(title: str, icon: str, colour: str):
    """Render a Work in Progress placeholder page."""
    if _MARK_DARK_PATH.exists():
        mark_b64 = _img_b64(_MARK_DARK_PATH)
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:14px;margin-bottom:2px">'
            f'<div style="background:{colour};border-radius:10px;width:44px;height:44px;'
            f'display:inline-flex;align-items:center;justify-content:center;'
            f'flex-shrink:0;box-shadow:0 2px 6px rgba(0,0,0,0.18)">'
            f'<img src="{mark_b64}" style="height:26px;width:auto"/>'
            f'</div>'
            f'<div>'
            f'<div style="font-size:22px;font-weight:700;color:#111827;line-height:1.15">'
            f'{icon} {title}</div>'
            f'<div style="font-size:11px;color:#6b7280;letter-spacing:0.3px">Woolf · {period}</div>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    st.divider()
    st.markdown(
        f'<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;'
        f'padding:80px 20px;text-align:center">'
        f'<div style="font-size:64px;margin-bottom:20px">🚧</div>'
        f'<div style="font-size:24px;font-weight:700;color:#111827;margin-bottom:10px">'
        f'Work in Progress</div>'
        f'<div style="font-size:15px;color:#6b7280;max-width:420px;line-height:1.6">'
        f'The <strong>{title}</strong> page is currently being built. '
        f'Check back soon for updates.</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════

def show_overview(college_filter: set | None = None, squad_name: str | None = None):
    """
    Render the overview table.
    college_filter: if set, only show colleges whose name is in this set (squad pages).
    squad_name:     if set, show the squad name/icon as the page header.
    """
    # ── Header ────────────────────────────────────────────────────────────────
    if _MARK_DARK_PATH.exists():
        mark_b64 = _img_b64(_MARK_DARK_PATH)
        if squad_name:
            icon   = SQUAD_ICONS.get(squad_name, "")
            title  = f"{icon} {squad_name}"
            colour = {"India Squad": "#1e3a8a", "US Squad": "#065f46",
                      "Udacity Squad": "#7c2d12", "Async Squad": "#4c1d95"}.get(squad_name, "#1e3a8a")
        else:
            title  = "Business Dashboard"
            colour = "#1e3a8a"
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:14px;margin-bottom:2px">'
            f'<div style="background:{colour};border-radius:10px;width:44px;height:44px;'
            f'display:inline-flex;align-items:center;justify-content:center;'
            f'flex-shrink:0;box-shadow:0 2px 6px rgba(0,0,0,0.18)">'
            f'<img src="{mark_b64}" style="height:26px;width:auto"/>'
            f'</div>'
            f'<div>'
            f'<div style="font-size:22px;font-weight:700;color:#111827;line-height:1.15">'
            f'{title}</div>'
            f'<div style="font-size:11px;color:#6b7280;letter-spacing:0.3px">Woolf · {period}</div>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(f"## {squad_name or 'Woolf Business Dashboard'}")
    st.caption(f"M-1: {m1_lbl}  ·  Y-1: {y1_lbl}  ·  Cached 1 hr")

    df = df_all[df_all["revenue_model"].isin(model_filter)]
    if college_filter:
        df = df[df["name"].isin(college_filter)]

    # ── Enrolment Funnel: ST → ST Converted → Enrolled ───────────────────────
    _funnel_st_new   = int(df["st_new_this_month"].sum())
    _funnel_st_till  = int(df["st_till_last_month"].sum())
    _funnel_conv     = int(df["st_converted_this_month"].sum())
    _funnel_enrol    = int(df["new_enrol"].sum())
    _funnel_conv_rate = (
        f"{round(_funnel_conv / _funnel_st_new * 100, 1)}%" if _funnel_st_new > 0 else "—"
    )
    _funnel_enrol_of_conv = (
        f"{round(_funnel_enrol / _funnel_conv * 100, 1)}%" if _funnel_conv > 0 else "—"
    )
    st.markdown(
        f'<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;'
        f'padding:12px 20px;margin:10px 0 16px;display:flex;align-items:center;'
        f'gap:0;flex-wrap:wrap">'
        f'<div style="text-align:center;padding:4px 20px;border-right:1px solid #e2e8f0">'
        f'<div style="font-size:11px;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:0.5px;color:#6b7280">ST Students (till last month)</div>'
        f'<div style="font-size:22px;font-weight:700;color:#111827">{_funnel_st_till:,}</div>'
        f'</div>'
        f'<div style="text-align:center;padding:4px 20px;border-right:1px solid #e2e8f0">'
        f'<div style="font-size:11px;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:0.5px;color:#6b7280">New ST ({period})</div>'
        f'<div style="font-size:22px;font-weight:700;color:#111827">{_funnel_st_new:,}</div>'
        f'</div>'
        f'<div style="padding:4px 12px;color:#9ca3af;font-size:18px">→</div>'
        f'<div style="text-align:center;padding:4px 20px;border-right:1px solid #e2e8f0">'
        f'<div style="font-size:11px;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:0.5px;color:#7c3aed">ST → Degree ({period})</div>'
        f'<div style="font-size:22px;font-weight:700;color:#7c3aed">{_funnel_conv:,}</div>'
        f'<div style="font-size:10px;color:#9ca3af">{_funnel_conv_rate} of new ST</div>'
        f'</div>'
        f'<div style="padding:4px 12px;color:#9ca3af;font-size:18px">→</div>'
        f'<div style="text-align:center;padding:4px 20px">'
        f'<div style="font-size:11px;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:0.5px;color:#1d4ed8">Enrolled ({period})</div>'
        f'<div style="font-size:22px;font-weight:700;color:#1d4ed8">{_funnel_enrol:,}</div>'
        f'<div style="font-size:10px;color:#9ca3af">{_funnel_enrol_of_conv} of converted</div>'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── SEAT-BASED ────────────────────────────────────────────────────────────
    seat = df[df["revenue_model"] == "SEAT_BASED"]
    if not seat.empty and "SEAT_BASED" in model_filter:
        net_total = int(seat["net_additions"].sum())
        net_sign  = f"+{net_total}" if net_total >= 0 else str(net_total)
        st.markdown(
            f'<div class="sec sec-seat">'
            f'<h2>🪑 Seat-Based</h2>'
            f'<span class="sub">{len(seat)} colleges · '
            f'{int(seat["new_enrol"].sum())} new enrolments · Net {net_sign}</span>'
            f'</div>', unsafe_allow_html=True)

        _pm_short = calendar.month_abbr[pm]
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Colleges",                         len(seat))
        k2.metric("New Enrolments",                   int(seat["new_enrol"].sum()))
        k3.metric(f"Paused ({_pm_short} {py})",       int(seat["pauses_m1"].sum()),
                  help="Students paused in the previous month — not billed this month")
        k4.metric(f"Archived ({_pm_short} {py})",     int(seat["archives_m1"].sum()),
                  help="Students archived in the previous month — not billed this month")
        k5.metric(f"Net ({period})",                   net_total,
                  help="New enrolments minus M-1 exits (seats gained net of prior-month losses)")

        # ── Sort: active seats → enrolments MTD → ST till → ST this month → alpha ──
        seat = seat.copy()
        seat["_s1"] = pd.to_numeric(seat["active_base"],              errors="coerce").fillna(0)
        seat["_s2"] = pd.to_numeric(seat["new_enrol"],                errors="coerce").fillna(0)
        seat["_s3"] = pd.to_numeric(seat["st_till_last_month"],       errors="coerce").fillna(0)
        seat["_s4"] = pd.to_numeric(seat["st_new_this_month"],        errors="coerce").fillna(0)
        seat = (seat.sort_values(["_s1","_s2","_s3","_s4","name"],
                                  ascending=[False,False,False,False,True])
                    .drop(columns=["_s1","_s2","_s3","_s4"]))

        # Column keys (used as dict keys AND html table headers)
        pm_short     = calendar.month_abbr[pm]
        paused_col   = f"Paused ({pm_short} {py})"
        archived_col = f"Archived ({pm_short} {py})"
        net_col      = f"Net ± ({period})"

        cols = ["College", active_lbl, proj_lbl, cur_lbl,
                m1_lbl, y1_lbl, vsm1_lbl, vsy1_lbl, "Est. EOM vs Proj",
                paused_col, archived_col, net_col,
                "Seat Fee", "Seat Rev ($)"]
        if show_st:
            cols += ["ST Till", "ST New", "ST→Deg", "ST Rev ($)"]
        if show_rpl:
            cols += ["RPL <20", "RPL ≥20", "PBA", "RPL/PBA Rev"]
        if show_oxford:
            cols += ["Oxford SBS", "SBS Rev ($)"]
        cols += ["Total Rev ($)"]

        # ── Running totals for the summary row ───────────────────────────────
        _t = dict(active=0, proj=0, cur=0, m1=0, y1=0,
                  m1_mtd=0, y1_mtd=0, eom_est=0,
                  pauses=0, archives=0, net=0,
                  seat_rev=0.0, st_rev=0.0, rpl_rev=0.0, sbs_rev=0.0, total_rev=0.0,
                  eom_rev=0.0,   # seat_rev (full-month) + projected ST/RPL/SBS
                  st_till=0, st_new=0, st_conv=0,
                  rpl_low=0, rpl_high=0, pba=0, oxford=0)

        rows = []
        for _, r in seat.iterrows():
            n_exp  = _seat_exp_num(r.get("seat_exp_rev"))
            n_st   = _st_rev_num(r.get("st_converted_this_month"), r.get("airlock_fee"))   if show_st    else None
            n_rpl  = _rpl_pba_num(r.get("rpl_low"), r.get("rpl_high"), r.get("pba_count"),
                                   0, r.get("airlock_fee"))                                 if show_rpl   else None
            n_sbs  = _sbs_rev_num(r.get("oxford_sbs"), r["name"])                          if show_oxford else None

            proj      = proj_month.get(r["name"])
            cur_enrol = int(r["new_enrol"])
            var_str   = calc_variance(cur_enrol, proj, r.get("completion_rate"))

            row = {
                "College":        sn(r["name"]),
                active_lbl: safe_int(r["active_base"]),
                proj_lbl:         proj if proj is not None else PENDING,
                cur_lbl:          cur_enrol,
                m1_lbl:           int(r["new_enrol_m1"]),
                y1_lbl:           int(r["new_enrol_y1"]),
                vsm1_lbl:         delta_detail(r["new_enrol"], r["new_enrol_m1_mtd"]),
                vsy1_lbl:         delta_detail(r["new_enrol"], r["new_enrol_y1_mtd"]),
                "Est. EOM vs Proj": var_str,
                paused_col:       int(r["pauses_m1"]),
                archived_col:     int(r["archives_m1"]),
                net_col:          int(r["net_additions"]),
                "Seat Fee":       fmt_seat_fee(r.get("seat_fee_min"), r.get("seat_fee_max")),
                "Seat Rev ($)":   disp(n_exp),
            }
            if show_st:
                row.update({
                    "ST Till":    int(r["st_till_last_month"]),
                    "ST New":     int(r["st_new_this_month"]),
                    "ST→Deg":     int(r["st_converted_this_month"]),
                    "ST Rev ($)": disp(n_st),
                })
            if show_rpl:
                row.update({
                    "RPL <20":     int(r["rpl_low"]),
                    "RPL ≥20":     int(r["rpl_high"]),
                    "PBA":         int(r["pba_count"]),
                    "RPL/PBA Rev": disp(n_rpl),
                })
            if show_oxford:
                row.update({
                    "Oxford SBS":  int(r["oxford_sbs"]),
                    "SBS Rev ($)": disp(n_sbs),
                })
            row["Total Rev ($)"] = total_rev(n_exp, n_st, n_rpl, n_sbs)
            rows.append(row)

            # ── Accumulate ───────────────────────────────────────────────────
            ab = _fee(r.get("active_base"))
            _t["active"]   += int(ab)          if ab   is not None else 0
            _t["proj"]     += int(proj)         if proj is not None else 0
            _t["cur"]      += cur_enrol
            _t["m1"]       += int(r["new_enrol_m1"])
            _t["y1"]       += int(r["new_enrol_y1"])
            _t["m1_mtd"]   += int(r["new_enrol_m1_mtd"])
            _t["y1_mtd"]   += int(r["new_enrol_y1_mtd"])
            # EOM enrolment estimate (same logic as calc_variance)
            cr = _fee(r.get("completion_rate"))
            if cr and cr >= _MIN_COMPLETION_RATE:
                _t["eom_est"] += round(cur_enrol / cr)
            else:
                _t["eom_est"] += cur_enrol
            _t["pauses"]   += int(r["pauses_m1"])
            _t["archives"] += int(r["archives_m1"])
            _t["net"]      += int(r["net_additions"])
            if n_exp:   _t["seat_rev"] += n_exp
            if n_st:    _t["st_rev"]   += n_st
            if n_rpl:   _t["rpl_rev"]  += n_rpl
            if n_sbs:   _t["sbs_rev"]  += n_sbs
            n_tot = sum(x for x in [n_exp, n_st, n_rpl, n_sbs] if x is not None)
            _t["total_rev"] += n_tot
            # EOM revenue: project seat count forward using enrollment trend.
            # active_base already includes students enrolled so far this month.
            # Remaining expected enrollments (eom_enrol − cur_enrol) will also
            # become active seats before month-end → add them to the billing count.
            # ST / RPL / SBS: leave as MTD actuals — no reliable intra-month trend.
            ab = _fee(r.get("active_base")) or 0
            if ab > 0 and n_exp:
                avg_fee_per_seat = n_exp / ab
                eom_enrol_college = round(cur_enrol / cr) if (cr and cr >= _MIN_COMPLETION_RATE) else cur_enrol
                additional_seats  = max(0, eom_enrol_college - cur_enrol)
                eom_seat          = (ab + additional_seats) * avg_fee_per_seat
            else:
                eom_seat = n_exp or 0
            _t["eom_rev"] += eom_seat + (n_st or 0) + (n_rpl or 0) + (n_sbs or 0)
            if show_st:
                _t["st_till"] += int(r["st_till_last_month"])
                _t["st_new"]  += int(r["st_new_this_month"])
                _t["st_conv"] += int(r["st_converted_this_month"])
            if show_rpl:
                _t["rpl_low"]  += int(r["rpl_low"])
                _t["rpl_high"] += int(r["rpl_high"])
                _t["pba"]      += int(r["pba_count"])
            if show_oxford:
                _t["oxford"] += int(r["oxford_sbs"])

        # ── Totals row ────────────────────────────────────────────────────────
        # Overall EOM variance: estimated EOM enrolments vs total projected
        _eom_var = fmt_variance(_t["eom_est"] - _t["proj"]) if _t["proj"] else ""
        _tr = {c: "" for c in cols}
        _tr["College"]          = "TOTAL"
        _tr[active_lbl]   = _t["active"]
        _tr[proj_lbl]           = _t["proj"]   if _t["proj"]   else PENDING
        _tr[cur_lbl]            = _t["cur"]
        _tr[m1_lbl]             = _t["m1"]
        _tr[y1_lbl]             = _t["y1"]
        _tr[vsm1_lbl]           = delta_detail(_t["cur"], _t["m1_mtd"])
        _tr[vsy1_lbl]           = delta_detail(_t["cur"], _t["y1_mtd"])
        _tr["Est. EOM vs Proj"] = _eom_var
        _tr[paused_col]         = _t["pauses"]
        _tr[archived_col]       = _t["archives"]
        _tr[net_col]            = _t["net"]
        _tr["Seat Fee"]         = ""
        _tr["Seat Rev ($)"]     = fmt_usd(_t["seat_rev"]) if _t["seat_rev"] else PENDING
        if show_st:
            _tr["ST Till"]    = _t["st_till"]
            _tr["ST New"]     = _t["st_new"]
            _tr["ST→Deg"]     = _t["st_conv"]
            _tr["ST Rev ($)"] = fmt_usd(_t["st_rev"])  if _t["st_rev"]  else PENDING
        if show_rpl:
            _tr["RPL <20"]     = _t["rpl_low"]
            _tr["RPL ≥20"]     = _t["rpl_high"]
            _tr["PBA"]         = _t["pba"]
            _tr["RPL/PBA Rev"] = fmt_usd(_t["rpl_rev"]) if _t["rpl_rev"] else PENDING
        if show_oxford:
            _tr["Oxford SBS"]  = _t["oxford"]
            _tr["SBS Rev ($)"] = fmt_usd(_t["sbs_rev"]) if _t["sbs_rev"] else PENDING
        _tr["Total Rev ($)"]    = fmt_usd(_t["total_rev"]) if _t["total_rev"] else PENDING
        rows.append(_tr)

        st.markdown(html_table(rows, cols,
                               wide_cols={vsm1_lbl, vsy1_lbl},
                               total_row=True),
                    unsafe_allow_html=True)

        # ── EOM / Actual revenue summary ─────────────────────────────────────
        if _t["total_rev"] > 0:
            if is_current_month:
                eom_delta     = _t["eom_rev"] - _t["total_rev"]
                eom_delta_str = (f"+{fmt_usd(eom_delta)}" if eom_delta >= 0
                                 else f"−{fmt_usd(abs(eom_delta))}")
                rev_html = (
                    f'📅 <strong>Est. EOM Revenue (Seat-Based):</strong> '
                    f'<span style="font-size:14px;font-weight:700;color:#1d4ed8">'
                    f'≈ {fmt_usd(_t["eom_rev"])}</span>'
                    f'&nbsp;&nbsp;<span style="color:#6b7280">({eom_delta_str} vs MTD · '
                    f'Seat Rev = (active today + expected new enrolments before month-end) × avg seat fee · '
                    f'ST / RPL / SBS held at MTD actuals)</span>'
                )
            else:
                rev_html = (
                    f'✅ <strong>Actual Revenue (Seat-Based):</strong> '
                    f'<span style="font-size:14px;font-weight:700;color:#1d4ed8">'
                    f'{fmt_usd(_t["total_rev"])}</span>'
                    f'&nbsp;&nbsp;<span style="color:#6b7280">(full month — {period})</span>'
                )
            st.markdown(
                f'<div class="wt-caption" style="margin-top:6px;font-size:12px;color:#374151">'
                f'{rev_html}</div>',
                unsafe_allow_html=True)

        st.markdown(
            '<div class="wt-caption">'
            f'Active (today) = ACTIVE/PENDING/SUBMITTED enrolled since contract-start date · '
            f'{proj_lbl} = manual Q2 forecast · '
            f'{cur_lbl} = {"enrolments so far this month" if is_current_month else "full month enrolments"} (day 1–{today.day if is_current_month else _last_day_sel}) · '
            f'{m1_lbl} / {y1_lbl} = full calendar month totals · '
            f'vs columns = {"MTD comparison (day 1–" + str(today.day) + " in each month)" if is_current_month else "full period comparison (day 1–" + str(_last_day_sel) + ")"}, shows ▲/▼ + raw "X vs Y" · '
            f'Est. EOM vs Proj = estimated full-month (MTD ÷ seasonal rate) minus projected; '
            f'* = completion rate <25% so extrapolation unreliable — shows MTD vs projected instead (minimum variance already locked in) · '
            f'Paused/Archived = {pm_short} {py} exits (not billed this month) · '
            'Seat Rev = Σ(active × per-degree seat fee) · ST Rev = ST→Deg × Airlock fee · '
            'RPL/PBA: <20 cr = 1× airlock, ≥20 cr = 2× airlock, PBA = 1× airlock, RPL Adm = $200 flat'
            '</div>',
            unsafe_allow_html=True)

    # ── REVENUE-SHARE ─────────────────────────────────────────────────────────
    revsh = df[df["revenue_model"] == "REVENUE_SHARE"]
    if not revsh.empty and "REVENUE_SHARE" in model_filter:
        net_revsh_total = int(revsh["net_revsh"].sum())
        net_sign = f"+{net_revsh_total}" if net_revsh_total >= 0 else str(net_revsh_total)
        st.markdown(
            f'<div class="sec sec-revsh">'
            f'<h2>📊 Revenue-Share</h2>'
            f'<span class="sub">{len(revsh)} colleges · '
            f'{int(revsh["new_enrol"].sum())} new enrolments · Net {net_sign}</span>'
            f'</div>', unsafe_allow_html=True)

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Colleges",          len(revsh))
        k2.metric("New Enrolments",    int(revsh["new_enrol"].sum()))
        k3.metric("Archived ≤30d",     int(revsh["archived_30d"].sum()),
                  help="Students enrolled within last 30 days who are now archived (refund window)")
        k4.metric(f"Net ({period})",   net_revsh_total,
                  help="New enrolments minus 30-day refund archives")

        # ── Sort: enrolments MTD → ST till → ST this month → alpha ──
        revsh = revsh.copy()
        revsh["_s1"] = pd.to_numeric(revsh["new_enrol"],          errors="coerce").fillna(0)
        revsh["_s2"] = pd.to_numeric(revsh["st_till_last_month"], errors="coerce").fillna(0)
        revsh["_s3"] = pd.to_numeric(revsh["st_new_this_month"],  errors="coerce").fillna(0)
        revsh = (revsh.sort_values(["_s1","_s2","_s3","name"],
                                    ascending=[False,False,False,True])
                      .drop(columns=["_s1","_s2","_s3"]))

        net_col  = f"Net ({period})"

        cols = ["College", proj_lbl, cur_lbl, m1_lbl, y1_lbl,
                vsm1_lbl, vsy1_lbl, "Est. EOM vs Proj", "Arch ≤30d", net_col,
                "Rev Share %", "Exp Rev ($)"]
        if show_st:
            cols += ["ST Till", "ST New", "ST→Deg", "ST Rev ($)"]
        if show_rpl:
            cols += ["RPL <20", "RPL ≥20", "PBA", "RPL/PBA Rev"]
        if show_oxford:
            cols += ["Oxford SBS", "SBS Rev ($)"]
        cols += ["Total Rev ($)"]

        rows = []
        _rt = dict(proj=0, cur=0, m1=0, y1=0, m1_mtd=0, y1_mtd=0,
                   eom_enrol=0, arch_30d=0, net=0,
                   exp_rev=0.0, st_rev=0.0, rpl_rev=0.0, sbs_rev=0.0, total_rev=0.0,
                   st_till=0, st_new=0, st_conv=0,
                   rpl_low=0, rpl_high=0, pba=0, oxford=0,
                   eom_rev=0.0)
        for _, r in revsh.iterrows():
            n_exp  = _revsh_exp_num(r.get("exp_rev_usd"))
            n_st   = _st_rev_num(r.get("st_converted_this_month"), r.get("airlock_fee"))  if show_st    else None
            n_rpl  = _rpl_pba_num(r.get("rpl_low"), r.get("rpl_high"), r.get("pba_count"),
                                   0, r.get("airlock_fee"))                                if show_rpl   else None
            n_sbs  = _sbs_rev_num(r.get("oxford_sbs"), r["name"])                         if show_oxford else None

            # ── Projection + variance ─────────────────────────────────────────
            proj      = proj_month.get(r["name"])
            cur_enrol = int(r["new_enrol"])
            var_str   = calc_variance(cur_enrol, proj, r.get("completion_rate"))

            row = {
                "College":    sn(r["name"]),
                proj_lbl:     proj if proj is not None else PENDING,
                cur_lbl:      cur_enrol,
                m1_lbl:       int(r["new_enrol_m1"]),
                y1_lbl:       int(r["new_enrol_y1"]),
                vsm1_lbl:     delta_detail(r["new_enrol"], r["new_enrol_m1_mtd"]),
                vsy1_lbl:     delta_detail(r["new_enrol"], r["new_enrol_y1_mtd"]),
                "Est. EOM vs Proj":   var_str,
                "Arch ≤30d":  int(r["archived_30d"]),
                net_col:      int(r["net_revsh"]),
                "Rev Share %": fmt_enrollment_pct(r.get("enrollment_pct")),
                "Exp Rev ($)": disp_revsh(r.get("exp_rev_usd"), r.get("est_from_min_rev")),
            }
            if show_st:
                row.update({
                    "ST Till":    int(r["st_till_last_month"]),
                    "ST New":     int(r["st_new_this_month"]),
                    "ST→Deg":     int(r["st_converted_this_month"]),
                    "ST Rev ($)": disp(n_st),
                })
            if show_rpl:
                row.update({
                    "RPL <20":     int(r["rpl_low"]),
                    "RPL ≥20":     int(r["rpl_high"]),
                    "PBA":         int(r["pba_count"]),
                    "RPL/PBA Rev": disp(n_rpl),
                })
            if show_oxford:
                row.update({
                    "Oxford SBS":  int(r["oxford_sbs"]),
                    "SBS Rev ($)": disp(n_sbs),
                })
            n_total = sum(x for x in [n_exp, n_st, n_rpl, n_sbs] if x is not None) or None
            row["Total Rev ($)"] = total_rev(n_exp, n_st, n_rpl, n_sbs)
            rows.append(row)

            # ── Accumulate totals ─────────────────────────────────────────────
            if proj is not None:
                _rt["proj"]    += int(proj)
            _rt["cur"]         += cur_enrol
            _rt["m1"]          += int(r["new_enrol_m1"])
            _rt["y1"]          += int(r["new_enrol_y1"])
            _rt["m1_mtd"]      += int(r["new_enrol_m1_mtd"])
            _rt["y1_mtd"]      += int(r["new_enrol_y1_mtd"])
            _rt["arch_30d"]    += int(r["archived_30d"])
            _rt["net"]         += int(r["net_revsh"])
            if n_exp:           _rt["exp_rev"]   += n_exp
            if n_st:            _rt["st_rev"]    += n_st
            if n_rpl:           _rt["rpl_rev"]   += n_rpl
            if n_sbs:           _rt["sbs_rev"]   += n_sbs
            if n_total:         _rt["total_rev"] += n_total
            if show_st:
                _rt["st_till"] += int(r["st_till_last_month"])
                _rt["st_new"]  += int(r["st_new_this_month"])
                _rt["st_conv"] += int(r["st_converted_this_month"])
            if show_rpl:
                _rt["rpl_low"]  += int(r["rpl_low"])
                _rt["rpl_high"] += int(r["rpl_high"])
                _rt["pba"]      += int(r["pba_count"])
            if show_oxford:
                _rt["oxford"]  += int(r["oxford_sbs"])
            # EOM enrolment estimate
            cr = _fee(r.get("completion_rate"))
            if cr and cr >= _MIN_COMPLETION_RATE:
                _rt["eom_enrol"] += round(cur_enrol / cr)
                if n_total:
                    _rt["eom_rev"] += n_total / cr
            else:
                _rt["eom_enrol"] += cur_enrol
                if n_total:
                    _rt["eom_rev"] += n_total   # floor: MTD as minimum

        # ── Totals row ────────────────────────────────────────────────────────
        _eom_var_revsh = fmt_variance(_rt["eom_enrol"] - _rt["proj"]) if _rt["proj"] else ""
        _revsh_total_row = {c: "" for c in cols}
        _revsh_total_row["College"]           = "TOTAL"
        _revsh_total_row[proj_lbl]            = _rt["proj"] if _rt["proj"] else PENDING
        _revsh_total_row[cur_lbl]             = _rt["cur"]
        _revsh_total_row[m1_lbl]              = _rt["m1"]
        _revsh_total_row[y1_lbl]              = _rt["y1"]
        _revsh_total_row[vsm1_lbl]            = delta_detail(_rt["cur"], _rt["m1_mtd"])
        _revsh_total_row[vsy1_lbl]            = delta_detail(_rt["cur"], _rt["y1_mtd"])
        _revsh_total_row["Est. EOM vs Proj"]  = _eom_var_revsh
        _revsh_total_row["Arch ≤30d"]         = _rt["arch_30d"]
        _revsh_total_row[net_col]             = _rt["net"]
        _revsh_total_row["Rev Share %"]       = ""
        _revsh_total_row["Exp Rev ($)"]       = fmt_usd(_rt["exp_rev"]) if _rt["exp_rev"] else PENDING
        if show_st:
            _revsh_total_row["ST Till"]    = _rt["st_till"]
            _revsh_total_row["ST New"]     = _rt["st_new"]
            _revsh_total_row["ST→Deg"]     = _rt["st_conv"]
            _revsh_total_row["ST Rev ($)"] = fmt_usd(_rt["st_rev"]) if _rt["st_rev"] else PENDING
        if show_rpl:
            _revsh_total_row["RPL <20"]     = _rt["rpl_low"]
            _revsh_total_row["RPL ≥20"]     = _rt["rpl_high"]
            _revsh_total_row["PBA"]         = _rt["pba"]
            _revsh_total_row["RPL/PBA Rev"] = fmt_usd(_rt["rpl_rev"]) if _rt["rpl_rev"] else PENDING
        if show_oxford:
            _revsh_total_row["Oxford SBS"]  = _rt["oxford"]
            _revsh_total_row["SBS Rev ($)"] = fmt_usd(_rt["sbs_rev"]) if _rt["sbs_rev"] else PENDING
        _revsh_total_row["Total Rev ($)"]   = fmt_usd(_rt["total_rev"]) if _rt["total_rev"] else PENDING
        rows.append(_revsh_total_row)

        st.markdown(html_table(rows, cols,
                               wide_cols={vsm1_lbl, vsy1_lbl},
                               total_row=True),
                    unsafe_allow_html=True)

        # ── EOM / Actual revenue summary ─────────────────────────────────────
        if _rt["total_rev"] > 0:
            if is_current_month:
                eom_delta     = _rt["eom_rev"] - _rt["total_rev"]
                eom_delta_str = (f"+{fmt_usd(eom_delta)}" if eom_delta >= 0
                                 else f"−{fmt_usd(abs(eom_delta))}")
                rev_html = (
                    f'📅 <strong>Est. EOM Revenue (Rev-Share):</strong> '
                    f'<span style="font-size:14px;font-weight:700;color:#1d4ed8">'
                    f'≈ {fmt_usd(_rt["eom_rev"])}</span>'
                    f'&nbsp;&nbsp;<span style="color:#6b7280">({eom_delta_str} vs MTD · '
                    f'projected from MTD ÷ seasonal completion rate)</span>'
                )
            else:
                rev_html = (
                    f'✅ <strong>Actual Revenue (Rev-Share):</strong> '
                    f'<span style="font-size:14px;font-weight:700;color:#1d4ed8">'
                    f'{fmt_usd(_rt["total_rev"])}</span>'
                    f'&nbsp;&nbsp;<span style="color:#6b7280">(full month — {period})</span>'
                )
            st.markdown(
                f'<div class="wt-caption" style="margin-top:6px;font-size:12px;color:#374151">'
                f'{rev_html}</div>',
                unsafe_allow_html=True)

        st.markdown(
            '<div class="wt-caption">'
            f'{proj_lbl} = manual Q2 forecast · '
            f'{cur_lbl} = {"enrolments so far this month" if is_current_month else "full month enrolments"} (day 1–{today.day if is_current_month else _last_day_sel}) · '
            f'{m1_lbl} / {y1_lbl} = full calendar month totals · '
            f'vs columns = {"MTD comparison (day 1–" + str(today.day) + " in each month)" if is_current_month else "full period comparison (day 1–" + str(_last_day_sel) + ")"}, shows ▲/▼ + raw "X vs Y" · '
            'Est. EOM vs Proj = estimated full-month (MTD ÷ seasonal rate) minus projected; '
            '* = MTD vs projected (completion rate <25%, extrapolation unreliable — shows minimum variance locked in) · '
            'Arch ≤30d = enrolled in last 30 days, now archived (refund window) · '
            'Exp Rev = Σ MAX(Rev Share% × tuition, Min Rev Share) per new enrolment (MTD); '
            '~ prefix = some students had no tuition data, estimated from min floor · '
            'SBS Rev = Oxford SBS enrolments × college SBS fee (from rates.json) · '
            'RPL/PBA: <20 cr = 1× airlock, ≥20 cr = 2× airlock, PBA = 1× airlock, RPL Adm = $200 flat'
            '</div>',
            unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: COLLEGE DETAIL
# ══════════════════════════════════════════════════════════════════════════════

def show_college_detail(college_name: str):
    college_row = df_all[df_all["name"] == college_name]
    if college_row.empty:
        st.error(f"College '{college_name}' not found in loaded data.")
        return

    r          = college_row.iloc[0]
    model      = r["revenue_model"]
    college_id = r["college_id"]
    is_seat    = (model == "SEAT_BASED")
    badge_cls  = "badge-seat" if is_seat else "badge-revsh"
    badge_lbl  = "Seat-Based" if is_seat else "Revenue Share"

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(f"""
    <div class="college-hdr">
      <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">
        <span style="font-size:22px;font-weight:700;color:#111827">{college_name}</span>
        <span class="badge {badge_cls}">{badge_lbl}</span>
      </div>
      <div style="margin-top:6px;color:#6b7280;font-size:13px">
        📅 Viewing: <strong>{period}</strong>
        &nbsp;·&nbsp; M-1: {m1_lbl} &nbsp;·&nbsp; Y-1: {y1_lbl}
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Quick-stats row ───────────────────────────────────────────────────────
    enrol_delta  = int(r["new_enrol"]) - int(r["new_enrol_m1"])
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("New Enrolments", int(r["new_enrol"]),
              delta=f"{'+' if enrol_delta >= 0 else ''}{enrol_delta} vs M-1")
    k2.metric("Paused",          int(r["pauses"]))
    k3.metric("Archived",        int(r["archives"]))
    k4.metric("Net Additions",   int(r["net_additions"]))
    if is_seat:
        k5.metric("Active Base", safe_int(r["active_base"]))
    else:
        k5.metric("ST Conversions", int(r["st_converted_this_month"]))

    st.divider()

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab1, tab2, tab3 = st.tabs([
        "📈 Enrolment Trend",
        "📚 Study Track",
        "🚀 Growth Levers",
    ])

    # ── Tab 1: Enrolment Trend ────────────────────────────────────────────────
    with tab1:
        with st.spinner("Loading enrolment trend…"):
            trend_df = get_trend(college_id)

        if trend_df.empty:
            st.info("No enrolment data found for this college in the last 14 months.")
        else:
            trend_df["net"] = (
                trend_df["new_enrol"] - trend_df["pauses"] - trend_df["archives"]
            )
            trend_idx = trend_df.set_index("month")

            st.markdown(f"##### Monthly Enrolments — last 14 months")
            st.bar_chart(trend_idx[["new_enrol"]], color="#2563eb")

            with st.expander("📊 Pauses & Archives"):
                st.line_chart(trend_idx[["pauses", "archives"]],
                              color=["#f59e0b", "#ef4444"])

            with st.expander("📋 Raw data"):
                st.dataframe(
                    trend_df.rename(columns={
                        "month": "Month", "new_enrol": "New",
                        "pauses": "Paused", "archives": "Archived", "net": "Net ±",
                    }),
                    hide_index=True, use_container_width=True,
                )

    # ── Tab 2: Study Track ────────────────────────────────────────────────────
    with tab2:
        with st.spinner("Loading study track trend…"):
            st_df = get_st_data(college_id)

        if st_df.empty:
            st.info("No study track data found for this college in the last 14 months.")
        else:
            st.markdown("##### New Study-Track Students — last 14 months")
            st.bar_chart(st_df.set_index("month")[["new_st"]],
                         color="#7c3aed")
            st.caption(
                "New study-track enrolments per month (source: st_students table — "
                "same as the ST New KPI above)"
            )

            # Quick conversion stats for current month
            this_st_new  = int(r["st_new_this_month"])
            this_st_conv = int(r["st_converted_this_month"])
            this_st_till = int(r["st_till_last_month"])
            conv_rate    = (
                f"{round(this_st_conv / this_st_new * 100, 1)}%"
                if this_st_new > 0 else "—"
            )
            st.markdown("")
            c1, c2, c3 = st.columns(3)
            c1.metric("ST till last month", this_st_till)
            c2.metric(f"New ST ({period})", this_st_new)
            c3.metric(f"ST → Degree ({period})", this_st_conv,
                      delta=f"{conv_rate} conversion rate")

    # ── Tab 3: Growth Levers ─────────────────────────────────────────────────
    with tab3:
        st.markdown("### 🚀 Growth Levers")

        new_enrol  = int(r["new_enrol"])
        st_new     = int(r["st_new_this_month"])
        st_conv    = int(r["st_converted_this_month"])
        st_till    = int(r["st_till_last_month"])
        rpl_low    = int(r["rpl_low"])
        rpl_high   = int(r["rpl_high"])
        pba        = int(r["pba_count"])
        oxford     = int(r["oxford_sbs"])

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### 📚 Study Track → Degree")
            conv_rate = (
                f"{round(st_conv / st_new * 100, 1)}%"
                if st_new > 0 else "—"
            )
            st.markdown(f"""
| Metric | Value |
|--------|-------|
| ST students (till last month) | **{st_till}** |
| New ST this month | **{st_new}** |
| ST → Degree conversions | **{st_conv}** |
| Conversion rate (this month) | **{conv_rate}** |
""")
            if st_new > 0 and st_conv < st_new:
                gap = st_new - st_conv
                st.warning(f"⚡ {gap} study-track students this month not yet converted.")

        with col2:
            st.markdown("#### 🏅 RPL, PBA & Oxford SBS")
            st.markdown(f"""
| Metric | Value |
|--------|-------|
| RPL exemptions (<20 credits) | **{rpl_low}** |
| RPL exemptions (≥20 credits) | **{rpl_high}** |
| PBA admissions | **{pba}** |
| Oxford SBS enrolments | **{oxford}** |
""")

        st.divider()

        # Opportunity chips
        st.markdown("#### 💡 Untapped Opportunities (this month)")
        opps = []
        if st_till > 0 and st_conv == 0:
            opps.append(f"🎯 {st_till} ST students have never converted to degree")
        if rpl_low == 0 and rpl_high == 0:
            opps.append("📋 No RPL exemptions processed — promote RPL pathways")
        if pba == 0:
            opps.append("🔄 No PBA admissions this month")
        if oxford == 0 and is_seat:
            opps.append("🎓 No Oxford SBS enrolments — check course promotion")
        if new_enrol == 0:
            opps.append("📉 Zero new enrolments — check pipeline health")

        if opps:
            chips = "".join(
                f'<span class="growth-chip">{o}</span>' for o in opps
            )
            st.markdown(chips, unsafe_allow_html=True)
        else:
            st.success("✅ All key growth levers are active this month!")

        if not is_seat:
            st.caption(
                "Note: Growth levers are most actionable for seat-based colleges. "
                "Revenue-share colleges focus primarily on new enrolments."
            )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: REVENUE OVERVIEW  (testing — hard-coded April 2026)
# ══════════════════════════════════════════════════════════════════════════════

def show_revenue_overview():
    """
    Revenue Overview — testing page, hard-coded to April 2026.
    Revenue figures sourced directly from the invoices system.
    Month filter in the sidebar does NOT apply here.
    """
    _REV_COLOUR = "#7c3aed"

    # ── Status badge HTML helper ───────────────────────────────────────────────
    _STATUS_STYLE = {
        "PAID":  ("background:#dcfce7;color:#166534;border:1px solid #86efac",  "✓ Paid"),
        "OPEN":  ("background:#dbeafe;color:#1e40af;border:1px solid #93c5fd",  "● Open"),
        "DRAFT": ("background:#fef9c3;color:#854d0e;border:1px solid #fde047",  "✎ Draft"),
    }
    def _status_badge(status: str) -> str:
        if not status:
            return ""
        style, label = _STATUS_STYLE.get(
            str(status).upper(),
            ("background:#f3f4f6;color:#6b7280;border:1px solid #d1d5db", str(status))
        )
        return (f'<span style="{style};border-radius:6px;padding:1px 6px;'
                f'font-size:9px;font-weight:700;white-space:nowrap">{label}</span>')

    def _cell(amount: float | None, status: str | None) -> str:
        """Amount on line 1, small status badge on line 2. Shows — if no amount."""
        if amount is None or amount == 0:
            return PENDING
        badge = _status_badge(status)
        badge_html = (f"<br><span style='font-size:9px;font-weight:400'>{badge}</span>"
                      if badge else "")
        return f"{fmt_usd(amount)}{badge_html}"

    # ── Header ─────────────────────────────────────────────────────────────────
    if _MARK_DARK_PATH.exists():
        mark_b64 = _img_b64(_MARK_DARK_PATH)
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:14px;margin-bottom:2px">'
            f'<div style="background:{_REV_COLOUR};border-radius:10px;width:44px;height:44px;'
            f'display:inline-flex;align-items:center;justify-content:center;'
            f'flex-shrink:0;box-shadow:0 2px 6px rgba(0,0,0,0.18)">'
            f'<img src="{mark_b64}" style="height:26px;width:auto"/>'
            f'</div>'
            f'<div>'
            f'<div style="font-size:22px;font-weight:700;color:#111827;line-height:1.15">'
            f'💰 Revenue Overview</div>'
            f'<div style="font-size:11px;color:#6b7280;letter-spacing:0.3px">'
            f'Woolf · April 2026 (testing)</div>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    st.divider()

    # ── Disclaimer banner ─────────────────────────────────────────────────────
    st.markdown(
        '<div style="background:#fef3c7;border:1px solid #fde68a;border-radius:10px;'
        'padding:12px 18px;margin-bottom:18px;display:flex;align-items:flex-start;gap:12px">'
        '<div style="font-size:22px;flex-shrink:0">⚠️</div>'
        '<div>'
        '<div style="font-size:13px;font-weight:700;color:#92400e;margin-bottom:4px">'
        'Testing Page — Work in Progress · April 2026 only</div>'
        '<div style="font-size:12px;color:#78350f;line-height:1.6">'
        'All revenue figures are pulled <strong>directly from the invoicing system</strong> '
        '— no estimates. The month filter does not apply to this page.<br>'
        '<strong>Draft</strong> invoices = system-generated, not yet reconciled (typically first week of month). '
        '<strong>Open</strong> = reconciled, awaiting payment. '
        '<strong>Paid</strong> = closed. '
        'SAAS Fee shows $0 where Q2 quarterly SAAS line not yet created. '
        'Columns show — where no invoice exists for a college.</div>'
        '</div>'
        '</div>',
        unsafe_allow_html=True,
    )

    # ── Load data ─────────────────────────────────────────────────────────────
    with st.spinner("📡 Loading April 2026 invoice data…"):
        df_colleges = get_data(2026, 4)   # for college name + revenue_model lookup
        df_inv      = get_april_invoices() # invoice-sourced breakdown

    # Merge on college_id so we know each college's revenue_model and name
    df_inv_r = df_inv.reset_index()   # college_id becomes a column
    df = df_colleges[["college_id","name","revenue_model"]].merge(
        df_inv_r, on="college_id", how="left"
    )

    def _rev_table(df_sub, section_title, section_css, fee_col_label):
        if df_sub.empty:
            return

        count       = len(df_sub)
        has_invoice = df_sub["invoice_name"].notna().sum()
        st.markdown(
            f'<div class="sec {section_css}">'
            f'<h2>{section_title}</h2>'
            f'<span class="sub">{count} colleges · '
            f'{has_invoice} with April invoices</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # Status for each revenue head (quarterly covers SAAS+Seat; monthly covers Growth+Add)
        # wide_cols so the two-line amount+badge fits without truncation
        COLS = ["College",
                "SAAS Fee ($)", fee_col_label,
                "Growth ($)", "Add. Items ($)",
                "Monthly Inv. ($)", "Total CV ($)"]
        WIDE = {"SAAS Fee ($)", fee_col_label, "Growth ($)",
                "Add. Items ($)", "Monthly Inv. ($)", "Total CV ($)"}

        rows = []
        tot  = dict(saas=0.0, fee=0.0, growth=0.0, add=0.0, inv=0.0, cv=0.0)

        for _, r in df_sub.sort_values("name").iterrows():
            has_inv   = pd.notna(r.get("invoice_name"))
            q_status  = r.get("quarterly_status")  if has_inv else None
            m_status  = r.get("invoice_status")    if has_inv else None
            n_saas    = _fee(r.get("saas_fee"))              if has_inv else None
            n_fee     = _fee(r.get("seat_fee"))              if has_inv else None
            n_gr      = _fee(r.get("growth"))                if has_inv else None
            n_add     = _fee(r.get("additional_items"))      if has_inv else None
            n_inv     = _fee(r.get("monthly_invoice_total")) if has_inv else None
            n_cv      = _fee(r.get("total_cv"))              if has_inv else None

            row = {
                "College":          sn(r["name"]),
                # Each revenue head: amount + its source invoice's status badge
                "SAAS Fee ($)":     _cell(n_saas, q_status) if has_inv else PENDING,
                fee_col_label:      _cell(n_fee,  q_status) if has_inv else PENDING,
                "Growth ($)":       _cell(n_gr,   m_status) if has_inv else PENDING,
                "Add. Items ($)":   _cell(n_add,  m_status) if has_inv else PENDING,
                "Monthly Inv. ($)": _cell(n_inv,  m_status) if has_inv else PENDING,
                "Total CV ($)":     _cell(n_cv,   None)     if has_inv else PENDING,
            }
            rows.append(row)

            if n_saas: tot["saas"]   += n_saas
            if n_fee:  tot["fee"]    += n_fee
            if n_gr:   tot["growth"] += n_gr
            if n_add:  tot["add"]    += n_add
            if n_inv:  tot["inv"]    += n_inv
            if n_cv:   tot["cv"]     += n_cv

        # Totals row — no status badge on totals, just amounts
        tr = {c: "" for c in COLS}
        tr["College"]         = "TOTAL"
        tr["SAAS Fee ($)"]    = fmt_usd(tot["saas"])   if tot["saas"]   else PENDING
        tr[fee_col_label]      = fmt_usd(tot["fee"])    if tot["fee"]    else PENDING
        tr["Growth ($)"]       = fmt_usd(tot["growth"]) if tot["growth"] else PENDING
        tr["Add. Items ($)"]   = fmt_usd(tot["add"])    if tot["add"]    else PENDING
        tr["Monthly Inv. ($)"] = fmt_usd(tot["inv"])    if tot["inv"]    else PENDING
        tr["Total CV ($)"]     = fmt_usd(tot["cv"])     if tot["cv"]     else PENDING
        rows.append(tr)

        st.markdown(
            html_table(rows, COLS, wide_cols=WIDE, total_row=True),
            unsafe_allow_html=True,
        )

        if tot["cv"] > 0:
            st.markdown(
                f'<div class="wt-caption" style="margin-top:6px;font-size:12px;color:#374151">'
                f'✅ <strong>Total CV (April 2026):</strong> '
                f'<span style="font-size:14px;font-weight:700;color:{_REV_COLOUR}">'
                f'{fmt_usd(tot["cv"])}</span>'
                f'&nbsp;&nbsp;<span style="color:#6b7280">'
                f'(SAAS {fmt_usd(tot["saas"])} · '
                f'{fee_col_label.replace(" ($)","")} {fmt_usd(tot["fee"])} · '
                f'Growth {fmt_usd(tot["growth"])} · '
                f'Add. Items {fmt_usd(tot["add"])})</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

    # ── Seat-Based ────────────────────────────────────────────────────────────
    seat = df[df["revenue_model"] == "SEAT_BASED"].copy()
    _rev_table(seat, "🪑 Seat-Based", "sec-seat", "Seat Fee ($)")

    # ── Revenue-Share ─────────────────────────────────────────────────────────
    revsh = df[df["revenue_model"] == "REVENUE_SHARE"].copy()
    _rev_table(revsh, "📊 Revenue-Share", "sec-revsh", "Rev Share ($)")

    # ── Legend ────────────────────────────────────────────────────────────────
    st.markdown(
        '<div class="wt-caption" style="margin-top:14px">'
        '<strong>Column guide (all figures from invoicing system):</strong> '
        'SAAS Fee = Q2 quarterly custom_prices "Historical…" line ÷ 3 · '
        'Seat Fee = quarterly "Prepaid Seats" ÷ 3 + SEAT_OVERAGE charges · '
        'Rev Share = ENROLLMENT purchases rev_share sum · '
        'Growth = Airlock + PBA + Import + RPL + Exemption purchase charges · '
        'Add. Items = custom_prices item_type="charge" on monthly invoice '
        '(e.g. legacy rev-share carry-overs) · '
        'Monthly Inv. = invoices.amount for the MONTHLY invoice · '
        'Total CV = SAAS + Seat/RevShare + Growth + Add. Items · '
        'Status: ✎ Draft = not yet reconciled · ● Open = reconciled, awaiting payment · '
        '✓ Paid = closed'
        '</div>',
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: ENROLMENT OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════

def show_enrolment_overview():
    """
    Enrolment Overview — same structure as the main Overview page but
    with all revenue columns removed. Respects the sidebar month/year selector.
    """
    _ENROL_COLOUR = "#0f766e"

    # ── Header ─────────────────────────────────────────────────────────────────
    if _MARK_DARK_PATH.exists():
        mark_b64 = _img_b64(_MARK_DARK_PATH)
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:12px;margin-bottom:2px">'
            f'<img src="{mark_b64}" style="height:36px;width:36px;border-radius:6px;'
            f'background:#e5e7eb;padding:5px;flex-shrink:0"/>'
            f'<div>'
            f'<div style="font-size:22px;font-weight:700;color:#111827;line-height:1.2">'
            f'📈 Enrolment Overview</div>'
            f'<div style="font-size:13px;color:#6b7280;margin-top:1px">{period}</div>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    df = df_all[df_all["revenue_model"].isin(model_filter)]

    # ── Enrolment Funnel Banner ────────────────────────────────────────────────
    _funnel_st_till  = int(df["st_till_last_month"].sum())
    _funnel_st_new   = int(df["st_new_this_month"].sum())
    _funnel_conv     = int(df["st_converted_this_month"].sum())
    _funnel_enrol    = int(df["new_enrol"].sum())

    # Fetch supplementary metrics (WLH, ST age, admission type).
    # conv_total and enrol_total come from the SAME query as their breakdowns
    # so tile number == sum of breakdown parts — always arithmetically clean.
    _extras = get_funnel_extras_cached(sel_year, sel_month)
    _wlh        = _extras.get("wlh_count",   0)
    _age_0_6m   = _extras.get("age_0_6m",    0)
    _age_6_12m  = _extras.get("age_6_12m",   0)
    _age_12pm   = _extras.get("age_12pm",    0)
    _conv_total = _extras.get("conv_total",  _funnel_conv)   # fallback to df total if query fails
    _adm_std    = _extras.get("adm_standard", 0)
    _adm_pba    = _extras.get("adm_pba",      0)
    _adm_rpl    = _extras.get("adm_rpl",      0)
    _enrol_total= _extras.get("enrol_total", _funnel_enrol)  # fallback to df total if query fails

    # Shared styles — better contrast, consistent across all tiles
    _lbl = ('font-size:11px;font-weight:600;text-transform:uppercase;'
            'letter-spacing:0.4px;color:#6b7280')
    _num = ('font-size:24px;font-weight:700;color:#111827;'
            'line-height:1.1;margin:4px 0 3px')
    _sub = 'font-size:11px;color:#374151'

    _wlh_sub = f'⚡ &gt;25 WLH: {_wlh:,}'
    _age_sub = f'0–6m: {_age_0_6m} · 6–12m: {_age_6_12m} · 12+m: {_age_12pm}'
    _adm_sub = f'Std: {_adm_std} · PBA: {_adm_pba} · RPL: {_adm_rpl}'

    # flex-wrap:nowrap + flex:1 keeps all 4 tiles on one line at any width
    _tile  = 'flex:1;min-width:0;text-align:center;padding:8px 12px'
    _sep   = 'flex-shrink:0;padding:0 6px;color:#d1d5db;font-size:18px;align-self:center'
    _divr  = 'flex-shrink:0;width:1px;background:#e2e8f0;align-self:stretch'

    st.markdown(
        f'<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;'
        f'padding:14px 16px;margin:12px 0 16px;display:flex;align-items:stretch;'
        f'flex-wrap:nowrap;gap:0">'
        # ── Tile 1: ST Students ──
        f'<div style="{_tile}">'
        f'<div style="{_lbl}">ST Students (till last month)</div>'
        f'<div style="{_num}">{_funnel_st_till:,}</div>'
        f'<div style="{_sub}">{_wlh_sub}</div>'
        f'</div>'
        f'<div style="{_divr}"></div>'
        # ── Tile 2: New ST ──
        f'<div style="{_tile}">'
        f'<div style="{_lbl}">New ST ({period})</div>'
        f'<div style="{_num}">{_funnel_st_new:,}</div>'
        f'<div style="{_sub}">&nbsp;</div>'
        f'</div>'
        f'<div style="{_sep}">→</div>'
        # ── Tile 3: ST→Degree ──
        f'<div style="{_tile}">'
        f'<div style="{_lbl}">ST → Degree ({period})</div>'
        f'<div style="{_num}">{_conv_total:,}</div>'
        f'<div style="{_sub}">{_age_sub}</div>'
        f'</div>'
        f'<div style="{_sep}">→</div>'
        # ── Tile 4: Enrolled ──
        f'<div style="{_tile}">'
        f'<div style="{_lbl}">Enrolled ({period})</div>'
        f'<div style="{_num}">{_enrol_total:,}</div>'
        f'<div style="{_sub}">{_adm_sub}</div>'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── SEAT-BASED ─────────────────────────────────────────────────────────────
    seat = df[df["revenue_model"] == "SEAT_BASED"]
    if not seat.empty and "SEAT_BASED" in model_filter:
        net_total = int(seat["net_additions"].sum())
        net_sign  = f"+{net_total}" if net_total >= 0 else str(net_total)
        st.markdown(
            f'<div class="sec sec-seat">'
            f'<h2>🪑 Seat-Based</h2>'
            f'<span class="sub">{len(seat)} colleges · '
            f'{int(seat["new_enrol"].sum())} new enrolments · Net {net_sign}</span>'
            f'</div>', unsafe_allow_html=True)

        _pm_short = calendar.month_abbr[pm]
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Colleges",                     len(seat))
        k2.metric("New Enrolments",               int(seat["new_enrol"].sum()))
        k3.metric(f"Paused ({_pm_short} {py})",   int(seat["pauses_m1"].sum()),
                  help="Students paused in the previous month")
        k4.metric(f"Archived ({_pm_short} {py})", int(seat["archives_m1"].sum()),
                  help="Students archived in the previous month")
        k5.metric(f"Net ({period})",               net_total,
                  help="New enrolments minus M-1 exits")

        # Sort: active seats → enrolments MTD → ST till → alpha
        seat = seat.copy()
        seat["_s1"] = pd.to_numeric(seat["active_base"],        errors="coerce").fillna(0)
        seat["_s2"] = pd.to_numeric(seat["new_enrol"],          errors="coerce").fillna(0)
        seat["_s3"] = pd.to_numeric(seat["st_till_last_month"], errors="coerce").fillna(0)
        seat = (seat.sort_values(["_s1","_s2","_s3","name"],
                                  ascending=[False,False,False,True])
                    .drop(columns=["_s1","_s2","_s3"]))

        pm_short     = calendar.month_abbr[pm]
        paused_col   = f"Paused ({pm_short} {py})"
        archived_col = f"Archived ({pm_short} {py})"
        net_col      = f"Net ± ({period})"

        eom_lbl = "Est. EOM"

        cols = ["College", active_lbl, proj_lbl, cur_lbl, eom_lbl,
                paused_col, archived_col, net_col,
                m1_lbl, vsm1_lbl, y1_lbl, vsy1_lbl,
                "ST Till", "ST >25h", "ST New", "ST→Deg",
                "PBA", "RPL Adm",
                "RPL <20", "RPL ≥20", "Oxford SBS"]

        _t = dict(active=0, proj=0, cur=0, eom_est=0,
                  pauses=0, archives=0, net=0,
                  m1_mtd=0, y1_mtd=0, m1=0, y1=0,
                  st_till=0, st_wlh=0, st_new=0, st_conv=0,
                  pba=0, rpl_adm=0,
                  rpl_low=0, rpl_high=0, oxford=0)
        rows = []
        for _, r in seat.iterrows():
            proj      = proj_month.get(r["name"])
            cur_enrol = int(r["new_enrol"])
            eom_html  = calc_eom_display(cur_enrol, proj,
                                         r.get("completion_rate"),
                                         r.get("avg_full_month"))

            row = {
                "College":    sn(r["name"]),
                active_lbl:   safe_int(r["active_base"]),
                proj_lbl:     proj if proj is not None else PENDING,
                cur_lbl:      cur_enrol,
                eom_lbl:      eom_html,
                paused_col:   int(r["pauses_m1"]),
                archived_col: int(r["archives_m1"]),
                net_col:      int(r["net_additions"]),
                m1_lbl:       int(r["new_enrol_m1"]),
                vsm1_lbl:     delta_detail(r["new_enrol"], r["new_enrol_m1_mtd"]),
                y1_lbl:       int(r["new_enrol_y1"]),
                vsy1_lbl:     delta_detail(r["new_enrol"], r["new_enrol_y1_mtd"]),
                "ST Till":    int(r["st_till_last_month"]),
                "ST >25h":    int(r.get("st_wlh_count", 0)),
                "ST New":     int(r["st_new_this_month"]),
                "ST→Deg":     int(r["st_converted_this_month"]),
                "PBA":        int(r["pba_count"]),
                "RPL Adm":    int(r.get("rpl_admission", 0)),
                "RPL <20":    int(r["rpl_low"]),
                "RPL ≥20":    int(r["rpl_high"]),
                "Oxford SBS": int(r["oxford_sbs"]),
            }
            rows.append(row)

            _t["active"]   += int(r["active_base"] or 0)
            _t["proj"]     += int(proj) if proj is not None else 0
            _t["cur"]      += cur_enrol
            cr  = _fee(r.get("completion_rate"))
            afm = _fee(r.get("avg_full_month"))
            if cr and cr >= _MIN_COMPLETION_RATE:
                _t["eom_est"] += round(cur_enrol / cr)
            elif afm and afm > cur_enrol:
                _t["eom_est"] += round(afm)
            else:
                _t["eom_est"] += cur_enrol
            _t["pauses"]   += int(r["pauses_m1"])
            _t["archives"] += int(r["archives_m1"])
            _t["net"]      += int(r["net_additions"])
            _t["m1"]       += int(r["new_enrol_m1"])
            _t["m1_mtd"]   += int(r["new_enrol_m1_mtd"])
            _t["y1"]       += int(r["new_enrol_y1"])
            _t["y1_mtd"]   += int(r["new_enrol_y1_mtd"])
            _t["st_till"]  += int(r["st_till_last_month"])
            _t["st_wlh"]   += int(r.get("st_wlh_count", 0))
            _t["st_new"]   += int(r["st_new_this_month"])
            _t["st_conv"]  += int(r["st_converted_this_month"])
            _t["pba"]      += int(r["pba_count"])
            _t["rpl_adm"]  += int(r.get("rpl_admission", 0))
            _t["rpl_low"]  += int(r["rpl_low"])
            _t["rpl_high"] += int(r["rpl_high"])
            _t["oxford"]   += int(r["oxford_sbs"])

        tr = {c: "" for c in cols}
        tr["College"]    = "TOTAL"
        tr[active_lbl]   = _t["active"]
        tr[proj_lbl]     = _t["proj"] if _t["proj"] else PENDING
        tr[cur_lbl]      = _t["cur"]
        if _t["proj"]:
            _eom_color = "#16a34a" if _t["eom_est"] >= _t["proj"] else "#dc2626"
            _eom_pct = round(_t["eom_est"] / _t["proj"] * 100)
            tr[eom_lbl] = (f"<span style='color:{_eom_color};font-weight:600'>"
                           f"{_t['eom_est']}</span>"
                           f"<br><span style='font-size:10px;font-weight:400;"
                           f"color:#9ca3af'>{_eom_pct}% of proj</span>")
        else:
            tr[eom_lbl] = PENDING
        tr[paused_col]   = _t["pauses"]
        tr[archived_col] = _t["archives"]
        tr[net_col]      = _t["net"]
        tr[m1_lbl]       = _t["m1"]
        tr[vsm1_lbl]     = delta_detail(_t["cur"], _t["m1_mtd"])
        tr[y1_lbl]       = _t["y1"]
        tr[vsy1_lbl]     = delta_detail(_t["cur"], _t["y1_mtd"])
        tr["ST Till"]    = _t["st_till"]
        tr["ST >25h"]    = _t["st_wlh"]
        tr["ST New"]     = _t["st_new"]
        tr["ST→Deg"]     = _t["st_conv"]
        tr["PBA"]        = _t["pba"]
        tr["RPL Adm"]    = _t["rpl_adm"]
        tr["RPL <20"]    = _t["rpl_low"]
        tr["RPL ≥20"]    = _t["rpl_high"]
        tr["Oxford SBS"] = _t["oxford"]
        rows.append(tr)

        st.markdown(html_table(rows, cols,
                               wide_cols={vsm1_lbl, vsy1_lbl},
                               narrow_cols={"College"},
                               total_row=True),
                    unsafe_allow_html=True)
        st.markdown(
            '<div class="wt-caption">'
            f'Active = ACTIVE/PENDING/SUBMITTED since contract-start · '
            f'{proj_lbl} = manual Q2 forecast · '
            f'{cur_lbl} = {"enrolments so far this month" if is_current_month else "full month enrolments"} '
            f'(day 1–{today.day if is_current_month else _last_day_sel}) · '
            f'Est. EOM = MTD ÷ seasonal rate (* = back-loader, uses hist. avg) · '
            f'{m1_lbl} / {y1_lbl} = full prior-month totals · '
            f'vs columns = {"MTD comparison" if is_current_month else "full period comparison"} · '
            f'Paused/Archived = {pm_short} {py} prior-month exits · '
            f'RPL Adm = admitted via RPL pathway'
            '</div>',
            unsafe_allow_html=True)

    # ── REVENUE-SHARE ──────────────────────────────────────────────────────────
    revsh = df[df["revenue_model"] == "REVENUE_SHARE"]
    if not revsh.empty and "REVENUE_SHARE" in model_filter:
        net_revsh_total = int(revsh["net_revsh"].sum())
        net_sign = f"+{net_revsh_total}" if net_revsh_total >= 0 else str(net_revsh_total)
        st.markdown(
            f'<div class="sec sec-revsh">'
            f'<h2>📊 Revenue-Share</h2>'
            f'<span class="sub">{len(revsh)} colleges · '
            f'{int(revsh["new_enrol"].sum())} new enrolments · Net {net_sign}</span>'
            f'</div>', unsafe_allow_html=True)

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Colleges",        len(revsh))
        k2.metric("New Enrolments",  int(revsh["new_enrol"].sum()))
        k3.metric("Archived ≤30d",   int(revsh["archived_30d"].sum()),
                  help="Students enrolled within last 30 days, now archived (refund window)")
        k4.metric(f"Net ({period})", net_revsh_total,
                  help="New enrolments minus 30-day refund archives")

        revsh = revsh.copy()
        revsh["_s1"] = pd.to_numeric(revsh["new_enrol"],          errors="coerce").fillna(0)
        revsh["_s2"] = pd.to_numeric(revsh["st_till_last_month"], errors="coerce").fillna(0)
        revsh["_s3"] = pd.to_numeric(revsh["st_new_this_month"],  errors="coerce").fillna(0)
        revsh = (revsh.sort_values(["_s1","_s2","_s3","name"],
                                    ascending=[False,False,False,True])
                      .drop(columns=["_s1","_s2","_s3"]))

        net_col = f"Net ({period})"

        eom_lbl = "Est. EOM"

        cols = ["College", proj_lbl, cur_lbl, eom_lbl,
                "Arch ≤30d", net_col,
                m1_lbl, vsm1_lbl, y1_lbl, vsy1_lbl,
                "ST Till", "ST >25h", "ST New", "ST→Deg",
                "PBA", "RPL Adm",
                "RPL <20", "RPL ≥20", "Oxford SBS"]

        _rt = dict(proj=0, cur=0, eom_enrol=0,
                   arch_30d=0, net=0,
                   m1_mtd=0, y1_mtd=0, m1=0, y1=0,
                   st_till=0, st_wlh=0, st_new=0, st_conv=0,
                   pba=0, rpl_adm=0,
                   rpl_low=0, rpl_high=0, oxford=0)
        rows = []
        for _, r in revsh.iterrows():
            proj      = proj_month.get(r["name"])
            cur_enrol = int(r["new_enrol"])
            eom_html  = calc_eom_display(cur_enrol, proj,
                                         r.get("completion_rate"),
                                         r.get("avg_full_month"))

            row = {
                "College":    sn(r["name"]),
                proj_lbl:     proj if proj is not None else PENDING,
                cur_lbl:      cur_enrol,
                eom_lbl:      eom_html,
                "Arch ≤30d":  int(r["archived_30d"]),
                net_col:      int(r["net_revsh"]),
                m1_lbl:       int(r["new_enrol_m1"]),
                vsm1_lbl:     delta_detail(r["new_enrol"], r["new_enrol_m1_mtd"]),
                y1_lbl:       int(r["new_enrol_y1"]),
                vsy1_lbl:     delta_detail(r["new_enrol"], r["new_enrol_y1_mtd"]),
                "ST Till":    int(r["st_till_last_month"]),
                "ST >25h":    int(r.get("st_wlh_count", 0)),
                "ST New":     int(r["st_new_this_month"]),
                "ST→Deg":     int(r["st_converted_this_month"]),
                "PBA":        int(r["pba_count"]),
                "RPL Adm":    int(r.get("rpl_admission", 0)),
                "RPL <20":    int(r["rpl_low"]),
                "RPL ≥20":    int(r["rpl_high"]),
                "Oxford SBS": int(r["oxford_sbs"]),
            }
            rows.append(row)

            _rt["proj"]     += int(proj) if proj is not None else 0
            _rt["cur"]      += cur_enrol
            cr  = _fee(r.get("completion_rate"))
            afm = _fee(r.get("avg_full_month"))
            if cr and cr >= _MIN_COMPLETION_RATE:
                _rt["eom_enrol"] += round(cur_enrol / cr)
            elif afm and afm > cur_enrol:
                _rt["eom_enrol"] += round(afm)
            else:
                _rt["eom_enrol"] += cur_enrol
            _rt["arch_30d"] += int(r["archived_30d"])
            _rt["net"]      += int(r["net_revsh"])
            _rt["m1"]       += int(r["new_enrol_m1"])
            _rt["m1_mtd"]   += int(r["new_enrol_m1_mtd"])
            _rt["y1"]       += int(r["new_enrol_y1"])
            _rt["y1_mtd"]   += int(r["new_enrol_y1_mtd"])
            _rt["st_till"]  += int(r["st_till_last_month"])
            _rt["st_wlh"]   += int(r.get("st_wlh_count", 0))
            _rt["st_new"]   += int(r["st_new_this_month"])
            _rt["st_conv"]  += int(r["st_converted_this_month"])
            _rt["pba"]      += int(r["pba_count"])
            _rt["rpl_adm"]  += int(r.get("rpl_admission", 0))
            _rt["rpl_low"]  += int(r["rpl_low"])
            _rt["rpl_high"] += int(r["rpl_high"])
            _rt["oxford"]   += int(r["oxford_sbs"])

        _rtr = {c: "" for c in cols}
        _rtr["College"]    = "TOTAL"
        _rtr[proj_lbl]     = _rt["proj"] if _rt["proj"] else PENDING
        _rtr[cur_lbl]      = _rt["cur"]
        if _rt["proj"]:
            _eom_color_r = "#16a34a" if _rt["eom_enrol"] >= _rt["proj"] else "#dc2626"
            _eom_pct_r = round(_rt["eom_enrol"] / _rt["proj"] * 100)
            _rtr[eom_lbl] = (f"<span style='color:{_eom_color_r};font-weight:600'>"
                             f"{_rt['eom_enrol']}</span>"
                             f"<br><span style='font-size:10px;font-weight:400;"
                             f"color:#9ca3af'>{_eom_pct_r}% of proj</span>")
        else:
            _rtr[eom_lbl] = PENDING
        _rtr["Arch ≤30d"]  = _rt["arch_30d"]
        _rtr[net_col]      = _rt["net"]
        _rtr[m1_lbl]       = _rt["m1"]
        _rtr[vsm1_lbl]     = delta_detail(_rt["cur"], _rt["m1_mtd"])
        _rtr[y1_lbl]       = _rt["y1"]
        _rtr[vsy1_lbl]     = delta_detail(_rt["cur"], _rt["y1_mtd"])
        _rtr["ST Till"]    = _rt["st_till"]
        _rtr["ST >25h"]    = _rt["st_wlh"]
        _rtr["ST New"]     = _rt["st_new"]
        _rtr["ST→Deg"]     = _rt["st_conv"]
        _rtr["PBA"]        = _rt["pba"]
        _rtr["RPL Adm"]    = _rt["rpl_adm"]
        _rtr["RPL <20"]    = _rt["rpl_low"]
        _rtr["RPL ≥20"]    = _rt["rpl_high"]
        _rtr["Oxford SBS"] = _rt["oxford"]
        rows.append(_rtr)

        st.markdown(html_table(rows, cols,
                               wide_cols={vsm1_lbl, vsy1_lbl},
                               narrow_cols={"College"},
                               total_row=True),
                    unsafe_allow_html=True)
        st.markdown(
            '<div class="wt-caption">'
            f'{proj_lbl} = manual Q2 forecast · '
            f'{cur_lbl} = {"enrolments so far this month" if is_current_month else "full month enrolments"} '
            f'(day 1–{today.day if is_current_month else _last_day_sel}) · '
            f'Est. EOM = MTD ÷ seasonal rate (* = back-loader, uses hist. avg) · '
            f'{m1_lbl} / {y1_lbl} = full prior-month totals · '
            f'vs columns = {"MTD comparison" if is_current_month else "full period comparison"} · '
            f'Arch ≤30d = enrolled within last 30 days, now archived (refund window) · '
            f'RPL Adm = admitted via RPL pathway'
            '</div>',
            unsafe_allow_html=True)


# ── Router ────────────────────────────────────────────────────────────────────
if page == "enrolment_overview":
    show_enrolment_overview()
elif page == "revenue_overview":
    show_revenue_overview()
elif page == "overview":
    show_overview()
elif page in _PAGE_ID_TO_SQUAD:
    sq = _PAGE_ID_TO_SQUAD[page]
    show_overview(
        college_filter=set(SQUAD_MAP[sq]),
        squad_name=sq,
    )
else:
    show_college_detail(page)
