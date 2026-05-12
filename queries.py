"""
Bulk queries for the Woolf Business Dashboard.
Queries run in parallel via ThreadPoolExecutor — typically loads in ~10-15s.
Read-only SELECTs against Woolf BigQuery (database_id=3).
"""

from datetime import date
import calendar
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from metabase import run_query

OXFORD_SBS_COURSE_NAMES = [
    "Business Essentials: The Complete Enterprise Toolkit - I",
    "Business Essentials: The Complete Enterprise Toolkit - II",
    "Cyber-Resilient Leadership for Digital Transformation",
    "Future Ready Leadership: Insights, Models and Practices",
    "Introduction to Advanced Business Analytics with AI",
    "AI and Business Analytics",
    "Mastering Digital Transformation: Building the Foundation for AI Adoption",
]
SBS_NAMES_SQL = ", ".join(f"'{n}'" for n in OXFORD_SBS_COURSE_NAMES)


# ── Date helpers ──────────────────────────────────────────────────────────────

def month_bounds(year: int, month: int):
    first = date(year, month, 1)
    last  = date(year, month, calendar.monthrange(year, month)[1])
    return first, last

def prev_month(year: int, month: int):
    if month == 1:
        return year - 1, 12
    return year, month - 1

def year_ago(year: int, month: int):
    return year - 1, month


# ── 1. Colleges ───────────────────────────────────────────────────────────────

def get_colleges() -> pd.DataFrame:
    return run_query("""
    SELECT id, name, revenue_model
    FROM production.orgs
    WHERE status = 'VERIFIED'
    ORDER BY revenue_model DESC, name
    """)


# ── Individual bulk queries (each returns college_id-indexed DataFrame) ───────

def _all_activity_counts(year: int, month: int,
                         py: int, pm: int,
                         yy1: int, my1: int,
                         today_day: int) -> pd.DataFrame:
    """
    Single activities table scan covering all 5 date windows:
      - Current month       → new_enrol, pauses, archives
      - M-1 full month      → new_enrol_m1, pauses_m1, archives_m1
      - M-1 MTD-capped      → new_enrol_m1_mtd  (apple-to-apple vs comparison)
      - Y-1 full month      → new_enrol_y1
      - Y-1 MTD-capped      → new_enrol_y1_mtd  (apple-to-apple vs comparison)

    Replaces 5 separate queries (_activity_counts ×3, _activity_counts_mtd ×2).
    BigQuery scans the table once from the earliest needed date → much faster.
    """
    first,    last    = month_bounds(year,  month)
    m1_first, m1_last = month_bounds(py,    pm)
    y1_first, y1_last = month_bounds(yy1,   my1)

    m1_cap_day = min(today_day, calendar.monthrange(py,  pm)[1])
    y1_cap_day = min(today_day, calendar.monthrange(yy1, my1)[1])
    m1_cap = date(py,  pm,  m1_cap_day)
    y1_cap = date(yy1, my1, y1_cap_day)

    sql = f"""
    SELECT
      college_id,
      -- Current month
      COUNTIF(kind = 'addDegreeStudent'     AND DATE(created) BETWEEN '{first}'    AND '{last}')     AS new_enrol,
      COUNTIF(kind = 'pauseDegreeStudent'   AND DATE(created) BETWEEN '{first}'    AND '{last}')     AS pauses,
      COUNTIF(kind = 'archiveDegreeStudent' AND DATE(created) BETWEEN '{first}'    AND '{last}')     AS archives,
      -- M-1 full month
      COUNTIF(kind = 'addDegreeStudent'     AND DATE(created) BETWEEN '{m1_first}' AND '{m1_last}')  AS new_enrol_m1,
      COUNTIF(kind = 'pauseDegreeStudent'   AND DATE(created) BETWEEN '{m1_first}' AND '{m1_last}')  AS pauses_m1,
      COUNTIF(kind = 'archiveDegreeStudent' AND DATE(created) BETWEEN '{m1_first}' AND '{m1_last}')  AS archives_m1,
      -- M-1 MTD-capped (day 1–{today_day} of M-1 vs day 1–{today_day} of current)
      COUNTIF(kind = 'addDegreeStudent'     AND DATE(created) BETWEEN '{m1_first}' AND '{m1_cap}')   AS new_enrol_m1_mtd,
      -- Y-1 full month
      COUNTIF(kind = 'addDegreeStudent'     AND DATE(created) BETWEEN '{y1_first}' AND '{y1_last}')  AS new_enrol_y1,
      -- Y-1 MTD-capped
      COUNTIF(kind = 'addDegreeStudent'     AND DATE(created) BETWEEN '{y1_first}' AND '{y1_cap}')   AS new_enrol_y1_mtd
    FROM production.activities
    WHERE kind IN ('addDegreeStudent','pauseDegreeStudent','archiveDegreeStudent')
      AND DATE(created) >= '{y1_first}'
    GROUP BY college_id
    """
    return run_query(sql).set_index("college_id")


def _active_base(year: int, month: int) -> pd.DataFrame:
    """
    Active students per college (ACTIVE / PENDING / SUBMITTED), filtered to
    students enrolled on or after the college's CURRENT phase start date.

    Current month  → live count (status field, as today).
    Past month     → historical snapshot as of last day of that month:
                     enrolled before EOD, not yet archived, not paused on that day.
    """
    today = date.today()
    is_current = (year == today.year and month == today.month)

    if is_current:
        # Live count — same as before
        sql = """
        WITH phase_start AS (
          SELECT s.college_id, MAX(sp.version) AS phase_date
          FROM production.subscription_phases sp
          JOIN production.subscriptions s ON s.id = sp.subscription_id
          GROUP BY s.college_id
        )
        SELECT
          ds.college_id,
          COUNT(*) AS active_base
        FROM production.degree_students ds
        LEFT JOIN phase_start ps ON ps.college_id = ds.college_id
        WHERE ds.status IN ('ACTIVE','PENDING','SUBMITTED')
          AND (ps.phase_date IS NULL OR DATE(ds.created) >= DATE(ps.phase_date))
        GROUP BY ds.college_id
        """
    else:
        # Historical snapshot: count students active on last day of that month
        _, last_day = month_bounds(year, month)
        sql = f"""
        WITH phase_start AS (
          SELECT s.college_id, MAX(sp.version) AS phase_date
          FROM production.subscription_phases sp
          JOIN production.subscriptions s ON s.id = sp.subscription_id
          GROUP BY s.college_id
        )
        SELECT
          ds.college_id,
          COUNT(*) AS active_base
        FROM production.degree_students ds
        LEFT JOIN phase_start ps ON ps.college_id = ds.college_id
        WHERE
          -- Enrolled on or before last day of month
          DATE(ds.created) <= '{last_day}'
          -- Within current billing phase
          AND (ps.phase_date IS NULL OR DATE(ds.created) >= DATE(ps.phase_date))
          -- Not archived before end of month
          AND (ds.delisted_timestamp IS NULL OR DATE(ds.delisted_timestamp) > '{last_day}')
          -- Not paused on the last day of month
          AND NOT (
            ds.pause_started IS NOT NULL
            AND DATE(ds.pause_started) <= '{last_day}'
            AND (ds.pause_ended IS NULL OR DATE(ds.pause_ended) > '{last_day}')
          )
        GROUP BY ds.college_id
        """
    return run_query(sql).set_index("college_id")


def _seat_exp_revenue(year: int, month: int) -> pd.DataFrame:
    """
    Expected monthly seat revenue per college = SUM(active_students_per_degree × seat_fee).

    Current month  → live active count × seat fee.
    Past month     → historical snapshot (EOD last day of month) × seat fee.
    """
    today = date.today()
    is_current = (year == today.year and month == today.month)

    if is_current:
        active_filter = "ds.status IN ('ACTIVE','PENDING','SUBMITTED')"
    else:
        _, last_day = month_bounds(year, month)
        active_filter = f"""
          DATE(ds.created) <= '{last_day}'
          AND (ds.delisted_timestamp IS NULL OR DATE(ds.delisted_timestamp) > '{last_day}')
          AND NOT (
            ds.pause_started IS NOT NULL
            AND DATE(ds.pause_started) <= '{last_day}'
            AND (ds.pause_ended IS NULL OR DATE(ds.pause_ended) > '{last_day}')
          )
        """

    sql = f"""
    WITH phase_start AS (
      SELECT s.college_id,
        MAX(sp.version) AS phase_date,
        MAX(sp.id)      AS phase_id
      FROM production.subscription_phases sp
      JOIN production.subscriptions s ON s.id = sp.subscription_id
      GROUP BY s.college_id
    ),
    seat_fee_per_degree AS (
      SELECT prd.college_id, prd.degree_id, MAX(pr.price) AS seat_fee
      FROM production.prices    pr
      JOIN production.products  prd ON prd.id    = pr.product_id
      JOIN production.services  svc ON svc.id    = prd.service_id
      JOIN phase_start           ps ON ps.phase_id = pr.phase_id
      WHERE svc.kind = 'SEAT' AND pr.price > 0
      GROUP BY prd.college_id, prd.degree_id
    )
    SELECT
      ds.college_id,
      SUM(sf.seat_fee) AS seat_exp_rev
    FROM production.degree_students ds
    JOIN phase_start             ps ON ps.college_id = ds.college_id
    LEFT JOIN seat_fee_per_degree sf ON sf.college_id = ds.college_id
                                    AND sf.degree_id  = ds.degree_id
    WHERE {active_filter}
      AND DATE(ds.created) >= DATE(ps.phase_date)
    GROUP BY ds.college_id
    """
    return run_query(sql).set_index("college_id")


def _st_new_counts(year: int, month: int) -> pd.DataFrame:
    """
    Study-track enrolment counts from the canonical st_students table.
    Joins st_students → students (via student_id = students.id) to get college_id.
    st_till_last_month: cumulative ST students created before this month.
    st_new_this_month:  new ST students created THIS month (distinct from degree conversion).
    """
    py, pm       = prev_month(year, month)
    _, last_prev = month_bounds(py, pm)
    first, last  = month_bounds(year, month)
    sql = f"""
    SELECT
      s.college_id,
      COUNTIF(DATE(st.created) <= '{last_prev}')                           AS st_till_last_month,
      COUNTIF(DATE(st.created) BETWEEN '{first}' AND '{last}')             AS st_new_this_month
    FROM production.st_students st
    JOIN production.students s ON s.id = st.student_id
    GROUP BY s.college_id
    """
    return run_query(sql).set_index("college_id")


def _st_conv_pba_counts(year: int, month: int) -> pd.DataFrame:
    """
    Single degree_students scan returning both:
      st_converted_this_month  – ST→Degree conversions this month
                                 (has_activity_before_invitation = true)
      pba_count                – PBA-converted enrolments this month

    Replaces two separate queries (_st_conversions + _pba_counts).
    """
    first, last = month_bounds(year, month)
    sql = f"""
    SELECT
      college_id,
      COUNTIF(DATE(created) BETWEEN '{first}' AND '{last}'
              AND has_activity_before_invitation = true)  AS st_converted_this_month,
      COUNTIF(is_pba_converted = true
              AND DATE(created) BETWEEN '{first}' AND '{last}') AS pba_count
    FROM production.degree_students
    GROUP BY college_id
    """
    return run_query(sql).set_index("college_id")


def _revsh_exp_revenue(year: int, month: int) -> pd.DataFrame:
    """
    Per-college expected revenue for revenue-share colleges this month.

    Per enrolled student, priority order:
      1. Last-3-months degree-level avg from actual purchases  ← PRIMARY
            → avg actual USD rev_share Woolf invoiced for this specific degree
              in the last 3 calendar months; reflects real discounts, waivers, FX.
              Applied first for ALL colleges — actual invoice data beats the formula.
      2. USD tuition formula  ← FALLBACK for new degrees with no purchase history
            → MAX(enrollment_pct × tuition_usd, min_rev_share)
              only used when there are no recent purchases for this degree
      3. College-level all-time historical avg from purchases
            → avg across ALL degrees in this college (all time)
            covers new programmes at established colleges (e.g. Scaler MSc CS)
      4. min_rev_share floor (last resort — guaranteed contractual minimum)

    NOTE: purchases with category IS NULL are treated as billable (not studytrack).
    The != 'STUDYTRACK' filter must use IS DISTINCT FROM to avoid excluding NULLs.

    Returns college_id-indexed DataFrame with:
      exp_rev_usd      – total expected revenue (USD) for new enrolments this month
      est_from_min_rev – count of students where even college-level history was
                         unavailable (estimate = min floor only; shown as ~ in UI)
    """
    first, last = month_bounds(year, month)
    # Last-3-months window: start of (month-2) so we cover m-2, m-1, current month
    py, pm = year, month
    for _ in range(2):
        py, pm = (py - 1, 12) if pm == 1 else (py, pm - 1)
    three_months_ago = date(py, pm, 1).isoformat()
    sql = f"""
    WITH latest_phases AS (
      SELECT s.college_id, sp.id AS phase_id,
        ROW_NUMBER() OVER (PARTITION BY s.college_id ORDER BY sp.version DESC) AS rn
      FROM production.subscription_phases sp
      JOIN production.subscriptions s ON s.id = sp.subscription_id
    ),
    -- Per-degree rates (enrollment_pct + min_rev_share) from the latest phase.
    -- Keyed by degree_id so different programmes (e.g. Directors' MBA at $450
    -- vs Award at $30) get the correct floors.
    degree_fees AS (
      SELECT
        lp.college_id,
        prd.degree_id,
        MAX(CASE WHEN svc.kind = 'ENROLLMENT'    THEN pr.price END) AS enrollment_pct,
        MAX(CASE WHEN svc.kind = 'MIN_REV_SHARE' THEN pr.price END) AS min_rev_share
      FROM latest_phases lp
      JOIN production.prices    pr  ON pr.phase_id = lp.phase_id
      JOIN production.products  prd ON prd.id       = pr.product_id
      JOIN production.services  svc ON svc.id       = prd.service_id
      WHERE lp.rn = 1
      GROUP BY lp.college_id, prd.degree_id
    ),
    -- Priority 2: last-3-months degree-level avg actual USD rev_share.
    -- Restricted to recent purchases so the avg reflects current tuition pricing.
    -- IMPORTANT: uses IS DISTINCT FROM to correctly include purchases where
    -- category IS NULL (e.g. Directors' Institute Award purchases have null category
    -- and were previously excluded by != 'STUDYTRACK', causing $30 floor misuse).
    avg_rev_per_degree AS (
      SELECT
        p.college_id,
        p.degree_id,
        AVG(GREATEST(p.rev_share, COALESCE(df2.min_rev_share, 0))) AS avg_rev_share
      FROM production.purchases p
      JOIN degree_fees df2
        ON df2.college_id = p.college_id
       AND df2.degree_id  = p.degree_id
      WHERE p.category IS DISTINCT FROM 'STUDYTRACK'
        AND p.rev_share IS NOT NULL
        AND DATE(p.created) >= '{three_months_ago}'
      GROUP BY p.college_id, p.degree_id
    ),
    -- Priority 3: college-level all-time historical avg actual USD rev_share.
    -- Used when a degree has no purchase history in the last 3 months.
    -- Example: Scaler MSc CS / MSc CS:AIML (new degrees) use the college avg
    -- of ~$237 from MSc AI history instead of falling to the $90 floor.
    avg_rev_per_college AS (
      SELECT
        p.college_id,
        AVG(GREATEST(p.rev_share, COALESCE(
          (SELECT MIN(df3.min_rev_share) FROM degree_fees df3
           WHERE df3.college_id = p.college_id),
          0
        ))) AS avg_rev_share
      FROM production.purchases p
      WHERE p.category IS DISTINCT FROM 'STUDYTRACK'
        AND p.rev_share IS NOT NULL
      GROUP BY p.college_id
    ),
    new_enrol AS (
      SELECT
        ds.college_id,
        ds.degree_id,
        -- USD tuition: prefer per-student invoice override, else degree default if USD
        COALESCE(
          ipo.tuition_due,
          CASE WHEN d.currency = 'USD' THEN CAST(d.tuition_cost AS NUMERIC) END
        ) AS tuition_usd
      FROM production.degree_students ds
      LEFT JOIN production.degrees d ON d.id = ds.degree_id
      LEFT JOIN production.invoice_purchase_overrides ipo
             ON ipo.degree_student_id = ds.id
            AND ipo.service_kind = 'ENROLLMENT'
            AND ipo.tuition_due IS NOT NULL
            AND ipo.tuition_due > 0
      WHERE DATE(ds.created) BETWEEN '{first}' AND '{last}'
    )
    SELECT
      ne.college_id,
      ROUND(SUM(
        CASE
          -- P1: last-3-months degree avg from actual invoices (all colleges)
          --     most accurate: reflects real tuition, discounts, FX adjustments
          WHEN ar.avg_rev_share IS NOT NULL
            THEN ar.avg_rev_share
          -- P2: USD tuition formula — new degree, no purchase history yet
          --     → MAX(enrollment_pct × tuition_usd, min_rev_share)
          WHEN ne.tuition_usd IS NOT NULL
            THEN GREATEST(df.enrollment_pct * ne.tuition_usd,
                          COALESCE(df.min_rev_share, 0))
          -- P3: college-level all-time avg — new programme, established college
          WHEN cr.avg_rev_share IS NOT NULL
            THEN cr.avg_rev_share
          -- P4: contractual min_rev_share floor (last resort)
          ELSE COALESCE(df.min_rev_share, 0)
        END
      ), 0) AS exp_rev_usd,
      -- Count students estimated from min floor (P4 used — no history, no USD tuition)
      COUNTIF(ar.avg_rev_share IS NULL
              AND ne.tuition_usd IS NULL
              AND cr.avg_rev_share IS NULL) AS est_from_min_rev
    FROM new_enrol ne
    LEFT JOIN degree_fees df
           ON df.college_id = ne.college_id
          AND df.degree_id  = ne.degree_id
    LEFT JOIN avg_rev_per_degree ar
           ON ar.college_id = ne.college_id
          AND ar.degree_id  = ne.degree_id
    LEFT JOIN avg_rev_per_college cr
           ON cr.college_id = ne.college_id
    GROUP BY ne.college_id
    """
    return run_query(sql).set_index("college_id")


def _revsh_archived_30d(year: int, month: int) -> pd.DataFrame:
    """
    Revenue-share 30-day refund window: students currently archived who enrolled
    within the last 30 days of month-end.  Fast single-table scan (~6s).
    """
    _, last = month_bounds(year, month)
    sql = f"""
    SELECT college_id, COUNT(*) AS archived_30d
    FROM production.degree_students
    WHERE status = 'ARCHIVED'
      AND DATE(created) >= DATE_SUB('{last}', INTERVAL 30 DAY)
      AND DATE(created) <= '{last}'
    GROUP BY college_id
    """
    return run_query(sql).set_index("college_id")


def _rpl_counts(year: int, month: int) -> pd.DataFrame:
    first, last = month_bounds(year, month)
    sql = f"""
    SELECT
      ds.college_id,
      COUNTIF(r.approved_credits < 20)  AS rpl_low,
      COUNTIF(r.approved_credits >= 20) AS rpl_high
    FROM production.rpl_exemption_requests r
    JOIN production.degree_students ds ON r.degree_student_id = ds.id
    WHERE DATE(r.processed) BETWEEN '{first}' AND '{last}'
      AND r.status = 'APPROVED'
    GROUP BY ds.college_id
    """
    return run_query(sql).set_index("college_id")


def _rpl_admissions(year: int, month: int) -> pd.DataFrame:
    first, last = month_bounds(year, month)
    sql = f"""
    SELECT
      ds.college_id,
      COUNT(*) AS rpl_admissions
    FROM production.degree_student_services dss
    JOIN production.degree_students ds ON dss.degree_student_id = ds.id
    WHERE dss.rpl IS NOT NULL
      AND DATE(dss.rpl) BETWEEN '{first}' AND '{last}'
    GROUP BY ds.college_id
    """
    return run_query(sql).set_index("college_id")


def _completion_rates(through_day: int) -> pd.DataFrame:
    """
    Per-college seasonal completion rate: average of (MTD_enrol / full_month_enrol)
    over the last 6 complete calendar months.

    Used to project the likely full-month total from the current MTD count:
        estimated_final = current_enrol / completion_rate
        variance        = estimated_final − projected

    Only months where total_month > 0 are included.  Returns NULL (NaN) for
    colleges with no historical data in the window.
    """
    sql = f"""
    WITH past_months AS (
      SELECT
        college_id,
        FORMAT_DATE('%Y-%m', DATE(created))                              AS ym,
        COUNTIF(EXTRACT(DAY FROM DATE(created)) <= {through_day})        AS mtd_count,
        COUNT(*)                                                          AS total_month
      FROM production.activities
      WHERE kind = 'addDegreeStudent'
        AND DATE(created) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 6 MONTH)
        AND DATE(created) <  DATE_TRUNC(CURRENT_DATE(), MONTH)
      GROUP BY college_id, ym
    )
    SELECT
      college_id,
      AVG(SAFE_DIVIDE(mtd_count, total_month)) AS completion_rate
    FROM past_months
    WHERE total_month > 0
    GROUP BY college_id
    """
    return run_query(sql).set_index("college_id")



def _oxford_sbs(year: int, month: int) -> pd.DataFrame:
    first, last = month_bounds(year, month)
    sql = f"""
    SELECT c.college_id, COUNT(*) AS oxford_sbs
    FROM production.ams_course_students acs
    JOIN production.courses c ON acs.course_id = c.id
    WHERE c.name IN ({SBS_NAMES_SQL})
      AND DATE(acs.created) BETWEEN '{first}' AND '{last}'
    GROUP BY c.college_id
    """
    return run_query(sql).set_index("college_id")


# ── Pricing / fee configuration ───────────────────────────────────────────────

def get_pricing_config() -> pd.DataFrame:
    """
    Per-college fee configuration from the latest subscription phase.

    Service kinds:
      SEAT, SEAT_OVERAGE          → seat billing
      AIRLOCK, EXEMPTION, IMPORT,
      PBA, RPL                    → StudyTrack service fees
      ENROLLMENT                  → revenue-share % (decimal, e.g. 0.10 = 10 %)
      MIN_REV_SHARE               → minimum monthly revenue guarantee

    Returns a DataFrame indexed by college_id.  Some colleges have two SEAT
    price tiers (e.g. Exeed: $50 for ≤90 ECTS, $20 for >90 ECTS) — these are
    captured as seat_fee_min / seat_fee_max.
    """
    sql = """
    WITH latest_phases AS (
      SELECT
        sp.id          AS phase_id,
        s.college_id,
        sp.version     AS phase_start,
        ROW_NUMBER() OVER (
          PARTITION BY s.college_id
          ORDER BY sp.version DESC
        ) AS rn
      FROM production.subscription_phases sp
      JOIN production.subscriptions s ON s.id = sp.subscription_id
    ),
    current_prices AS (
      SELECT
        lp.college_id,
        lp.phase_start,
        svc.kind,
        pr.price
      FROM latest_phases lp
      JOIN production.prices    pr  ON pr.phase_id  = lp.phase_id
      JOIN production.products  prd ON prd.id        = pr.product_id
      JOIN production.services  svc ON svc.id        = prd.service_id
      WHERE lp.rn = 1
        AND pr.price > 0
    )
    SELECT
      college_id,
      MAX(phase_start)                                       AS phase_start,
      MIN(CASE WHEN kind = 'SEAT'          THEN price END)   AS seat_fee_min,
      MAX(CASE WHEN kind = 'SEAT'          THEN price END)   AS seat_fee_max,
      MAX(CASE WHEN kind = 'SEAT_OVERAGE'  THEN price END)   AS seat_overage_fee,
      MAX(CASE WHEN kind = 'AIRLOCK'       THEN price END)   AS airlock_fee,
      MAX(CASE WHEN kind = 'EXEMPTION'     THEN price END)   AS exemption_fee,
      MAX(CASE WHEN kind = 'IMPORT'        THEN price END)   AS import_fee,
      MAX(CASE WHEN kind = 'PBA'           THEN price END)   AS pba_fee,
      MAX(CASE WHEN kind = 'RPL'           THEN price END)   AS rpl_fee,
      MAX(CASE WHEN kind = 'ENROLLMENT'    THEN price END)   AS enrollment_pct,
      MAX(CASE WHEN kind = 'MIN_REV_SHARE' THEN price END)   AS min_rev_share
    FROM current_prices
    GROUP BY college_id
    """
    return run_query(sql).set_index("college_id")


# ── College detail queries ────────────────────────────────────────────────────

def get_college_trend(college_id: str, months: int = 14) -> pd.DataFrame:
    """Monthly enrolment / pause / archive counts for a single college."""
    sql = f"""
    SELECT
      FORMAT_DATE('%Y-%m', DATE(created)) AS month,
      COUNTIF(kind = 'addDegreeStudent')     AS new_enrol,
      COUNTIF(kind = 'pauseDegreeStudent')   AS pauses,
      COUNTIF(kind = 'archiveDegreeStudent') AS archives
    FROM production.activities
    WHERE kind IN ('addDegreeStudent','pauseDegreeStudent','archiveDegreeStudent')
      AND college_id = '{college_id}'
      AND DATE(created) >= DATE_SUB(CURRENT_DATE(), INTERVAL {months} MONTH)
    GROUP BY month
    ORDER BY month
    """
    return run_query(sql)


def get_st_trend(college_id: str, months: int = 14) -> pd.DataFrame:
    """
    Monthly new study-track enrolments for a single college.
    Uses st_students (same source as the st_new_this_month KPI metric),
    NOT degree_students.has_activity_before_invitation which shows
    ST→Degree conversions — a different metric.
    """
    sql = f"""
    SELECT
      FORMAT_DATE('%Y-%m', DATE(st.created)) AS month,
      COUNT(*)                                AS new_st
    FROM production.st_students st
    JOIN production.students s ON s.id = st.student_id
    WHERE s.college_id = '{college_id}'
      AND DATE(st.created) >= DATE_SUB(CURRENT_DATE(), INTERVAL {months} MONTH)
    GROUP BY month
    ORDER BY month
    """
    return run_query(sql)


def get_graduation_data(college_id: str) -> dict:
    """
    Graduation stats for a college.
    'COMPLETED' status in degree_students = graduated.
    Returns a dict with:
      - summary  : DataFrame(total_graduates, total_enrolled, grad_rate_pct)
      - trend    : DataFrame(month, graduates) — monthly new graduates, all time
    """
    summary_sql = f"""
    SELECT
      COUNTIF(status = 'COMPLETED')  AS total_graduates,
      COUNT(*)                        AS total_enrolled,
      ROUND(
        SAFE_DIVIDE(COUNTIF(status = 'COMPLETED'), COUNT(*)) * 100, 1
      )                               AS grad_rate_pct
    FROM production.degree_students
    WHERE college_id = '{college_id}'
    """

    trend_sql = f"""
    SELECT
      FORMAT_DATE('%Y-%m', DATE(created)) AS month,
      COUNT(*)                             AS graduates
    FROM production.degree_students
    WHERE college_id  = '{college_id}'
      AND status      = 'COMPLETED'
    GROUP BY month
    ORDER BY month
    """

    return {
        "summary": run_query(summary_sql),
        "trend":   run_query(trend_sql),
    }


# ── Enrolment Overview: funnel extras ────────────────────────────────────────

def get_funnel_extras(year: int, month: int) -> dict:
    """
    Returns supplementary funnel metrics for the Enrolment Overview top banner.

    Keys returned
    ─────────────
    wlh_count    : ST students (created before this month) with workload_count > 25
    age_0_3m     : ST→Deg converters this month who were ST for < 3 months
    age_3_6m     : 3–6 months
    age_6_12m    : 6–12 months
    age_12pm     : 12+ months
    adm_standard : degree_students enrolled this month via standard admission
    adm_pba      : via PBA (is_pba_converted = true)
    adm_rpl      : via RPL (rpl_exemption_request_id IS NOT NULL)
    """
    first, _ = month_bounds(year, month)
    if month == 12:
        next_first = date(year + 1, 1, 1)
    else:
        next_first = date(year, month + 1, 1)

    sql = f"""
    WITH
    st_wlh AS (
      -- workload_count is stored in minutes; 25 WLH = 1500 min
      -- degree_count = 0 excludes students already converted to a degree
      SELECT COUNT(*) AS cnt
      FROM production.st_students
      WHERE DATE(created) < '{first}'
        AND workload_count >= 1500
        AND degree_count = 0
    ),
    st_age AS (
      SELECT
        CASE
          WHEN DATE_DIFF(DATE(ds.created), DATE(sts.created), DAY) < 90  THEN '0-3m'
          WHEN DATE_DIFF(DATE(ds.created), DATE(sts.created), DAY) < 180 THEN '3-6m'
          WHEN DATE_DIFF(DATE(ds.created), DATE(sts.created), DAY) < 365 THEN '6-12m'
          ELSE '12+m'
        END AS band,
        COUNT(*) AS cnt
      FROM production.degree_students ds
      JOIN production.students s      ON s.user_id      = ds.user_id
      JOIN production.st_students sts ON sts.student_id = s.id
      WHERE ds.has_activity_before_invitation = true
        AND DATE(ds.created) >= '{first}'
        AND DATE(ds.created) < '{next_first}'
      GROUP BY band
    ),
    admission AS (
      SELECT
        SUM(CASE WHEN is_pba_converted = true THEN 1 ELSE 0 END)  AS pba,
        SUM(CASE WHEN rpl_exemption_request_id IS NOT NULL
                      AND is_pba_converted = false THEN 1 ELSE 0 END) AS rpl,
        SUM(CASE WHEN is_pba_converted = false
                      AND rpl_exemption_request_id IS NULL THEN 1 ELSE 0 END) AS standard
      FROM production.degree_students
      WHERE DATE(created) >= '{first}'
        AND DATE(created) < '{next_first}'
    )
    SELECT
      (SELECT cnt FROM st_wlh)                              AS wlh_count,
      (SELECT cnt FROM st_age WHERE band = '0-3m'  )        AS age_0_3m,
      (SELECT cnt FROM st_age WHERE band = '3-6m'  )        AS age_3_6m,
      (SELECT cnt FROM st_age WHERE band = '6-12m' )        AS age_6_12m,
      (SELECT cnt FROM st_age WHERE band = '12+m'  )        AS age_12pm,
      (SELECT standard FROM admission)                      AS adm_standard,
      (SELECT pba      FROM admission)                      AS adm_pba,
      (SELECT rpl      FROM admission)                      AS adm_rpl
    """
    row = run_query(sql)
    if row.empty:
        return {}
    r = row.iloc[0]
    return {
        "wlh_count":    int(r.get("wlh_count")    or 0),
        "age_0_3m":     int(r.get("age_0_3m")     or 0),
        "age_3_6m":     int(r.get("age_3_6m")     or 0),
        "age_6_12m":    int(r.get("age_6_12m")    or 0),
        "age_12pm":     int(r.get("age_12pm")     or 0),
        "adm_standard": int(r.get("adm_standard") or 0),
        "adm_pba":      int(r.get("adm_pba")      or 0),
        "adm_rpl":      int(r.get("adm_rpl")      or 0),
    }


# ── Revenue Overview: April 2026 invoice data ────────────────────────────────

def _april_invoices() -> pd.DataFrame:
    """
    April 2026 revenue breakdown per college, sourced directly from invoices.

    Columns (indexed by college_id):
      invoice_name          – invoice reference (e.g. ABI-0002)
      invoice_status        – DRAFT / OPEN / PAID
      saas_fee              – monthly share of Q2 SAAS (custom_prices 'Historical…' ÷ 3)
      seat_fee              – monthly share of prepaid seats (÷ 3) + seat overage
      growth                – Airlock + PBA + Import + RPL + Exemption (from purchases × prices)
      additional_items      – custom_prices.item_type='charge' on monthly invoice
      total_cv              – saas + seat + growth + additional
      monthly_invoice_total – invoices.amount for the MONTHLY invoice (actual billed)

    SAAS fee = $0 if no 'Historical…' line exists in the Q2 quarterly yet.
    Columns are PENDING in the dashboard until the invoice is generated.
    """
    sql = """
    WITH

    -- Q2 2026 quarterly custom_prices per college (Apr–Jun)
    quarterly AS (
      SELECT
        i.college_id,
        MAX(i.status)                                             AS quarterly_status,
        SUM(CASE WHEN LOWER(cp.name) LIKE '%historical%'
                 THEN cp.price * cp.quantity ELSE 0 END) / 3.0   AS saas_fee,
        SUM(CASE WHEN LOWER(cp.name) LIKE '%prepaid seat%'
                 THEN cp.price * cp.quantity ELSE 0 END) / 3.0   AS seat_prepaid_monthly
      FROM production.custom_prices cp
      JOIN production.invoices i ON i.id = cp.invoice_id
      WHERE i.kind = 'QUARTERLY'
        AND i.billing_started = '2026-04-01T00:00:00Z'
      GROUP BY i.college_id
    ),

    -- April MONTHLY purchases (growth + seat overage) using actual prices
    monthly_purchases AS (
      SELECT
        i.college_id,
        i.name     AS invoice_name,
        i.status   AS invoice_status,
        i.amount   AS invoice_total,
        SUM(CASE WHEN svc.kind = 'SEAT_OVERAGE'
                 THEN COALESCE(pr.price, 0) ELSE 0 END)           AS seat_overage,
        SUM(CASE WHEN svc.kind IN ('AIRLOCK','PBA','IMPORT','RPL','EXEMPTION')
                 THEN COALESCE(pr.price, 0) ELSE 0 END)           AS growth
      FROM production.purchases p
      JOIN production.invoices i      ON i.id   = p.invoice_id
      LEFT JOIN production.products  prod ON prod.id = p.product_id
      LEFT JOIN production.services  svc  ON svc.id  = prod.service_id
      LEFT JOIN production.prices    pr   ON pr.id   = p.price_id
      WHERE i.kind = 'MONTHLY'
        AND i.billing_started = '2026-04-01T00:00:00Z'
        AND p.status = 'BILLABLE'
      GROUP BY i.college_id, i.name, i.status, i.amount
    ),

    -- April MONTHLY additional items (legacy rev-share carryovers, custom adjustments)
    additional AS (
      SELECT
        i.college_id,
        SUM(cp.price * cp.quantity)                               AS additional_items
      FROM production.custom_prices cp
      JOIN production.invoices i ON i.id = cp.invoice_id
      WHERE i.kind = 'MONTHLY'
        AND i.billing_started = '2026-04-01T00:00:00Z'
        AND cp.item_type = 'charge'
      GROUP BY i.college_id
    )

    SELECT
      mp.college_id,
      mp.invoice_name,
      mp.invoice_status,
      -- quarterly invoice status (covers SAAS Fee + Seat Fee columns)
      q.quarterly_status,
      ROUND(COALESCE(q.saas_fee,           0), 2)                 AS saas_fee,
      ROUND(COALESCE(q.seat_prepaid_monthly,0)
            + COALESCE(mp.seat_overage,    0), 2)                 AS seat_fee,
      -- monthly invoice status (covers Growth + Add. Items + Monthly Inv. columns)
      ROUND(COALESCE(mp.growth,            0), 2)                 AS growth,
      ROUND(COALESCE(ad.additional_items,  0), 2)                 AS additional_items,
      ROUND(
        COALESCE(q.saas_fee,           0)
        + COALESCE(q.seat_prepaid_monthly, 0)
        + COALESCE(mp.seat_overage,    0)
        + COALESCE(mp.growth,          0)
        + COALESCE(ad.additional_items,0), 2)                     AS total_cv,
      ROUND(mp.invoice_total, 2)                                  AS monthly_invoice_total
    FROM monthly_purchases mp
    LEFT JOIN quarterly  q  ON q.college_id  = mp.college_id
    LEFT JOIN additional ad ON ad.college_id = mp.college_id
    """
    return run_query(sql).set_index("college_id")


# ── Master loader ─────────────────────────────────────────────────────────────

def load_all_colleges(year: int, month: int) -> pd.DataFrame:
    """
    Runs all queries in parallel and merges into one DataFrame.
    ~17 queries fired simultaneously → loads in the time of the slowest single query.

    Two sets of M-1 / Y-1 activity data:
      _m1 / _y1       = full calendar month  → used for display columns + pauses/archives
      _m1_mtd / _y1_mtd = capped at today's day-of-month → used only for vs-MTD comparisons
    """
    py,  pm  = prev_month(year, month)
    yy1, my1 = year_ago(year, month)
    today = date.today()
    is_current = (year == today.year and month == today.month)
    # For current month: cap comparisons at today's day-of-month (apple-to-apple MTD)
    # For past months: cap at last day of selected month (full month vs equivalent period)
    today_day = today.day if is_current else calendar.monthrange(year, month)[1]

    tasks = {
        # ── Single activities scan covers this/m1/y1/m1_mtd/y1_mtd (was 5 queries) ──
        "activity":    (_all_activity_counts,    (year, month, py, pm, yy1, my1, today_day)),
        "colleges":    (get_colleges,            ()),
        "active_base": (_active_base,            (year, month)),
        "seat_rev":    (_seat_exp_revenue,       (year, month)),
        "st_new":      (_st_new_counts,          (year, month)),
        # ── Single degree_students scan for ST conversions + PBA (was 2 queries) ──
        "st_conv_pba": (_st_conv_pba_counts,     (year, month)),
        "rpl":         (_rpl_counts,             (year, month)),
        "oxford":      (_oxford_sbs,             (year, month)),
        "arch30d":     (_revsh_archived_30d,     (year, month)),
        "revsh_rev":   (_revsh_exp_revenue,      (year, month)),
        "pricing":     (get_pricing_config,      ()),
        "completion":  (_completion_rates,       (today_day,)),
        # NOTE: _rpl_admissions (degree_student_services) excluded — 80-120s BigQuery scan.
        # Total: 12 parallel queries (was 17)
    }

    results = {}
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(fn, *args): key for key, (fn, args) in tasks.items()}
        for future in as_completed(futures):
            key = futures[future]
            results[key] = future.result()

    colleges = results["colleges"].set_index("id")
    df = colleges.copy()

    # ── Join count/activity data first, then fillna(0) for numeric counts ──
    for key in ["activity", "active_base", "st_new", "st_conv_pba", "rpl", "oxford", "arch30d"]:
        df = df.join(results[key], how="left")

    df = df.fillna(0)

    # ── Join pricing + revenue queries AFTER fillna (preserve NaN) ────────────
    df = df.join(results["pricing"],    how="left")
    df = df.join(results["revsh_rev"],  how="left")
    df = df.join(results["seat_rev"],   how="left")
    df = df.join(results["completion"], how="left")

    # Active base only shown for seat-based
    df.loc[df["revenue_model"] != "SEAT_BASED", "active_base"] = None

    # Seat-based net: gains this month minus FULL prior-month exits
    df["net_additions"] = df["new_enrol"] - df["pauses_m1"] - df["archives_m1"]

    # Revenue-share net: new enrolments minus 30-day refund archives
    df["net_revsh"] = df["new_enrol"] - df["archived_30d"]

    return df.reset_index().rename(columns={"index": "college_id", "id": "college_id"})
