"""
===============================================================================
  SQL Query Builder
===============================================================================
Generates all SQL statements from the lender + step configuration.
Every public function returns a list[str] of SQL statements ready for
execute_statements().
===============================================================================
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from lending_report.config import (
    AMOUNT_DIVISOR,
    TABLE_PREFIX,
    get_active_lenders,
    get_active_steps,
    get_non_base_steps,
)

log = logging.getLogger(__name__)

# ═════════════════════════════════════════════════════════════════════════════
#  TIME WINDOWS
# ═════════════════════════════════════════════════════════════════════════════

def _first_of_month(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _prev_month_first(dt: datetime) -> datetime:
    first = _first_of_month(dt)
    return _first_of_month(first - timedelta(days=1))


def _same_day_prev_month(dt: datetime) -> datetime:
    """Return the same day-of-month in the previous month, clamped."""
    pm = _prev_month_first(dt)
    import calendar
    max_day = calendar.monthrange(pm.year, pm.month)[1]
    day = min(dt.day, max_day)
    return pm.replace(day=day, hour=dt.hour, minute=dt.minute,
                      second=dt.second, microsecond=dt.microsecond)


def get_hourly_windows(
    execution_time: datetime | None = None,
) -> Dict[str, Tuple[datetime, datetime]]:
    """
    Time windows for the **hourly** report.

    Returns dict  window_name → (start_inclusive, end_exclusive).
      today      : 00:00 today  → NOW
      yesterday  : 00:00 yesterday → same clock-time yesterday
      mtd        : 1st of month 00:00 → 00:00 today  (through T-1)
      lmtd       : 1st of prev month → same offset in prev month
    """
    now = execution_time or datetime.now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_start - timedelta(days=1)
    yesterday_same_time = now - timedelta(days=1)

    mtd_start = _first_of_month(now)
    mtd_end = today_start  # up to but not including today (T-1 inclusive)

    lmtd_start = _prev_month_first(now)
    lmtd_end = _same_day_prev_month(mtd_end)

    return {
        "today": (today_start, now),
        "yesterday": (yesterday_start, yesterday_same_time),
        "mtd": (mtd_start, mtd_end),
        "lmtd": (lmtd_start, lmtd_end),
    }


def get_daily_windows(
    execution_date: datetime | None = None,
) -> Dict[str, Tuple[datetime, datetime]]:
    """
    Time windows for the **daily** report.

    Returns dict  window_name → (start_inclusive, end_exclusive).
      t_minus_1  : full T-1 day
      t_minus_2  : full T-2 day
      mtd        : 1st of month → end of T-1
      lmtd       : 1st of prev month → same relative day prev month
    """
    today = (execution_date or datetime.now()).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    t1_start = today - timedelta(days=1)
    t1_end = today
    t2_start = today - timedelta(days=2)
    t2_end = t1_start

    mtd_start = _first_of_month(today)
    mtd_end = t1_end

    lmtd_start = _prev_month_first(today)
    lmtd_end = _same_day_prev_month(mtd_end)

    return {
        "t_minus_1": (t1_start, t1_end),
        "t_minus_2": (t2_start, t2_end),
        "mtd": (mtd_start, mtd_end),
        "lmtd": (lmtd_start, lmtd_end),
    }


def _widest_range(
    windows: Dict[str, Tuple[datetime, datetime]],
) -> Tuple[str, str]:
    """Return (earliest_start, latest_end) across all windows as strings."""
    starts = [w[0] for w in windows.values()]
    ends = [w[1] for w in windows.values()]
    fmt = "%Y-%m-%d %H:%M:%S"
    return min(starts).strftime(fmt), max(ends).strftime(fmt)


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ═════════════════════════════════════════════════════════════════════════════
#  TABLE NAMING
# ═════════════════════════════════════════════════════════════════════════════

def _tbl(report_prefix: str, lender_key: str, suffix: str) -> str:
    """
    Canonical table name.
    e.g.  _tbl('lrh', 'smfg', 'base')  →  'lrh_smfg_base'
    """
    return f"{report_prefix}_{lender_key}_{suffix}"


def _report_prefix(report_type: str) -> str:
    return f"{TABLE_PREFIX}{'h' if report_type == 'hourly' else 'd'}"


# ═════════════════════════════════════════════════════════════════════════════
#  BASE TABLE
# ═════════════════════════════════════════════════════════════════════════════
#
#  The base table covers the WIDEST date range needed (LMTD start → now).
#  Time-window filtering happens in the final summary aggregation.
#  Includes a NEW/OLD user_type flag.
#

_FLDG_BASE_SQL = """
    SELECT DISTINCT
        b.mbkloanid,
        b.createdat,
        b.memberid,
        CASE
            WHEN prev.memberid IS NULL THEN 'NEW'
            ELSE 'OLD'
        END AS user_type
    FROM lending.boost b
    LEFT JOIN (
        SELECT DISTINCT memberid
        FROM lending.boost
        WHERE kycflow = 'HYBRID_KYC_FLOW'
          AND createdat < '{new_user_cutoff}'
    ) prev ON b.memberid = prev.memberid
    WHERE b.kycflow = 'HYBRID_KYC_FLOW'
      AND b.createdat >= '{date_start}'
      AND b.createdat < '{date_end}'
"""


def build_base_table(
    report_type: str,
    lender_cfg: dict,
    windows: Dict[str, Tuple[datetime, datetime]],
) -> List[str]:
    """SQL statements to create the base table for one lender."""
    rp = _report_prefix(report_type)
    tbl = _tbl(rp, lender_cfg["key"], "base")
    date_start, date_end = _widest_range(windows)

    mtd_start = _fmt(_first_of_month(datetime.now()))

    if (
        lender_cfg.get("type") == "DISTRIBUTION"
        and "basic_details" in lender_cfg.get("custom_sql", {})
    ):
        select_sql = lender_cfg["custom_sql"]["basic_details"].format(
            date_start=date_start,
            date_end=date_end,
            new_user_cutoff=mtd_start,
        )
    else:
        select_sql = _FLDG_BASE_SQL.format(
            date_start=date_start,
            date_end=date_end,
            new_user_cutoff=mtd_start,
        )

    return [
        f"DROP TABLE IF EXISTS {tbl}",
        f"CREATE TABLE {tbl} AS {select_sql}",
        (
            f"ALTER TABLE {tbl} "
            f"ADD INDEX idx_mbk(mbkloanid), "
            f"ADD INDEX idx_dt(createdat), "
            f"ADD INDEX idx_ut(user_type)"
        ),
    ]


# ═════════════════════════════════════════════════════════════════════════════
#  STEP TABLES
# ═════════════════════════════════════════════════════════════════════════════

def _resolve_join_table(
    report_prefix: str,
    lender_key: str,
    step_cfg: dict,
) -> str:
    """Work out which table {join_table} should point to."""
    dep = step_cfg.get("depends_on") or "base"
    if dep == "base":
        return _tbl(report_prefix, lender_key, "base")
    return _tbl(report_prefix, lender_key, dep)


def build_step_table(
    report_type: str,
    lender_cfg: dict,
    step_cfg: dict,
) -> List[str]:
    """SQL statements to create one intermediate step table."""
    rp = _report_prefix(report_type)
    lk = lender_cfg["key"]
    tbl = _tbl(rp, lk, step_cfg["key"])
    base_tbl = _tbl(rp, lk, "base")
    join_tbl = _resolve_join_table(rp, lk, step_cfg)

    if (
        lender_cfg.get("type") == "DISTRIBUTION"
        and step_cfg["key"] in lender_cfg.get("custom_sql", {})
    ):
        raw_sql = lender_cfg["custom_sql"][step_cfg["key"]]
    else:
        raw_sql = step_cfg["fldg_sql"]

    select_sql = raw_sql.format(
        base_table=base_tbl,
        join_table=join_tbl,
        partner_id=lender_cfg["partner_id"],
        eligible_partners_csv=lender_cfg.get("eligible_partners_csv", ""),
    )

    return [
        f"DROP TABLE IF EXISTS {tbl}",
        f"CREATE TABLE {tbl} AS {select_sql}",
        f"ALTER TABLE {tbl} ADD INDEX idx_mbk(mbkloanid)",
    ]


# ═════════════════════════════════════════════════════════════════════════════
#  PER-LENDER SUMMARY  (raw counts per time window × user_type)
# ═════════════════════════════════════════════════════════════════════════════

def build_lender_summary(
    report_type: str,
    lender_cfg: dict,
    windows: Dict[str, Tuple[datetime, datetime]],
) -> List[str]:
    """
    Creates lr{h|d}_{lender}_summary with columns:
      time_window, user_type, <step_count columns>, <amount columns>
    One row per (time_window, user_type) combination.
    WITH ROLLUP gives us the 'ALL' user_type row automatically.
    """
    rp = _report_prefix(report_type)
    lk = lender_cfg["key"]
    summary_tbl = _tbl(rp, lk, "summary")
    base_tbl = _tbl(rp, lk, "base")

    steps = get_non_base_steps()

    # ── Build the SELECT column list (reused per window) ─────────────
    select_cols: list[str] = []
    select_cols.append("COUNT(DISTINCT b.mbkloanid) AS basic_details")
    for s in steps:
        alias = s["key"]
        select_cols.append(
            f"COUNT(DISTINCT {alias}.mbkloanid) AS {alias}"
        )
        if s.get("has_amount"):
            select_cols.append(
                f"ROUND(COALESCE(SUM({alias}.amount), 0) / {AMOUNT_DIVISOR}, 2)"
                f" AS {alias}_amt_cr"
            )

    select_block = ",\n        ".join(select_cols)

    # ── Build the JOIN list ──────────────────────────────────────────
    join_lines: list[str] = []
    for s in steps:
        alias = s["key"]
        step_tbl = _tbl(rp, lk, alias)
        join_lines.append(
            f"LEFT JOIN {step_tbl} {alias} ON {alias}.mbkloanid = b.mbkloanid"
        )
    join_block = "\n    ".join(join_lines)

    # ── One SELECT per window, combined with UNION ALL ───────────────
    union_parts: list[str] = []
    for wname, (wstart, wend) in windows.items():
        part = f"""
    SELECT
        '{wname}' AS time_window,
        COALESCE(b.user_type, 'ALL') AS user_type,
        {select_block}
    FROM {base_tbl} b
    {join_block}
    WHERE b.createdat >= '{_fmt(wstart)}'
      AND b.createdat <  '{_fmt(wend)}'
    GROUP BY b.user_type WITH ROLLUP"""
        union_parts.append(part)

    full_select = "\n    UNION ALL\n".join(union_parts)

    return [
        f"DROP TABLE IF EXISTS {summary_tbl}",
        f"CREATE TABLE {summary_tbl} AS {full_select}",
        (
            f"ALTER TABLE {summary_tbl} "
            f"ADD INDEX idx_tw(time_window), "
            f"ADD INDEX idx_ut(user_type)"
        ),
    ]


# ═════════════════════════════════════════════════════════════════════════════
#  PER-LENDER FUNNEL  (summary + row & column percentages)
# ═════════════════════════════════════════════════════════════════════════════

def build_lender_funnel(
    report_type: str,
    lender_cfg: dict,
) -> List[str]:
    """
    Creates lr{h|d}_{lender}_funnel from the summary table.
    Adds two percentage columns per step:
      • {step}_pct_tof   = step / basic_details × 100
      • {step}_pct_prev  = step / previous_step × 100
    """
    rp = _report_prefix(report_type)
    lk = lender_cfg["key"]
    funnel_tbl = _tbl(rp, lk, "funnel")
    summary_tbl = _tbl(rp, lk, "summary")

    steps = get_non_base_steps()

    cols: list[str] = ["s.time_window", "s.user_type", "s.basic_details"]
    prev_key = "basic_details"

    for s in steps:
        k = s["key"]
        cols.append(f"s.{k}")
        cols.append(
            f"ROUND(s.{k} * 100.0 / NULLIF(s.basic_details, 0), 2) AS {k}_pct_tof"
        )
        cols.append(
            f"ROUND(s.{k} * 100.0 / NULLIF(s.{prev_key}, 0), 2) AS {k}_pct_prev"
        )
        if s.get("has_amount"):
            cols.append(f"s.{k}_amt_cr")

        prev_key = k

    select_block = ",\n        ".join(cols)

    return [
        f"DROP TABLE IF EXISTS {funnel_tbl}",
        f"CREATE TABLE {funnel_tbl} AS SELECT {select_block} FROM {summary_tbl} s",
        (
            f"ALTER TABLE {funnel_tbl} "
            f"ADD INDEX idx_tw(time_window), "
            f"ADD INDEX idx_ut(user_type)"
        ),
    ]


# ═════════════════════════════════════════════════════════════════════════════
#  OVERALL SUMMARY  (sum across all active lenders)
# ═════════════════════════════════════════════════════════════════════════════

def build_overall_summary(report_type: str) -> List[str]:
    rp = _report_prefix(report_type)
    overall_tbl = f"{rp}_overall_summary"
    lenders = get_active_lenders()
    steps = get_non_base_steps()

    # columns to aggregate
    agg_cols = ["SUM(basic_details) AS basic_details"]
    for s in steps:
        agg_cols.append(f"SUM({s['key']}) AS {s['key']}")
        if s.get("has_amount"):
            agg_cols.append(f"SUM({s['key']}_amt_cr) AS {s['key']}_amt_cr")
    agg_block = ",\n        ".join(agg_cols)

    # UNION ALL of all lender summaries
    union_parts = [
        f"SELECT * FROM {_tbl(rp, l['key'], 'summary')}" for l in lenders
    ]
    union_block = "\n    UNION ALL\n    ".join(union_parts)

    select_sql = f"""
    SELECT
        time_window,
        user_type,
        {agg_block}
    FROM (
        {union_block}
    ) combined
    GROUP BY time_window, user_type
    """

    return [
        f"DROP TABLE IF EXISTS {overall_tbl}",
        f"CREATE TABLE {overall_tbl} AS {select_sql}",
        (
            f"ALTER TABLE {overall_tbl} "
            f"ADD INDEX idx_tw(time_window), "
            f"ADD INDEX idx_ut(user_type)"
        ),
    ]


# ═════════════════════════════════════════════════════════════════════════════
#  OVERALL FUNNEL  (overall summary + row/column percentages)
# ═════════════════════════════════════════════════════════════════════════════

def build_overall_funnel(report_type: str) -> List[str]:
    rp = _report_prefix(report_type)
    funnel_tbl = f"{rp}_overall_funnel"
    summary_tbl = f"{rp}_overall_summary"

    steps = get_non_base_steps()
    cols: list[str] = ["s.time_window", "s.user_type", "s.basic_details"]
    prev_key = "basic_details"

    for s in steps:
        k = s["key"]
        cols.append(f"s.{k}")
        cols.append(
            f"ROUND(s.{k} * 100.0 / NULLIF(s.basic_details, 0), 2) AS {k}_pct_tof"
        )
        cols.append(
            f"ROUND(s.{k} * 100.0 / NULLIF(s.{prev_key}, 0), 2) AS {k}_pct_prev"
        )
        if s.get("has_amount"):
            cols.append(f"s.{k}_amt_cr")
        prev_key = k

    select_block = ",\n        ".join(cols)

    return [
        f"DROP TABLE IF EXISTS {funnel_tbl}",
        f"CREATE TABLE {funnel_tbl} AS SELECT {select_block} FROM {summary_tbl} s",
        (
            f"ALTER TABLE {funnel_tbl} "
            f"ADD INDEX idx_tw(time_window), "
            f"ADD INDEX idx_ut(user_type)"
        ),
    ]


# ═════════════════════════════════════════════════════════════════════════════
#  UNIQUE USER TOF  (de-duplicated across lenders by memberid)
# ═════════════════════════════════════════════════════════════════════════════

def build_unique_tof(
    report_type: str,
    windows: Dict[str, Tuple[datetime, datetime]],
) -> List[str]:
    rp = _report_prefix(report_type)
    tof_tbl = f"{rp}_unique_tof"
    lenders = get_active_lenders()

    # UNION of all lender base tables (UNION removes duplicates by memberid)
    union_parts = [
        f"SELECT memberid, mbkloanid, createdat FROM {_tbl(rp, l['key'], 'base')}"
        for l in lenders
    ]
    union_block = "\n        UNION\n        ".join(union_parts)

    window_parts: list[str] = []
    for wname, (wstart, wend) in windows.items():
        window_parts.append(f"""
    SELECT
        '{wname}' AS time_window,
        COUNT(DISTINCT memberid)  AS unique_users,
        COUNT(DISTINCT mbkloanid) AS unique_applications
    FROM (
        {union_block}
    ) combined
    WHERE createdat >= '{_fmt(wstart)}'
      AND createdat <  '{_fmt(wend)}'""")

    full_select = "\n    UNION ALL\n".join(window_parts)

    return [
        f"DROP TABLE IF EXISTS {tof_tbl}",
        f"CREATE TABLE {tof_tbl} AS {full_select}",
        f"ALTER TABLE {tof_tbl} ADD INDEX idx_tw(time_window)",
    ]


# ═════════════════════════════════════════════════════════════════════════════
#  TOPLINE COMPARISON  (Today vs Yesterday / T-1 vs T-2 / MTD vs LMTD)
# ═════════════════════════════════════════════════════════════════════════════

def build_topline_comparison(report_type: str) -> List[str]:
    """
    Produces a table with rows:
      metric | period_a_label | period_a_value | period_b_label | period_b_value | pct_change

    For hourly: Today vs Yesterday, MTD vs LMTD
    For daily:  T-1 vs T-2, MTD vs LMTD
    """
    rp = _report_prefix(report_type)
    topline_tbl = f"{rp}_topline_comparison"
    overall_tbl = f"{rp}_overall_summary"

    if report_type == "hourly":
        pairs = [
            ("today", "Today", "yesterday", "Yesterday"),
            ("mtd", "MTD", "lmtd", "LMTD"),
        ]
    else:
        pairs = [
            ("t_minus_1", "T-1", "t_minus_2", "T-2"),
            ("mtd", "MTD", "lmtd", "LMTD"),
        ]

    metrics = [
        ("Drawdown Count", "drawdown"),
        ("Drawdown Amount (Cr)", "drawdown_amt_cr"),
        ("Sanction Count", "sanction"),
        ("Sanction Amount (Cr)", "sanction_amt_cr"),
        ("Offer Count", "offer"),
    ]

    union_parts: list[str] = []
    for pa_key, pa_label, pb_key, pb_label in pairs:
        for metric_label, col in metrics:
            union_parts.append(f"""
    SELECT
        '{pa_label} vs {pb_label}' AS comparison,
        '{metric_label}' AS metric,
        '{pa_label}' AS period_a_label,
        MAX(CASE WHEN time_window = '{pa_key}' THEN {col} END) AS period_a_value,
        '{pb_label}' AS period_b_label,
        MAX(CASE WHEN time_window = '{pb_key}' THEN {col} END) AS period_b_value,
        ROUND(
            (MAX(CASE WHEN time_window = '{pa_key}' THEN {col} END)
           - MAX(CASE WHEN time_window = '{pb_key}' THEN {col} END))
            * 100.0
            / NULLIF(MAX(CASE WHEN time_window = '{pb_key}' THEN {col} END), 0),
            2
        ) AS pct_change
    FROM {overall_tbl}
    WHERE user_type = 'ALL'
      AND time_window IN ('{pa_key}', '{pb_key}')""")

    full_select = "\n    UNION ALL\n".join(union_parts)

    return [
        f"DROP TABLE IF EXISTS {topline_tbl}",
        f"CREATE TABLE {topline_tbl} AS {full_select}",
    ]


# ═════════════════════════════════════════════════════════════════════════════
#  CLEANUP  (drop intermediate step tables; keep summaries)
# ═════════════════════════════════════════════════════════════════════════════

def build_cleanup(report_type: str) -> List[str]:
    """Drop per-lender intermediate step tables (base + step tables)."""
    rp = _report_prefix(report_type)
    stmts: list[str] = []
    for lender in get_active_lenders():
        lk = lender["key"]
        stmts.append(f"DROP TABLE IF EXISTS {_tbl(rp, lk, 'base')}")
        for step in get_non_base_steps():
            stmts.append(f"DROP TABLE IF EXISTS {_tbl(rp, lk, step['key'])}")
    return stmts


# ═════════════════════════════════════════════════════════════════════════════
#  PUBLIC ORCHESTRATOR
# ═════════════════════════════════════════════════════════════════════════════

def get_all_table_names(report_type: str) -> List[str]:
    """Return every table name this report run will create (for logging)."""
    rp = _report_prefix(report_type)
    lenders = get_active_lenders()
    names: list[str] = []
    for l in lenders:
        lk = l["key"]
        names.append(_tbl(rp, lk, "base"))
        for s in get_non_base_steps():
            names.append(_tbl(rp, lk, s["key"]))
        names.append(_tbl(rp, lk, "summary"))
        names.append(_tbl(rp, lk, "funnel"))
    names.append(f"{rp}_overall_summary")
    names.append(f"{rp}_overall_funnel")
    names.append(f"{rp}_unique_tof")
    names.append(f"{rp}_topline_comparison")
    return names
