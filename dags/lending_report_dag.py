"""
===============================================================================
  LENDING REPORT DAG  (Single DAG — Hourly + Daily)
===============================================================================

  WHAT IT DOES
  ────────────
  Runs every hour.  Creates a common base of intermediate tables on MySQL,
  then builds all summary grids from them.

  Two mailers fire from this same DAG:
    • Hourly mailer — Today, Yesterday, MTD, LMTD
    • Daily  mailer — T-1, T-2, MTD, LMTD  (fires once at DAILY_MAIL_HOUR)

  HOW TO ADD A NEW FLDG LENDER
  ────────────────────────────
  1.  Add one row to LENDER_ROWS  →  (partner_id, 'Name', 'FLDG')
  2.  Done.  All queries auto-include the new lender.

  HOW TO ADD A DISTRIBUTION LENDER
  ────────────────────────────────
  1.  Add one row to LENDER_ROWS  →  (partner_id, 'Name', 'DISTRIBUTION')
  2.  In create_base()   — append an INSERT for their applications table.
  3.  In create_journey() — append an INSERT for their journey table.
  4.  Repeat for any step whose source table differs.
      Commented-out examples are provided inside each function.

  HOW TO ADD / REMOVE / REORDER FUNNEL STEPS
  ───────────────────────────────────────────
  Each step is a separate function (create_address, create_journey, …).
  • To remove a step, comment out its task in the DAG section.
  • To add a step, write a new create_xxx() function and wire it in.
  • To reorder, change the task dependency chain at the bottom.

  OPEN vs CLOSED FUNNEL
  ─────────────────────
  By default every step table joins to lr_base (open funnel — independent).
  To make step B depend on step A (closed funnel), change the JOIN in
  step B's SQL from  lr_base  to  lr_<step_A>.

  SCHEMA
  ──────
  All temporary tables are created in  mobikwik_schema  (configurable via
  SCHEMA constant below).  Tables are dropped and recreated each run so
  every query stays well under the 20-minute MySQL limit.

  ═════════════════════════════════════════════════════════════════════════
  SAMPLE OUTPUT — lr_lenderwise  (what you will see in MySQL)
  ═════════════════════════════════════════════════════════════════════════

  time_window | lender    | user_type | basic_details | address | lpa_run | … | offer | … | sanction | sanction_amt_cr | drawdown | drawdown_amt_cr
  ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
  today       | SMFG      | NEW       | 3 500         | 2 800   |    420  |   |   180 |   |       60 |            1.20 |       30 |            0.50
  today       | SMFG      | OLD       | 6 500         | 5 200   |    780  |   |   320 |   |      140 |            2.80 |       70 |            1.20
  today       | SMFG      | ALL       |10 000         | 8 000   |  1 200  |   |   500 |   |      200 |            4.00 |      100 |            1.70
  today       | Fullerton | ALL       |10 000         | 8 000   |  1 000  |   |   400 |   |      150 |            3.00 |       80 |            1.30
  yesterday   | SMFG      | ALL       | 9 500         | 7 500   |  1 100  |   |   470 |   |      190 |            3.80 |       95 |            1.60
  …           | …         | …         | …             | …       |    …    |   |   …   |   |      …   |            …    |       …  |            …
  mtd         | SMFG      | ALL       |250 000        |200 000  | 30 000  |   |12 500 |   |    5 000 |          100.00 |    2 500 |           42.50
  lmtd        | SMFG      | ALL       |240 000        |190 000  | 28 000  |   |11 800 |   |    4 700 |           94.00 |    2 350 |           40.00

  ═════════════════════════════════════════════════════════════════════════
  SAMPLE OUTPUT — lr_overall  (sum of all lenders)
  ═════════════════════════════════════════════════════════════════════════

  time_window | user_type | basic_details | address | lpa_run | … | sanction_amt_cr | drawdown_amt_cr
  ────────────────────────────────────────────────────────────────────────────────────────────────────
  today       | ALL       |20 000         |16 000   |  2 200  |   |            7.00 |            3.00
  yesterday   | ALL       |19 000         |15 000   |  2 100  |   |            6.80 |            2.90
  mtd         | ALL       |500 000        |400 000  | 58 000  |   |          194.00 |           82.50

  ═════════════════════════════════════════════════════════════════════════
  SAMPLE OUTPUT — lr_funnel  (percentages)
  ═════════════════════════════════════════════════════════════════════════

  time_window | lender | user_type | step_name      | step_count | pct_of_tof | pct_of_prev
  ─────────────────────────────────────────────────────────────────────────────────────────────
  today       | SMFG   | ALL       | Basic Details  |     10 000 |     100.00 |      100.00
  today       | SMFG   | ALL       | Address        |      8 000 |      80.00 |       80.00
  today       | SMFG   | ALL       | LPA Run        |      1 200 |      12.00 |       15.00
  today       | SMFG   | ALL       | LPA Pass       |      1 100 |      11.00 |       91.67
  …

  ═════════════════════════════════════════════════════════════════════════
  SAMPLE OUTPUT — lr_unique_tof
  ═════════════════════════════════════════════════════════════════════════

  time_window | unique_users | unique_applications
  ─────────────────────────────────────────────────
  today       |        9 200 |              10 000
  yesterday   |        8 800 |               9 500

  ═════════════════════════════════════════════════════════════════════════
  SAMPLE OUTPUT — lr_topline
  ═════════════════════════════════════════════════════════════════════════

  comparison         | metric               | period_a | value_a | period_b  | value_b | pct_change
  ───────────────────────────────────────────────────────────────────────────────────────────────────
  Today vs Yesterday | Drawdown Amount (Cr)  | Today    |    3.00 | Yesterday |    2.90 |      +3.45
  Today vs Yesterday | Sanction Amount (Cr)  | Today    |    7.00 | Yesterday |    6.80 |      +2.94
  Today vs Yesterday | Offer Count           | Today    |     900 | Yesterday |     870 |      +3.45
  MTD vs LMTD        | Drawdown Amount (Cr)  | MTD      |   82.50 | LMTD      |   78.00 |      +5.77
  …
===============================================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import calendar
import logging
import pymysql
import sqlalchemy as sa

log = logging.getLogger(__name__)


###########################################################################
#  CONFIGURATION — edit this section only
###########################################################################

SCHEMA = "mobikwik_schema"

# Add / remove lenders here.  Format: (partner_id, 'Display Name', 'FLDG' or 'DISTRIBUTION')
LENDER_ROWS = [
    (4,  "SMFG",      "FLDG"),
    (7,  "Fullerton", "FLDG"),
    # (99, "NewLender", "FLDG"),          ← just add a row for a new FLDG lender
    # (50, "DistPartner", "DISTRIBUTION"),← distribution lenders need INSERT blocks too
]

DAILY_MAIL_HOUR = 7   # send daily mailer when the DAG runs at this hour (UTC)
AMOUNT_DIVISOR = 1e7  # divide amounts by this to get ₹ Crores


###########################################################################
#  DATABASE
###########################################################################

def get_engine():
    pymysql.install_as_MySQLdb()
    return sa.create_engine(
        "mysql+pymysql://analytics:vsn%400pl3TYujk23(o"
        "@data-analytics-mysql-prod.mbkinternal.in:3308/mobinew",
        pool_recycle=1800,
        pool_pre_ping=True,
    )


def run_sql(statements):
    """Execute a list of SQL strings inside one transaction."""
    engine = get_engine()
    with engine.begin() as conn:
        for s in statements:
            s = s.strip()
            if s:
                log.info("SQL ▸ %s", s[:200].replace("\n", " "))
                conn.execute(sa.text(s))


###########################################################################
#  TIME WINDOWS
###########################################################################

def _fmt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def compute_windows():
    """
    Return (windows_dict, widest_start, widest_end).

    windows_dict keys
    ─────────────────
    Hourly mailer  : today, yesterday, mtd, lmtd
    Daily  mailer  : t_minus_1, t_minus_2, mtd, lmtd

    Each value is a tuple (start_inclusive, end_exclusive) as strings.
    """
    now       = datetime.now()
    today_00  = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yest_00   = today_00 - timedelta(days=1)
    yest_now  = now       - timedelta(days=1)          # same clock-time yesterday
    db4y_00   = today_00 - timedelta(days=2)

    mtd_start = today_00.replace(day=1)
    mtd_end   = today_00                               # through T-1 complete

    lmtd_start   = (mtd_start - timedelta(days=1)).replace(day=1)
    t1_day       = (today_00 - timedelta(days=1)).day
    max_prev_day = calendar.monthrange(lmtd_start.year, lmtd_start.month)[1]
    lmtd_end     = lmtd_start.replace(day=min(t1_day, max_prev_day)) + timedelta(days=1)

    new_user_cutoff = _fmt(mtd_start)   # users before this date are "OLD"

    windows = {
        "today":     (_fmt(today_00), _fmt(now)),
        "yesterday": (_fmt(yest_00),  _fmt(yest_now)),
        "t_minus_1": (_fmt(yest_00),  _fmt(today_00)),
        "t_minus_2": (_fmt(db4y_00),  _fmt(yest_00)),
        "mtd":       (_fmt(mtd_start),  _fmt(mtd_end)),
        "lmtd":      (_fmt(lmtd_start), _fmt(lmtd_end)),
    }

    widest_start = _fmt(lmtd_start)
    widest_end   = _fmt(now)

    return windows, widest_start, widest_end, new_user_cutoff


###########################################################################
#  STEP 0 — LENDER REFERENCE TABLE
###########################################################################

def create_lenders(**ctx):
    stmts = [
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_lenders",
        f"""CREATE TABLE {SCHEMA}.lr_lenders (
                lender_id   INT          NOT NULL,
                lender_name VARCHAR(50)  NOT NULL,
                lender_type VARCHAR(20)  NOT NULL,
                PRIMARY KEY (lender_id)
            )""",
    ]
    for lid, lname, ltype in LENDER_ROWS:
        stmts.append(
            f"INSERT INTO {SCHEMA}.lr_lenders VALUES ({lid}, '{lname}', '{ltype}')"
        )
    run_sql(stmts)


###########################################################################
#  STEP 1 — BASE TABLE  (all HYBRID_KYC_FLOW users in widest date range)
###########################################################################

def create_base(**ctx):
    w, ws, we, cutoff = compute_windows()
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_base",
        f"""CREATE TABLE {SCHEMA}.lr_base AS
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
                  AND createdat < '{cutoff}'
            ) prev ON b.memberid = prev.memberid
            WHERE b.kycflow = 'HYBRID_KYC_FLOW'
              AND b.createdat >= '{ws}'
              AND b.createdat <  '{we}'
        """,
        f"""ALTER TABLE {SCHEMA}.lr_base
            ADD INDEX idx_mbk (mbkloanid),
            ADD INDEX idx_dt  (createdat),
            ADD INDEX idx_ut  (user_type)""",
    ])

    # ── DISTRIBUTION LENDER EXAMPLE ──
    # Uncomment and fill in for each distribution lender whose base comes
    # from a different table.
    #
    # run_sql([f"""
    #     INSERT INTO {SCHEMA}.lr_base
    #     SELECT DISTINCT
    #         app_id   AS mbkloanid,
    #         created  AS createdat,
    #         user_id  AS memberid,
    #         'NEW'    AS user_type
    #     FROM dist_schema.applications
    #     WHERE created >= '{ws}' AND created < '{we}'
    # """])


###########################################################################
#  STEP 2 — ADDRESS
###########################################################################

def create_address(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_address",
        f"""CREATE TABLE {SCHEMA}.lr_address AS
            SELECT DISTINCT mpd.mbkloanid
            FROM lending.memberprofiledetails mpd
            INNER JOIN {SCHEMA}.lr_base b ON mpd.mbkloanid = b.mbkloanid
            WHERE mpd.PermanentPincode IS NOT NULL
              AND mpd.PermanentPincode > 0
        """,
        f"ALTER TABLE {SCHEMA}.lr_address ADD INDEX idx_mbk (mbkloanid)",
    ])


###########################################################################
#  STEP 3 — JOURNEY  (one row per user × lender)
###########################################################################

def create_journey(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_journey",
        f"""CREATE TABLE {SCHEMA}.lr_journey AS
            SELECT
                u.mbk_loan_id AS mbkloanid,
                l.lender_id,
                MAX(u.user_journey_status_stage = 'PreLenderSanityChecks')   AS lpa_run,
                MAX(u.user_journey_status_stage = 'PostLenderSanityChecks')  AS lpa_pass,
                MAX(u.user_journey_status_stage = 'PreBreFraudRulesSuccess') AS pre_bre,
                MAX(u.user_journey_status_stage = 'BureauPullSuccess')       AS bureau_pull,
                MAX(u.user_journey_status_stage = 'BreSuccess')              AS bre_success,
                MAX(u.user_journey_status_stage = 'PostBreFraudRuleSuccess') AS post_bre,
                MAX(u.user_journey_status_stage = 'PanKycValidationDone')    AS pan_kyc
            FROM lending.user_journey_status u
            INNER JOIN {SCHEMA}.lr_base   b ON u.mbk_loan_id = b.mbkloanid
            INNER JOIN {SCHEMA}.lr_lenders l
                ON FIND_IN_SET(l.lender_id, u.eligible_partners) > 0
                AND l.lender_type = 'FLDG'
            GROUP BY u.mbk_loan_id, l.lender_id
        """,
        f"""ALTER TABLE {SCHEMA}.lr_journey
            ADD INDEX idx_mbk (mbkloanid),
            ADD INDEX idx_lid (lender_id)""",
    ])

    # ── DISTRIBUTION LENDER JOURNEY ──
    # run_sql([f"""
    #     INSERT INTO {SCHEMA}.lr_journey
    #     SELECT
    #         app_id AS mbkloanid,
    #         50     AS lender_id,
    #         MAX(stage = 'LPA_RUN')   AS lpa_run,
    #         ...
    #     FROM dist_schema.journey
    #     INNER JOIN {SCHEMA}.lr_base b ON app_id = b.mbkloanid
    #     GROUP BY app_id
    # """])


###########################################################################
#  STEP 4 — LENDER DETAILS
###########################################################################

def create_lender_det(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_lender_det",
        f"""CREATE TABLE {SCHEMA}.lr_lender_det AS
            SELECT DISTINCT
                lad.mbk_loan_id AS mbkloanid,
                lad.lending_partner AS lender_id
            FROM lending.lender_additional_details lad
            INNER JOIN {SCHEMA}.lr_base    b ON lad.mbk_loan_id = b.mbkloanid
            INNER JOIN {SCHEMA}.lr_lenders l ON lad.lending_partner = l.lender_id
        """,
        f"""ALTER TABLE {SCHEMA}.lr_lender_det
            ADD INDEX idx_mbk (mbkloanid),
            ADD INDEX idx_lid (lender_id)""",
    ])


###########################################################################
#  STEP 5 — OFFER
###########################################################################

def create_offer(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_offer",
        f"""CREATE TABLE {SCHEMA}.lr_offer AS
            SELECT DISTINCT
                c.mbkloanid,
                c.lendingpartnerid AS lender_id
            FROM lending.creditline c
            INNER JOIN {SCHEMA}.lr_base    b ON c.mbkloanid = b.mbkloanid
            INNER JOIN {SCHEMA}.lr_lenders l ON c.lendingpartnerid = l.lender_id
        """,
        f"""ALTER TABLE {SCHEMA}.lr_offer
            ADD INDEX idx_mbk (mbkloanid),
            ADD INDEX idx_lid (lender_id)""",
    ])


###########################################################################
#  STEP 6 — OFFER ACCEPTED  (joined through offer for lender specificity)
###########################################################################

def create_offer_accept(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_offer_accept",
        f"""CREATE TABLE {SCHEMA}.lr_offer_accept AS
            SELECT DISTINCT
                ua.mbkloanid,
                o.lender_id
            FROM lending.useracceptancedetails ua
            INNER JOIN {SCHEMA}.lr_offer o ON ua.mbkloanid = o.mbkloanid
            WHERE ua.stage = 'SAVE_OFFER'
        """,
        f"""ALTER TABLE {SCHEMA}.lr_offer_accept
            ADD INDEX idx_mbk (mbkloanid),
            ADD INDEX idx_lid (lender_id)""",
    ])


###########################################################################
#  STEP 7 — KYC
###########################################################################

def create_kyc(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_kyc",
        f"""CREATE TABLE {SCHEMA}.lr_kyc AS
            SELECT DISTINCT
                uki.mbk_loan_id AS mbkloanid,
                uki.lending_partner AS lender_id
            FROM lending.user_kyc_info uki
            INNER JOIN {SCHEMA}.lr_base    b ON uki.mbk_loan_id = b.mbkloanid
            INNER JOIN {SCHEMA}.lr_lenders l ON uki.lending_partner = l.lender_id
        """,
        f"""ALTER TABLE {SCHEMA}.lr_kyc
            ADD INDEX idx_mbk (mbkloanid),
            ADD INDEX idx_lid (lender_id)""",
    ])


###########################################################################
#  STEP 8 — BANK  (user-level, no lender column)
###########################################################################

def create_bank(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_bank",
        f"""CREATE TABLE {SCHEMA}.lr_bank AS
            SELECT DISTINCT bv.mbkloanid
            FROM lending.bankverification bv
            INNER JOIN {SCHEMA}.lr_base b ON bv.mbkloanid = b.mbkloanid
            WHERE bv.bankverified = 1
        """,
        f"ALTER TABLE {SCHEMA}.lr_bank ADD INDEX idx_mbk (mbkloanid)",
    ])


###########################################################################
#  STEP 9 — NACH  (user-level)
###########################################################################

def create_nach(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_nach",
        f"""CREATE TABLE {SCHEMA}.lr_nach AS
            SELECT DISTINCT nr.mbkloanid
            FROM lending.NachRegistration nr
            INNER JOIN {SCHEMA}.lr_base b ON nr.mbkloanid = b.mbkloanid
            WHERE nr.status IN ('eMandateSuccess', 'pNachSuccess')
        """,
        f"ALTER TABLE {SCHEMA}.lr_nach ADD INDEX idx_mbk (mbkloanid)",
    ])


###########################################################################
#  STEP 10 — SANCTION  (lender-level, with amount)
###########################################################################

def create_sanction(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_sanction",
        f"""CREATE TABLE {SCHEMA}.lr_sanction AS
            SELECT
                c.mbkloanid,
                c.lendingpartnerid AS lender_id,
                SUM(c.sanctionedlineamount) AS amount
            FROM lending.creditline c
            INNER JOIN {SCHEMA}.lr_base    b ON c.mbkloanid = b.mbkloanid
            INNER JOIN {SCHEMA}.lr_lenders l ON c.lendingpartnerid = l.lender_id
            WHERE c.status LIKE '%,11%'
            GROUP BY c.mbkloanid, c.lendingpartnerid
        """,
        f"""ALTER TABLE {SCHEMA}.lr_sanction
            ADD INDEX idx_mbk (mbkloanid),
            ADD INDEX idx_lid (lender_id)""",
    ])


###########################################################################
#  STEP 11 — DRAWDOWN  (user-level, with amount)
###########################################################################

def create_drawdown(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_drawdown",
        f"""CREATE TABLE {SCHEMA}.lr_drawdown AS
            SELECT
                d.mbkloanid,
                SUM(d.drawamount) AS amount
            FROM lending.drawdown d
            INNER JOIN {SCHEMA}.lr_base b ON d.mbkloanid = b.mbkloanid
            WHERE d.drawdownstatus IN (4, 17)
            GROUP BY d.mbkloanid
        """,
        f"ALTER TABLE {SCHEMA}.lr_drawdown ADD INDEX idx_mbk (mbkloanid)",
    ])


###########################################################################
#  STEP 12 — LENDERWISE SUMMARY
###########################################################################
#
#  One row per (time_window, lender, user_type).
#
#  For each lender we build a SELECT that:
#    • starts from lr_base  (the full user pool)
#    • LEFT JOINs every step table
#    • lender-aware tables are filtered to this lender via subqueries
#      so every join stays 1:1 on mbkloanid (no fan-out)
#    • GROUP BY user_type WITH ROLLUP gives NEW, OLD, ALL rows
#
#  All lenders × all windows are combined with UNION ALL and written
#  as individual INSERT statements to stay within the 20-min query limit.
#
###########################################################################

_LENDERWISE_SELECT = """
    SELECT
        '{{wname}}'  AS time_window,
        '{{lname}}'  AS lender,
        COALESCE(b.user_type, 'ALL') AS user_type,

        COUNT(DISTINCT b.mbkloanid)                                              AS basic_details,
        COUNT(DISTINCT a.mbkloanid)                                              AS address,

        COUNT(DISTINCT CASE WHEN j.lpa_run     = 1 THEN j.mbkloanid END)        AS lpa_run,
        COUNT(DISTINCT CASE WHEN j.lpa_pass    = 1 THEN j.mbkloanid END)        AS lpa_pass,
        COUNT(DISTINCT CASE WHEN j.pre_bre     = 1 THEN j.mbkloanid END)        AS pre_bre,
        COUNT(DISTINCT CASE WHEN j.bureau_pull = 1 THEN j.mbkloanid END)        AS bureau_pull,
        COUNT(DISTINCT CASE WHEN j.bre_success = 1 THEN j.mbkloanid END)        AS bre_success,
        COUNT(DISTINCT CASE WHEN j.post_bre    = 1 THEN j.mbkloanid END)        AS post_bre,
        COUNT(DISTINCT CASE WHEN j.pan_kyc     = 1 THEN j.mbkloanid END)        AS pan_kyc,

        COUNT(DISTINCT ld.mbkloanid)                                             AS lender_details,
        COUNT(DISTINCT o.mbkloanid)                                              AS offer,
        COUNT(DISTINCT oa.mbkloanid)                                             AS offer_accepted,
        COUNT(DISTINCT k.mbkloanid)                                              AS kyc,
        COUNT(DISTINCT bk.mbkloanid)                                             AS bank,
        COUNT(DISTINCT n.mbkloanid)                                              AS nach,

        COUNT(DISTINCT s.mbkloanid)                                              AS sanction,
        ROUND(COALESCE(SUM(s.amount), 0) / {amt_div}, 2)                        AS sanction_amt_cr,
        COUNT(DISTINCT d.mbkloanid)                                              AS drawdown,
        ROUND(COALESCE(SUM(d.amount), 0) / {amt_div}, 2)                        AS drawdown_amt_cr

    FROM {schema}.lr_base b

    LEFT JOIN {schema}.lr_address a
        ON a.mbkloanid = b.mbkloanid

    LEFT JOIN (SELECT mbkloanid, lpa_run, lpa_pass, pre_bre, bureau_pull,
                      bre_success, post_bre, pan_kyc
               FROM {schema}.lr_journey WHERE lender_id = {{lid}}) j
        ON j.mbkloanid = b.mbkloanid

    LEFT JOIN (SELECT DISTINCT mbkloanid
               FROM {schema}.lr_lender_det WHERE lender_id = {{lid}}) ld
        ON ld.mbkloanid = b.mbkloanid

    LEFT JOIN (SELECT DISTINCT mbkloanid
               FROM {schema}.lr_offer WHERE lender_id = {{lid}}) o
        ON o.mbkloanid = b.mbkloanid

    LEFT JOIN (SELECT DISTINCT mbkloanid
               FROM {schema}.lr_offer_accept WHERE lender_id = {{lid}}) oa
        ON oa.mbkloanid = b.mbkloanid

    LEFT JOIN (SELECT DISTINCT mbkloanid
               FROM {schema}.lr_kyc WHERE lender_id = {{lid}}) k
        ON k.mbkloanid = b.mbkloanid

    LEFT JOIN {schema}.lr_bank bk
        ON bk.mbkloanid = b.mbkloanid

    LEFT JOIN {schema}.lr_nach n
        ON n.mbkloanid = b.mbkloanid

    LEFT JOIN (SELECT mbkloanid, SUM(amount) AS amount
               FROM {schema}.lr_sanction WHERE lender_id = {{lid}}
               GROUP BY mbkloanid) s
        ON s.mbkloanid = b.mbkloanid

    LEFT JOIN {schema}.lr_drawdown d
        ON d.mbkloanid = b.mbkloanid

    WHERE b.createdat >= '{{wstart}}'
      AND b.createdat <  '{{wend}}'
    GROUP BY b.user_type WITH ROLLUP
""".format(schema=SCHEMA, amt_div=int(AMOUNT_DIVISOR))


def create_lenderwise(**ctx):
    windows, _, _, _ = compute_windows()

    stmts = [
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_lenderwise",
        f"""CREATE TABLE {SCHEMA}.lr_lenderwise (
                time_window      VARCHAR(20),
                lender           VARCHAR(50),
                user_type        VARCHAR(10),
                basic_details    BIGINT DEFAULT 0,
                address          BIGINT DEFAULT 0,
                lpa_run          BIGINT DEFAULT 0,
                lpa_pass         BIGINT DEFAULT 0,
                pre_bre          BIGINT DEFAULT 0,
                bureau_pull      BIGINT DEFAULT 0,
                bre_success      BIGINT DEFAULT 0,
                post_bre         BIGINT DEFAULT 0,
                pan_kyc          BIGINT DEFAULT 0,
                lender_details   BIGINT DEFAULT 0,
                offer            BIGINT DEFAULT 0,
                offer_accepted   BIGINT DEFAULT 0,
                kyc              BIGINT DEFAULT 0,
                bank             BIGINT DEFAULT 0,
                nach             BIGINT DEFAULT 0,
                sanction         BIGINT DEFAULT 0,
                sanction_amt_cr  DECIMAL(14,2) DEFAULT 0,
                drawdown         BIGINT DEFAULT 0,
                drawdown_amt_cr  DECIMAL(14,2) DEFAULT 0,
                INDEX idx_tw (time_window),
                INDEX idx_ln (lender(50)),
                INDEX idx_ut (user_type)
            )""",
    ]

    for lid, lname, _ in LENDER_ROWS:
        for wname, (wstart, wend) in windows.items():
            insert_sql = _LENDERWISE_SELECT.format(
                wname=wname, lname=lname, lid=lid, wstart=wstart, wend=wend,
            )
            stmts.append(f"INSERT INTO {SCHEMA}.lr_lenderwise {insert_sql}")

    run_sql(stmts)


###########################################################################
#  STEP 13 — OVERALL SUMMARY  (sum of all lenders)
###########################################################################

def create_overall(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_overall",
        f"""CREATE TABLE {SCHEMA}.lr_overall AS
            SELECT
                time_window,
                user_type,
                SUM(basic_details)   AS basic_details,
                SUM(address)         AS address,
                SUM(lpa_run)         AS lpa_run,
                SUM(lpa_pass)        AS lpa_pass,
                SUM(pre_bre)         AS pre_bre,
                SUM(bureau_pull)     AS bureau_pull,
                SUM(bre_success)     AS bre_success,
                SUM(post_bre)        AS post_bre,
                SUM(pan_kyc)         AS pan_kyc,
                SUM(lender_details)  AS lender_details,
                SUM(offer)           AS offer,
                SUM(offer_accepted)  AS offer_accepted,
                SUM(kyc)             AS kyc,
                SUM(bank)            AS bank,
                SUM(nach)            AS nach,
                SUM(sanction)        AS sanction,
                SUM(sanction_amt_cr) AS sanction_amt_cr,
                SUM(drawdown)        AS drawdown,
                SUM(drawdown_amt_cr) AS drawdown_amt_cr
            FROM {SCHEMA}.lr_lenderwise
            GROUP BY time_window, user_type
        """,
        f"""ALTER TABLE {SCHEMA}.lr_overall
            ADD INDEX idx_tw (time_window),
            ADD INDEX idx_ut (user_type)""",
    ])


###########################################################################
#  STEP 14 — FUNNEL  (row + column percentages, pivoted to rows)
###########################################################################
#
#  Produces one row per (time_window, lender, user_type, step_name) with
#  columns:  step_count, pct_of_tof (% of basic_details),
#            pct_of_prev (% of preceding step).
#

_STEP_ORDER = [
    ("basic_details",  "Basic Details",  "basic_details"),
    ("address",        "Address",        "basic_details"),
    ("lpa_run",        "LPA Run",        "address"),
    ("lpa_pass",       "LPA Pass",       "lpa_run"),
    ("pre_bre",        "Pre BRE",        "lpa_pass"),
    ("bureau_pull",    "Bureau Pull",    "pre_bre"),
    ("bre_success",    "BRE Success",    "bureau_pull"),
    ("post_bre",       "Post BRE",       "bre_success"),
    ("pan_kyc",        "PAN Validation", "post_bre"),
    ("lender_details", "Lender Details", "pan_kyc"),
    ("offer",          "Offer",          "lender_details"),
    ("offer_accepted", "Offer Accepted", "offer"),
    ("kyc",            "KYC",            "offer_accepted"),
    ("bank",           "Bank",           "kyc"),
    ("nach",           "NACH",           "bank"),
    ("sanction",       "Sanction",       "nach"),
    ("drawdown",       "Drawdown",       "sanction"),
]


def create_funnel(**ctx):
    unions = []
    for col, label, prev_col in _STEP_ORDER:
        unions.append(f"""
            SELECT
                time_window,
                lender,
                user_type,
                '{label}'  AS step_name,
                {col}      AS step_count,
                ROUND({col} * 100.0 / NULLIF(basic_details, 0), 2)  AS pct_of_tof,
                ROUND({col} * 100.0 / NULLIF({prev_col}, 0), 2)     AS pct_of_prev
            FROM {SCHEMA}.lr_lenderwise
        """)

    full_select = " UNION ALL ".join(unions)

    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_funnel",
        f"CREATE TABLE {SCHEMA}.lr_funnel AS {full_select}",
        f"""ALTER TABLE {SCHEMA}.lr_funnel
            ADD INDEX idx_tw (time_window),
            ADD INDEX idx_ln (lender(50))""",
    ])


###########################################################################
#  STEP 15 — UNIQUE USER TOF  (de-duplicated by memberid)
###########################################################################

def create_unique_tof(**ctx):
    windows, _, _, _ = compute_windows()

    unions = []
    for wname, (wstart, wend) in windows.items():
        unions.append(f"""
            SELECT
                '{wname}' AS time_window,
                COUNT(DISTINCT memberid)  AS unique_users,
                COUNT(DISTINCT mbkloanid) AS unique_applications
            FROM {SCHEMA}.lr_base
            WHERE createdat >= '{wstart}' AND createdat < '{wend}'
        """)

    full_select = " UNION ALL ".join(unions)

    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_unique_tof",
        f"CREATE TABLE {SCHEMA}.lr_unique_tof AS {full_select}",
        f"ALTER TABLE {SCHEMA}.lr_unique_tof ADD INDEX idx_tw (time_window)",
    ])


###########################################################################
#  STEP 16 — TOPLINE COMPARISON  (Today vs Yesterday, MTD vs LMTD, etc.)
###########################################################################

def create_topline(**ctx):
    pairs = [
        ("today",     "Today",  "yesterday", "Yesterday"),
        ("t_minus_1", "T-1",    "t_minus_2", "T-2"),
        ("mtd",       "MTD",    "lmtd",      "LMTD"),
    ]
    metrics = [
        ("Drawdown Count",        "drawdown"),
        ("Drawdown Amount (Cr)",  "drawdown_amt_cr"),
        ("Sanction Count",        "sanction"),
        ("Sanction Amount (Cr)",  "sanction_amt_cr"),
        ("Offer Count",           "offer"),
    ]

    unions = []
    for pa_key, pa_label, pb_key, pb_label in pairs:
        for metric_label, col in metrics:
            unions.append(f"""
                SELECT
                    '{pa_label} vs {pb_label}'  AS comparison,
                    '{metric_label}'            AS metric,
                    '{pa_label}'                AS period_a,
                    MAX(CASE WHEN time_window = '{pa_key}' THEN {col} END) AS value_a,
                    '{pb_label}'                AS period_b,
                    MAX(CASE WHEN time_window = '{pb_key}' THEN {col} END) AS value_b,
                    ROUND(
                        ( MAX(CASE WHEN time_window = '{pa_key}' THEN {col} END)
                        - MAX(CASE WHEN time_window = '{pb_key}' THEN {col} END) )
                        * 100.0
                        / NULLIF(MAX(CASE WHEN time_window = '{pb_key}' THEN {col} END), 0)
                    , 2) AS pct_change
                FROM {SCHEMA}.lr_overall
                WHERE user_type = 'ALL'
                  AND time_window IN ('{pa_key}', '{pb_key}')
            """)

    full_select = " UNION ALL ".join(unions)

    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_topline",
        f"CREATE TABLE {SCHEMA}.lr_topline AS {full_select}",
    ])


###########################################################################
#  STEP 17 — HOURLY MAILER
###########################################################################

def send_hourly_mail(**ctx):
    """
    Reads summary tables and formats the hourly report.

    Replace the log.info calls with your email sending logic
    (e.g. Airflow EmailOperator, SMTP, etc.).
    """
    engine = get_engine()
    import pandas as pd

    hourly_windows = ("today", "yesterday", "mtd", "lmtd")

    overall = pd.read_sql(
        f"""SELECT * FROM {SCHEMA}.lr_overall
            WHERE user_type = 'ALL' AND time_window IN {hourly_windows}""",
        engine,
    )
    lenderwise = pd.read_sql(
        f"""SELECT * FROM {SCHEMA}.lr_lenderwise
            WHERE user_type = 'ALL' AND time_window IN {hourly_windows}""",
        engine,
    )
    topline = pd.read_sql(
        f"""SELECT * FROM {SCHEMA}.lr_topline
            WHERE comparison IN ('Today vs Yesterday', 'MTD vs LMTD')""",
        engine,
    )
    unique_tof = pd.read_sql(
        f"""SELECT * FROM {SCHEMA}.lr_unique_tof
            WHERE time_window IN {hourly_windows}""",
        engine,
    )

    subject = f"Hourly Lending Report — {datetime.now().strftime('%d-%b-%Y %H:%M')}"
    body = _format_report_html(
        title="HOURLY LENDING REPORT",
        overall=overall,
        lenderwise=lenderwise,
        topline=topline,
        unique_tof=unique_tof,
    )
    log.info("HOURLY MAILER\n%s", subject)
    log.info("Body preview:\n%s", body[:2000])

    # ── SEND EMAIL HERE ──
    # from airflow.utils.email import send_email
    # send_email(to=['team@company.com'], subject=subject, html_content=body)


###########################################################################
#  STEP 18 — DAILY MAILER  (conditional — runs only at DAILY_MAIL_HOUR)
###########################################################################

def send_daily_mail(**ctx):
    if datetime.now().hour != DAILY_MAIL_HOUR:
        log.info("Skipping daily mailer (current hour != %s)", DAILY_MAIL_HOUR)
        return

    engine = get_engine()
    import pandas as pd

    daily_windows = ("t_minus_1", "t_minus_2", "mtd", "lmtd")

    overall = pd.read_sql(
        f"""SELECT * FROM {SCHEMA}.lr_overall
            WHERE user_type = 'ALL' AND time_window IN {daily_windows}""",
        engine,
    )
    lenderwise = pd.read_sql(
        f"""SELECT * FROM {SCHEMA}.lr_lenderwise
            WHERE user_type = 'ALL' AND time_window IN {daily_windows}""",
        engine,
    )
    topline = pd.read_sql(
        f"""SELECT * FROM {SCHEMA}.lr_topline
            WHERE comparison IN ('T-1 vs T-2', 'MTD vs LMTD')""",
        engine,
    )
    unique_tof = pd.read_sql(
        f"""SELECT * FROM {SCHEMA}.lr_unique_tof
            WHERE time_window IN {daily_windows}""",
        engine,
    )

    subject = f"Daily Lending Report — {datetime.now().strftime('%d-%b-%Y')}"
    body = _format_report_html(
        title="DAILY LENDING REPORT",
        overall=overall,
        lenderwise=lenderwise,
        topline=topline,
        unique_tof=unique_tof,
    )
    log.info("DAILY MAILER\n%s", subject)
    log.info("Body preview:\n%s", body[:2000])

    # ── SEND EMAIL HERE ──
    # from airflow.utils.email import send_email
    # send_email(to=['team@company.com'], subject=subject, html_content=body)


###########################################################################
#  EMAIL HTML FORMATTER
###########################################################################

def _format_report_html(title, overall, lenderwise, topline, unique_tof):
    """Build a simple HTML email body from dataframes."""
    css = """
    <style>
        body { font-family: Calibri, Arial, sans-serif; font-size: 13px; }
        h2   { color: #2c3e50; }
        h3   { color: #2980b9; margin-top: 24px; }
        table { border-collapse: collapse; margin-bottom: 20px; }
        th    { background: #2c3e50; color: #fff; padding: 6px 12px; text-align: right; }
        td    { border: 1px solid #ddd; padding: 6px 12px; text-align: right; }
        tr:nth-child(even) { background: #f2f2f2; }
        td:first-child, th:first-child { text-align: left; }
    </style>
    """

    def df_to_html(df):
        if df.empty:
            return "<p><em>No data</em></p>"
        return df.to_html(index=False, border=0, na_rep="—")

    sections = [
        f"<h2>{title} — {datetime.now().strftime('%d-%b-%Y %H:%M')}</h2>",
        "<h3>Overall Summary</h3>",
        df_to_html(overall),
        "<h3>Unique User TOF Summary</h3>",
        df_to_html(unique_tof),
        "<h3>Topline Comparison (Amounts in ₹ Cr)</h3>",
        df_to_html(topline),
    ]

    for lender_name in lenderwise["lender"].unique():
        ldf = lenderwise[lenderwise["lender"] == lender_name]
        sections.append(f"<h3>Lenderwise Summary — {lender_name}</h3>")
        sections.append(df_to_html(ldf.drop(columns=["lender"])))

    return f"<html><head>{css}</head><body>{''.join(sections)}</body></html>"


###########################################################################
#  CLEANUP  (optional — drop intermediate step tables)
###########################################################################

_INTERMEDIATE_TABLES = [
    "lr_base", "lr_address", "lr_journey", "lr_lender_det",
    "lr_offer", "lr_offer_accept", "lr_kyc", "lr_bank",
    "lr_nach", "lr_sanction", "lr_drawdown",
]


def cleanup(**ctx):
    stmts = [f"DROP TABLE IF EXISTS {SCHEMA}.{t}" for t in _INTERMEDIATE_TABLES]
    run_sql(stmts)


###########################################################################
#  DAG DEFINITION
###########################################################################

default_args = {
    "owner": "analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="lending_report",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["lending", "report"],
)

t0  = PythonOperator(task_id="create_lenders",     python_callable=create_lenders,     dag=dag)
t1  = PythonOperator(task_id="create_base",         python_callable=create_base,         dag=dag)
t2  = PythonOperator(task_id="create_address",      python_callable=create_address,      dag=dag)
t3  = PythonOperator(task_id="create_journey",      python_callable=create_journey,      dag=dag)
t4  = PythonOperator(task_id="create_lender_det",   python_callable=create_lender_det,   dag=dag)
t5  = PythonOperator(task_id="create_offer",        python_callable=create_offer,        dag=dag)
t6  = PythonOperator(task_id="create_offer_accept", python_callable=create_offer_accept, dag=dag)
t7  = PythonOperator(task_id="create_kyc",          python_callable=create_kyc,          dag=dag)
t8  = PythonOperator(task_id="create_bank",         python_callable=create_bank,         dag=dag)
t9  = PythonOperator(task_id="create_nach",         python_callable=create_nach,         dag=dag)
t10 = PythonOperator(task_id="create_sanction",     python_callable=create_sanction,     dag=dag)
t11 = PythonOperator(task_id="create_drawdown",     python_callable=create_drawdown,     dag=dag)
t12 = PythonOperator(task_id="create_lenderwise",   python_callable=create_lenderwise,   dag=dag)
t13 = PythonOperator(task_id="create_overall",      python_callable=create_overall,      dag=dag)
t14 = PythonOperator(task_id="create_funnel",       python_callable=create_funnel,       dag=dag)
t15 = PythonOperator(task_id="create_unique_tof",   python_callable=create_unique_tof,   dag=dag)
t16 = PythonOperator(task_id="create_topline",      python_callable=create_topline,      dag=dag)
t17 = PythonOperator(task_id="send_hourly_mail",    python_callable=send_hourly_mail,    dag=dag)
t18 = PythonOperator(task_id="send_daily_mail",     python_callable=send_daily_mail,     dag=dag)
# t19 = PythonOperator(task_id="cleanup",           python_callable=cleanup,             dag=dag)

# ── Task dependencies ────────────────────────────────────────────────
#
#  create_lenders → create_base
#                       │
#                       ├── create_address ──────────────────────┐
#                       ├── create_journey ──────────────────────┤
#                       ├── create_lender_det ───────────────────┤
#                       ├── create_offer → create_offer_accept ──┤
#                       ├── create_kyc ──────────────────────────┤
#                       ├── create_bank ─────────────────────────┤
#                       ├── create_nach ─────────────────────────┤
#                       ├── create_sanction ─────────────────────┤
#                       └── create_drawdown ─────────────────────┤
#                                                                │
#                       create_lenderwise ◄──────────────────────┘
#                           │
#                           ├── create_overall → create_topline ──┐
#                           ├── create_funnel ────────────────────┤
#                           └── create_unique_tof ────────────────┤
#                                                                 │
#                           ┌─────────────────────────────────────┘
#                           ├── send_hourly_mail
#                           └── send_daily_mail

t0 >> t1

t1 >> [t2, t3, t4, t5, t7, t8, t9, t10, t11]
t5 >> t6

[t2, t3, t4, t6, t7, t8, t9, t10, t11] >> t12

t12 >> t13 >> t16
t12 >> t14
t12 >> t15

[t14, t15, t16] >> t17
[t14, t15, t16] >> t18
# t17 >> t19
# t18 >> t19
