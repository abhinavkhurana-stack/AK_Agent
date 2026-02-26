"""
===============================================================================
  LENDING REPORT DAG  (Single DAG — Hourly + Daily + Closed Funnel)
===============================================================================

  3 EMAILS
  ────────
  1. Hourly Lending Summary - EMI Open Funnel BBK   (every hour)
  2. Daily  Lending Summary - EMI Open Funnel BBK   (once/day at 9 AM IST)
  3. Daily  Lending Summary - EMI Closed Funnel BBK  (once/day at 9 AM IST)

  OPEN vs CLOSED
  ──────────────
  Both funnels reuse the SAME intermediate step tables (lr_base, lr_address,
  lr_journey, lr_offer …).  The only difference is how they COUNT:
    • Open  — each step counted independently
    • Closed — each step requires ALL prior steps to have passed

  HOW TO ADD A NEW FLDG LENDER
  ────────────────────────────
  1.  Add one row to LENDER_ROWS  →  (partner_id, 'Name', 'FLDG')
  2.  Done.  All queries auto-include the new lender.

  TESTING
  ───────
  To force-fire the daily emails outside of 9 AM IST, call:
      test_daily_open_mail()
      test_closed_daily_mail()
  Or trigger the test_* Airflow tasks (wired at the bottom of the DAG).
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
#  CONFIGURATION
###########################################################################

SCHEMA = "mobikwik_schema"

LENDER_ROWS = [
    (16, "PFL",    "FLDG"),
    (20, "NAC",    "FLDG"),
    (24, "SMFGPL", "FLDG"),
    # (99, "NewLender", "FLDG"),
]

AMOUNT_DIVISOR = 1e7
EMAIL_TO = ["abhinav.khurana@mobikwik.com"]
DAILY_MAIL_HOUR_IST = 9


###########################################################################
#  DATABASE
###########################################################################

def get_engine():
    pymysql.install_as_MySQLdb()
    return sa.create_engine(
        "mysql+pymysql://analytics:vsn%400pl3TYujk23(o"
        "@data-analytics-mysql-prod.mbkinternal.in:3308/mobinew",
        pool_recycle=1800, pool_pre_ping=True,
    )

def run_sql(statements):
    engine = get_engine()
    with engine.begin() as conn:
        for s in statements:
            s = s.strip()
            if s:
                log.info("SQL > %s", s[:200].replace("\n", " "))
                conn.execute(sa.text(s))


###########################################################################
#  TIME WINDOWS
###########################################################################

def _fmt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def compute_windows():
    now       = datetime.now()
    today_00  = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yest_00   = today_00 - timedelta(days=1)
    yest_now  = now - timedelta(days=1)
    db4y_00   = today_00 - timedelta(days=2)

    mtd_start    = today_00.replace(day=1)
    mtd_end      = today_00
    lmtd_start   = (mtd_start - timedelta(days=1)).replace(day=1)
    t1_day       = (today_00 - timedelta(days=1)).day
    max_prev_day = calendar.monthrange(lmtd_start.year, lmtd_start.month)[1]
    lmtd_end     = lmtd_start.replace(day=min(t1_day, max_prev_day)) + timedelta(days=1)

    new_user_cutoff = _fmt(mtd_start)

    windows = {
        "today":     (_fmt(today_00), _fmt(now)),
        "yesterday": (_fmt(yest_00),  _fmt(yest_now)),
        "t_minus_1": (_fmt(yest_00),  _fmt(today_00)),
        "t_minus_2": (_fmt(db4y_00),  _fmt(yest_00)),
        "mtd":       (_fmt(mtd_start),  _fmt(mtd_end)),
        "lmtd":      (_fmt(lmtd_start), _fmt(lmtd_end)),
    }
    return windows, _fmt(lmtd_start), _fmt(now), new_user_cutoff


###########################################################################
#  STEP 0 — LENDER REFERENCE TABLE
###########################################################################

def create_lenders(**ctx):
    stmts = [
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_lenders",
        f"""CREATE TABLE {SCHEMA}.lr_lenders (
                lender_id INT NOT NULL, lender_name VARCHAR(50) NOT NULL,
                lender_type VARCHAR(20) NOT NULL, PRIMARY KEY (lender_id))""",
    ]
    for lid, lname, ltype in LENDER_ROWS:
        stmts.append(f"INSERT INTO {SCHEMA}.lr_lenders VALUES ({lid},'{lname}','{ltype}')")
    run_sql(stmts)


###########################################################################
#  STEP 1 — BASE
###########################################################################

def create_base(**ctx):
    w, ws, we, cutoff = compute_windows()
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_base",
        f"""CREATE TABLE {SCHEMA}.lr_base AS
            SELECT DISTINCT b.mbkloanid, b.createdat, b.memberid,
                CASE WHEN prev.memberid IS NULL THEN 'NEW' ELSE 'OLD' END AS user_type
            FROM lending.boost b
            LEFT JOIN (
                SELECT DISTINCT memberid FROM lending.boost
                WHERE kycflow='HYBRID_KYC_FLOW' AND createdat < '{cutoff}'
            ) prev ON b.memberid = prev.memberid
            WHERE b.kycflow='HYBRID_KYC_FLOW'
              AND b.createdat >= '{ws}' AND b.createdat < '{we}'""",
        f"ALTER TABLE {SCHEMA}.lr_base ADD INDEX idx_mbk(mbkloanid), ADD INDEX idx_dt(createdat), ADD INDEX idx_ut(user_type)",
    ])


###########################################################################
#  STEP 2–11 — INTERMEDIATE STEP TABLES  (shared by open + closed)
###########################################################################

def create_address(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_address",
        f"""CREATE TABLE {SCHEMA}.lr_address AS
            SELECT DISTINCT mpd.mbkloanid
            FROM lending.memberprofiledetails mpd
            INNER JOIN {SCHEMA}.lr_base b ON mpd.mbkloanid=b.mbkloanid
            WHERE mpd.PermanentPincode IS NOT NULL AND mpd.PermanentPincode>0""",
        f"ALTER TABLE {SCHEMA}.lr_address ADD INDEX idx_mbk(mbkloanid)",
    ])

def create_journey(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_journey",
        f"""CREATE TABLE {SCHEMA}.lr_journey AS
            SELECT u.mbk_loan_id AS mbkloanid, l.lender_id,
                MAX(u.user_journey_status_stage='PreLenderSanityChecks')   AS lpa_run,
                MAX(u.user_journey_status_stage='PostLenderSanityChecks')  AS lpa_pass,
                MAX(u.user_journey_status_stage='PreBreFraudRulesSuccess') AS pre_bre,
                MAX(u.user_journey_status_stage='BureauPullSuccess')       AS bureau_pull,
                MAX(u.user_journey_status_stage='BreSuccess')              AS bre_success,
                MAX(u.user_journey_status_stage='PostBreFraudRuleSuccess') AS post_bre,
                MAX(u.user_journey_status_stage='PanKycValidationDone')    AS pan_kyc
            FROM lending.user_journey_status u
            INNER JOIN {SCHEMA}.lr_base b ON u.mbk_loan_id=b.mbkloanid
            INNER JOIN {SCHEMA}.lr_lenders l
                ON FIND_IN_SET(l.lender_id, u.eligible_partners)>0 AND l.lender_type='FLDG'
            GROUP BY u.mbk_loan_id, l.lender_id""",
        f"ALTER TABLE {SCHEMA}.lr_journey ADD INDEX idx_mbk(mbkloanid), ADD INDEX idx_lid(lender_id)",
    ])

def create_lender_det(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_lender_det",
        f"""CREATE TABLE {SCHEMA}.lr_lender_det AS
            SELECT DISTINCT lad.mbk_loan_id AS mbkloanid, lad.lending_partner AS lender_id
            FROM lending.lender_additional_details lad
            INNER JOIN {SCHEMA}.lr_base b ON lad.mbk_loan_id=b.mbkloanid
            INNER JOIN {SCHEMA}.lr_lenders l ON lad.lending_partner=l.lender_id""",
        f"ALTER TABLE {SCHEMA}.lr_lender_det ADD INDEX idx_mbk(mbkloanid), ADD INDEX idx_lid(lender_id)",
    ])

def create_offer(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_offer",
        f"""CREATE TABLE {SCHEMA}.lr_offer AS
            SELECT DISTINCT c.mbkloanid, c.lendingpartnerid AS lender_id
            FROM lending.creditline c
            INNER JOIN {SCHEMA}.lr_base b ON c.mbkloanid=b.mbkloanid
            INNER JOIN {SCHEMA}.lr_lenders l ON c.lendingpartnerid=l.lender_id""",
        f"ALTER TABLE {SCHEMA}.lr_offer ADD INDEX idx_mbk(mbkloanid), ADD INDEX idx_lid(lender_id)",
    ])

def create_offer_accept(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_offer_accept",
        f"""CREATE TABLE {SCHEMA}.lr_offer_accept AS
            SELECT DISTINCT ua.mbkloanid, o.lender_id
            FROM lending.useracceptancedetails ua
            INNER JOIN {SCHEMA}.lr_offer o ON ua.mbkloanid=o.mbkloanid
            WHERE ua.stage='SAVE_OFFER'""",
        f"ALTER TABLE {SCHEMA}.lr_offer_accept ADD INDEX idx_mbk(mbkloanid), ADD INDEX idx_lid(lender_id)",
    ])

def create_kyc(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_kyc",
        f"""CREATE TABLE {SCHEMA}.lr_kyc AS
            SELECT DISTINCT uki.mbk_loan_id AS mbkloanid, uki.lending_partner AS lender_id
            FROM lending.user_kyc_info uki
            INNER JOIN {SCHEMA}.lr_base b ON uki.mbk_loan_id=b.mbkloanid
            INNER JOIN {SCHEMA}.lr_lenders l ON uki.lending_partner=l.lender_id""",
        f"ALTER TABLE {SCHEMA}.lr_kyc ADD INDEX idx_mbk(mbkloanid), ADD INDEX idx_lid(lender_id)",
    ])

def create_bank(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_bank",
        f"""CREATE TABLE {SCHEMA}.lr_bank AS
            SELECT DISTINCT bv.mbkloanid
            FROM lending.bankverification bv
            INNER JOIN {SCHEMA}.lr_base b ON bv.mbkloanid=b.mbkloanid
            WHERE bv.bankverified=1""",
        f"ALTER TABLE {SCHEMA}.lr_bank ADD INDEX idx_mbk(mbkloanid)",
    ])

def create_nach(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_nach",
        f"""CREATE TABLE {SCHEMA}.lr_nach AS
            SELECT DISTINCT nr.mbkloanid
            FROM lending.NachRegistration nr
            INNER JOIN {SCHEMA}.lr_base b ON nr.mbkloanid=b.mbkloanid
            WHERE nr.status IN ('eMandateSuccess','pNachSuccess')""",
        f"ALTER TABLE {SCHEMA}.lr_nach ADD INDEX idx_mbk(mbkloanid)",
    ])

def create_sanction(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_sanction",
        f"""CREATE TABLE {SCHEMA}.lr_sanction AS
            SELECT c.mbkloanid, c.lendingpartnerid AS lender_id, SUM(c.sanctionedlineamount) AS amount
            FROM lending.creditline c
            INNER JOIN {SCHEMA}.lr_base b ON c.mbkloanid=b.mbkloanid
            INNER JOIN {SCHEMA}.lr_lenders l ON c.lendingpartnerid=l.lender_id
            WHERE c.status LIKE '%,11%'
            GROUP BY c.mbkloanid, c.lendingpartnerid""",
        f"ALTER TABLE {SCHEMA}.lr_sanction ADD INDEX idx_mbk(mbkloanid), ADD INDEX idx_lid(lender_id)",
    ])

def create_drawdown(**ctx):
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_drawdown",
        f"""CREATE TABLE {SCHEMA}.lr_drawdown AS
            SELECT d.mbkloanid, SUM(d.drawamount) AS amount
            FROM lending.drawdown d
            INNER JOIN {SCHEMA}.lr_base b ON d.mbkloanid=b.mbkloanid
            WHERE d.drawdownstatus IN (4,17)
            GROUP BY d.mbkloanid""",
        f"ALTER TABLE {SCHEMA}.lr_drawdown ADD INDEX idx_mbk(mbkloanid)",
    ])


###########################################################################
#  STEP 12 — OPEN FUNNEL LENDERWISE SUMMARY
###########################################################################

_OPEN_SELECT = """
    SELECT
        '{{wname}}'  AS time_window,
        '{{lname}}'  AS lender,
        COALESCE(b.user_type, 'ALL') AS user_type,

        COUNT(DISTINCT b.mbkloanid)                                       AS basic_details,
        COUNT(DISTINCT a.mbkloanid)                                       AS address,
        COUNT(DISTINCT CASE WHEN j.lpa_run=1     THEN j.mbkloanid END)   AS lpa_run,
        COUNT(DISTINCT CASE WHEN j.lpa_pass=1    THEN j.mbkloanid END)   AS lpa_pass,
        COUNT(DISTINCT CASE WHEN j.pre_bre=1     THEN j.mbkloanid END)   AS pre_bre,
        COUNT(DISTINCT CASE WHEN j.bureau_pull=1 THEN j.mbkloanid END)   AS bureau_pull,
        COUNT(DISTINCT CASE WHEN j.bre_success=1 THEN j.mbkloanid END)   AS bre_success,
        COUNT(DISTINCT CASE WHEN j.post_bre=1    THEN j.mbkloanid END)   AS post_bre,
        COUNT(DISTINCT CASE WHEN j.pan_kyc=1     THEN j.mbkloanid END)   AS pan_kyc,
        COUNT(DISTINCT o.mbkloanid)                                       AS offer,
        COUNT(DISTINCT oa.mbkloanid)                                      AS offer_accepted,
        COUNT(DISTINCT bk.mbkloanid)                                      AS bank,
        COUNT(DISTINCT n.mbkloanid)                                       AS nach,
        COUNT(DISTINCT s.mbkloanid)                                       AS sanction,
        ROUND(COALESCE(SUM(s.amount),0)/{amt_div},2)                     AS sanction_amt_cr,
        COUNT(DISTINCT d.mbkloanid)                                       AS drawdown,
        ROUND(COALESCE(SUM(d.amount),0)/{amt_div},2)                     AS drawdown_amt_cr

    FROM {sch}.lr_base b
    LEFT JOIN {sch}.lr_address a ON a.mbkloanid=b.mbkloanid
    LEFT JOIN (SELECT mbkloanid,lpa_run,lpa_pass,pre_bre,bureau_pull,
                      bre_success,post_bre,pan_kyc
               FROM {sch}.lr_journey WHERE lender_id={{lid}}) j ON j.mbkloanid=b.mbkloanid
    LEFT JOIN (SELECT DISTINCT mbkloanid FROM {sch}.lr_offer WHERE lender_id={{lid}}) o ON o.mbkloanid=b.mbkloanid
    LEFT JOIN (SELECT DISTINCT mbkloanid FROM {sch}.lr_offer_accept WHERE lender_id={{lid}}) oa ON oa.mbkloanid=b.mbkloanid
    LEFT JOIN {sch}.lr_bank bk ON bk.mbkloanid=b.mbkloanid
    LEFT JOIN {sch}.lr_nach n  ON n.mbkloanid=b.mbkloanid
    LEFT JOIN (SELECT mbkloanid,SUM(amount) AS amount FROM {sch}.lr_sanction WHERE lender_id={{lid}} GROUP BY mbkloanid) s ON s.mbkloanid=b.mbkloanid
    LEFT JOIN {sch}.lr_drawdown d ON d.mbkloanid=b.mbkloanid

    WHERE b.createdat >= '{{wstart}}' AND b.createdat < '{{wend}}'
    GROUP BY b.user_type WITH ROLLUP
""".format(sch=SCHEMA, amt_div=int(AMOUNT_DIVISOR))


###########################################################################
#  STEP 12c — CLOSED FUNNEL LENDERWISE SUMMARY
###########################################################################
#
#  Same JOINs, but each step's COUNT requires ALL prior steps to be present.
#  Journey flags are multiplied cumulatively (1*1*1=1, 1*0*1=0) so
#  cf_pan_kyc=1 means every journey stage passed.
#

_CLOSED_SELECT = """
    SELECT
        '{{wname}}'  AS time_window,
        '{{lname}}'  AS lender,
        COALESCE(b.user_type, 'ALL') AS user_type,

        COUNT(DISTINCT b.mbkloanid)  AS basic_details,
        COUNT(DISTINCT a.mbkloanid)  AS address,

        COUNT(DISTINCT CASE WHEN a.mbkloanid IS NOT NULL AND j.cf1=1 THEN j.mbkloanid END) AS lpa_run,
        COUNT(DISTINCT CASE WHEN a.mbkloanid IS NOT NULL AND j.cf2=1 THEN j.mbkloanid END) AS lpa_pass,
        COUNT(DISTINCT CASE WHEN a.mbkloanid IS NOT NULL AND j.cf3=1 THEN j.mbkloanid END) AS pre_bre,
        COUNT(DISTINCT CASE WHEN a.mbkloanid IS NOT NULL AND j.cf4=1 THEN j.mbkloanid END) AS bureau_pull,
        COUNT(DISTINCT CASE WHEN a.mbkloanid IS NOT NULL AND j.cf5=1 THEN j.mbkloanid END) AS bre_success,
        COUNT(DISTINCT CASE WHEN a.mbkloanid IS NOT NULL AND j.cf6=1 THEN j.mbkloanid END) AS post_bre,
        COUNT(DISTINCT CASE WHEN a.mbkloanid IS NOT NULL AND j.cf7=1 THEN j.mbkloanid END) AS pan_kyc,

        COUNT(DISTINCT CASE WHEN a.mbkloanid IS NOT NULL AND j.cf7=1
              AND o.mbkloanid IS NOT NULL THEN o.mbkloanid END) AS offer,
        COUNT(DISTINCT CASE WHEN a.mbkloanid IS NOT NULL AND j.cf7=1
              AND o.mbkloanid IS NOT NULL AND oa.mbkloanid IS NOT NULL THEN oa.mbkloanid END) AS offer_accepted,
        COUNT(DISTINCT CASE WHEN a.mbkloanid IS NOT NULL AND j.cf7=1
              AND o.mbkloanid IS NOT NULL AND oa.mbkloanid IS NOT NULL
              AND bk.mbkloanid IS NOT NULL THEN bk.mbkloanid END) AS bank,
        COUNT(DISTINCT CASE WHEN a.mbkloanid IS NOT NULL AND j.cf7=1
              AND o.mbkloanid IS NOT NULL AND oa.mbkloanid IS NOT NULL
              AND bk.mbkloanid IS NOT NULL AND n.mbkloanid IS NOT NULL THEN n.mbkloanid END) AS nach,
        COUNT(DISTINCT CASE WHEN a.mbkloanid IS NOT NULL AND j.cf7=1
              AND o.mbkloanid IS NOT NULL AND oa.mbkloanid IS NOT NULL
              AND bk.mbkloanid IS NOT NULL AND n.mbkloanid IS NOT NULL
              AND s.mbkloanid IS NOT NULL THEN s.mbkloanid END) AS sanction,
        ROUND(COALESCE(SUM(CASE WHEN a.mbkloanid IS NOT NULL AND j.cf7=1
              AND o.mbkloanid IS NOT NULL AND oa.mbkloanid IS NOT NULL
              AND bk.mbkloanid IS NOT NULL AND n.mbkloanid IS NOT NULL
              THEN s.amount ELSE 0 END),0)/{amt_div},2) AS sanction_amt_cr,
        COUNT(DISTINCT CASE WHEN a.mbkloanid IS NOT NULL AND j.cf7=1
              AND o.mbkloanid IS NOT NULL AND oa.mbkloanid IS NOT NULL
              AND bk.mbkloanid IS NOT NULL AND n.mbkloanid IS NOT NULL
              AND s.mbkloanid IS NOT NULL AND d.mbkloanid IS NOT NULL THEN d.mbkloanid END) AS drawdown,
        ROUND(COALESCE(SUM(CASE WHEN a.mbkloanid IS NOT NULL AND j.cf7=1
              AND o.mbkloanid IS NOT NULL AND oa.mbkloanid IS NOT NULL
              AND bk.mbkloanid IS NOT NULL AND n.mbkloanid IS NOT NULL
              AND s.mbkloanid IS NOT NULL
              THEN d.amount ELSE 0 END),0)/{amt_div},2) AS drawdown_amt_cr

    FROM {sch}.lr_base b
    LEFT JOIN {sch}.lr_address a ON a.mbkloanid=b.mbkloanid
    LEFT JOIN (
        SELECT mbkloanid,
            lpa_run                                                          AS cf1,
            lpa_run*lpa_pass                                                 AS cf2,
            lpa_run*lpa_pass*pre_bre                                         AS cf3,
            lpa_run*lpa_pass*pre_bre*bureau_pull                             AS cf4,
            lpa_run*lpa_pass*pre_bre*bureau_pull*bre_success                 AS cf5,
            lpa_run*lpa_pass*pre_bre*bureau_pull*bre_success*post_bre        AS cf6,
            lpa_run*lpa_pass*pre_bre*bureau_pull*bre_success*post_bre*pan_kyc AS cf7
        FROM {sch}.lr_journey WHERE lender_id={{lid}}
    ) j ON j.mbkloanid=b.mbkloanid
    LEFT JOIN (SELECT DISTINCT mbkloanid FROM {sch}.lr_offer WHERE lender_id={{lid}}) o ON o.mbkloanid=b.mbkloanid
    LEFT JOIN (SELECT DISTINCT mbkloanid FROM {sch}.lr_offer_accept WHERE lender_id={{lid}}) oa ON oa.mbkloanid=b.mbkloanid
    LEFT JOIN {sch}.lr_bank bk ON bk.mbkloanid=b.mbkloanid
    LEFT JOIN {sch}.lr_nach n  ON n.mbkloanid=b.mbkloanid
    LEFT JOIN (SELECT mbkloanid,SUM(amount) AS amount FROM {sch}.lr_sanction WHERE lender_id={{lid}} GROUP BY mbkloanid) s ON s.mbkloanid=b.mbkloanid
    LEFT JOIN {sch}.lr_drawdown d ON d.mbkloanid=b.mbkloanid

    WHERE b.createdat >= '{{wstart}}' AND b.createdat < '{{wend}}'
    GROUP BY b.user_type WITH ROLLUP
""".format(sch=SCHEMA, amt_div=int(AMOUNT_DIVISOR))


###########################################################################
#  SUMMARY TABLE COLUMN SPEC  (shared by open + closed create functions)
###########################################################################

_SUMMARY_COLS = """(
    time_window VARCHAR(20), lender VARCHAR(50), user_type VARCHAR(10),
    basic_details BIGINT DEFAULT 0, address BIGINT DEFAULT 0,
    lpa_run BIGINT DEFAULT 0, lpa_pass BIGINT DEFAULT 0,
    pre_bre BIGINT DEFAULT 0, bureau_pull BIGINT DEFAULT 0,
    bre_success BIGINT DEFAULT 0, post_bre BIGINT DEFAULT 0,
    pan_kyc BIGINT DEFAULT 0,
    offer BIGINT DEFAULT 0, offer_accepted BIGINT DEFAULT 0,
    bank BIGINT DEFAULT 0, nach BIGINT DEFAULT 0,
    sanction BIGINT DEFAULT 0, sanction_amt_cr DECIMAL(14,2) DEFAULT 0,
    drawdown BIGINT DEFAULT 0, drawdown_amt_cr DECIMAL(14,2) DEFAULT 0,
    INDEX idx_tw(time_window), INDEX idx_ln(lender(50)), INDEX idx_ut(user_type)
)"""

def _create_summary(table_name, select_template):
    windows, _, _, _ = compute_windows()
    stmts = [
        f"DROP TABLE IF EXISTS {SCHEMA}.{table_name}",
        f"CREATE TABLE {SCHEMA}.{table_name} {_SUMMARY_COLS}",
    ]
    for lid, lname, _ in LENDER_ROWS:
        for wname, (wstart, wend) in windows.items():
            sql = select_template.format(wname=wname, lname=lname, lid=lid, wstart=wstart, wend=wend)
            stmts.append(f"INSERT INTO {SCHEMA}.{table_name} {sql}")
    run_sql(stmts)

def _create_overall(src_table, dst_table):
    cols = ("basic_details,address,lpa_run,lpa_pass,pre_bre,bureau_pull,"
            "bre_success,post_bre,pan_kyc,offer,offer_accepted,bank,nach,"
            "sanction,sanction_amt_cr,drawdown,drawdown_amt_cr")
    sums = ",".join(f"SUM({c}) AS {c}" for c in cols.split(","))
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.{dst_table}",
        f"""CREATE TABLE {SCHEMA}.{dst_table} AS
            SELECT time_window, user_type, {sums}
            FROM {SCHEMA}.{src_table} GROUP BY time_window, user_type""",
        f"ALTER TABLE {SCHEMA}.{dst_table} ADD INDEX idx_tw(time_window), ADD INDEX idx_ut(user_type)",
    ])


###########################################################################
#  OPEN FUNNEL — create tasks
###########################################################################

def create_open_lenderwise(**ctx):
    _create_summary("lr_lenderwise", _OPEN_SELECT)

def create_open_overall(**ctx):
    _create_overall("lr_lenderwise", "lr_overall")


###########################################################################
#  CLOSED FUNNEL — create tasks
###########################################################################

def create_closed_lenderwise(**ctx):
    _create_summary("lrc_lenderwise", _CLOSED_SELECT)

def create_closed_overall(**ctx):
    _create_overall("lrc_lenderwise", "lrc_overall")


###########################################################################
#  UNIQUE TOF + TOPLINE  (shared, computed from open funnel)
###########################################################################

def create_unique_tof(**ctx):
    windows, _, _, _ = compute_windows()
    unions = []
    for wname, (wstart, wend) in windows.items():
        unions.append(f"""
            SELECT '{wname}' AS time_window,
                COUNT(DISTINCT memberid) AS unique_users,
                COUNT(DISTINCT mbkloanid) AS unique_applications
            FROM {SCHEMA}.lr_base
            WHERE createdat>='{wstart}' AND createdat<'{wend}'""")
    run_sql([
        f"DROP TABLE IF EXISTS {SCHEMA}.lr_unique_tof",
        f"CREATE TABLE {SCHEMA}.lr_unique_tof AS {' UNION ALL '.join(unions)}",
        f"ALTER TABLE {SCHEMA}.lr_unique_tof ADD INDEX idx_tw(time_window)",
    ])


###########################################################################
#  FUNNEL DISPLAY ROWS  (drives the email grid)
###########################################################################

_FUNNEL_DISPLAY = [
    ("Basic_Details",                               "basic_details",   None,             None),
    ("Address_Details",                             "address",         None,             None),
    ("% Address_details_from_Basic_Details",         None,             "address",        "basic_details"),
    ("LPA_RUN",                                     "lpa_run",         None,             None),
    ("%LPA_Run_from_Address_details",                None,             "lpa_run",        "address"),
    ("LPA_PASS",                                    "lpa_pass",        None,             None),
    ("%LPA_PASS_from_LPA_RUN",                       None,             "lpa_pass",       "lpa_run"),
    ("Pre_BRE",                                     "pre_bre",         None,             None),
    ("% Pre_BRE_from_LPA_PASS",                      None,             "pre_bre",        "lpa_pass"),
    ("Bureau_Pull",                                 "bureau_pull",     None,             None),
    ("%Bureau_Pull_from_Pre_BRE",                    None,             "bureau_pull",    "pre_bre"),
    ("BRE_Success",                                 "bre_success",     None,             None),
    ("%BRE_Success_from_Bureau_Pull",                None,             "bre_success",    "bureau_pull"),
    ("Post_BRE",                                    "post_bre",        None,             None),
    ("%Post_BRE_from_BRE_Success",                   None,             "post_bre",       "bre_success"),
    ("PAN_Validation",                              "pan_kyc",         None,             None),
    ("% PAN_Validation_from_Post_BRE",               None,             "pan_kyc",        "post_bre"),
    ("Offer_Generated",                             "offer",           None,             None),
    ("% Offer_Generated_from_PAN_Validation",        None,             "offer",          "pan_kyc"),
    ("Offer_Accepted",                              "offer_accepted",  None,             None),
    ("%Offer_accepted_from_Offer_Generated",         None,             "offer_accepted", "offer"),
    ("Bank_Verified",                               "bank",            None,             None),
    ("%Bank_Verified_from_Offer_Accepted",           None,             "bank",           "offer_accepted"),
    ("NACH",                                        "nach",            None,             None),
    ("% NACH_from_Bank_Verified",                    None,             "nach",           "bank"),
    ("Loan_Sanctioned",                             "sanction",        None,             None),
    ("% Loan_Sanctioned_from_NACH",                  None,             "sanction",       "nach"),
    ("Drawdown",                                    "drawdown",        None,             None),
    ("% Drawdown_from_Loan_Sanctioned",              None,             "drawdown",       "sanction"),
    ("% Drawdown_from_Offer_Generated",              None,             "drawdown",       "offer"),
    ("%Drawdown_from_Basic_Details",                 None,             "drawdown",       "basic_details"),
    ("Sanctioned_Amount (in Cr.)",                  "sanction_amt_cr", None,             None),
    ("Drawdown_Amount (in Cr.)",                    "drawdown_amt_cr", None,             None),
]


###########################################################################
#  EMAIL HELPERS
###########################################################################

def _html_head():
    return (
        "<html>\n<head>\n<style>\n"
        "body { font-family: Arial, sans-serif; }\n"
        "table { width: 100%; border-collapse: collapse; }\n"
        "th, td { border: 1px solid black; padding: 8px; text-align: left; }\n"
        "th { background-color: #f2f2f2; }\n"
        "</style>\n</head>\n<body>\n"
    )

def _make_lookup(df):
    lk = {}
    for _, row in df.iterrows():
        lk[(row["time_window"], row["user_type"])] = row
    return lk

def _val(lookup, tw, ut, col):
    row = lookup.get((tw, ut))
    if row is None: return 0
    try:
        v = row.get(col, 0)
        return float(v) if v is not None else 0
    except (ValueError, TypeError): return 0

def _fc(v):
    try:
        n = int(round(float(v)))
        s = str(abs(n))
        if len(s) <= 3: return ("-"+s) if n<0 else s
        last3, rest = s[-3:], s[:-3]
        parts = []
        while rest: parts.append(rest[-2:]); rest = rest[:-2]
        f = ",".join(reversed(parts))+","+last3
        return ("-"+f) if n<0 else f
    except (ValueError, TypeError): return ""

def _fa(v):
    try:
        f = float(v)
        return f"{f:.2f}" if f else ""
    except (ValueError, TypeError): return ""

def _fpct(num, den):
    try:
        n, d = float(num or 0), float(den or 0)
        return f"{n/d*100:.2f}%" if d else ""
    except (ValueError, TypeError): return ""

def _pct_change(a, b):
    try:
        a, b = float(a or 0), float(b or 0)
        if b == 0: return "None" if a == 0 else ""
        return f"{(a-b)/b*100:.2f}%"
    except (ValueError, TypeError): return ""

def _make_funnel_df(summary_df, windows, wlabels, utypes):
    import pandas as pd
    lk = _make_lookup(summary_df)
    col_names, col_keys = ["Particular"], []
    for ut_key, ut_label in utypes:
        for wk in windows:
            col_names.append(f"{wlabels[wk]}_{ut_label}")
            col_keys.append((wk, ut_key))
    rows = []
    for label, val_col, pct_num, pct_den in _FUNNEL_DISPLAY:
        row = [label]
        for wk, ut in col_keys:
            if val_col is None:
                row.append(_fpct(_val(lk, wk, ut, pct_num), _val(lk, wk, ut, pct_den)))
            elif val_col.endswith("_amt_cr"):
                row.append(_fa(_val(lk, wk, ut, val_col)))
            else:
                v = _val(lk, wk, ut, val_col)
                row.append(_fc(v) if v else "")
        rows.append(row)
    return pd.DataFrame(rows, columns=col_names)

def _make_topline_df(lenderwise, overall, pa_key, pb_key, pa_lbl, pb_lbl):
    import pandas as pd
    metrics = [("Drawdown","drawdown_amt_cr",True),("Sanction","sanction_amt_cr",True),("Offer_Gen","offer",False)]
    col_names = ["Lender"]
    for m,_,_ in metrics: col_names += [f"{m}_{pa_lbl}",f"{m}_{pb_lbl}",f"{m}_Change"]
    def _row(label, df):
        lk = _make_lookup(df); r = [label]
        for _,col,is_amt in metrics:
            va, vb = _val(lk,pa_key,"ALL",col), _val(lk,pb_key,"ALL",col)
            fmt = _fa if is_amt else _fc
            r += [fmt(va), fmt(vb), _pct_change(va,vb)]
        return r
    rows = [_row("Total", overall)]
    for ln in sorted(lenderwise["lender"].unique()):
        rows.append(_row(ln, lenderwise[lenderwise["lender"]==ln]))
    return pd.DataFrame(rows, columns=col_names)


###########################################################################
#  BUILD EMAIL BODY  (reusable for open + closed)
###########################################################################

def _build_body(overall, lenderwise, unique_tof, windows, wlabels, utypes,
                topline_cfgs, greeting="Hi Team,<br>Please find the summary below:"):
    import pandas as pd
    body = _html_head()
    body += f"<p>{greeting}</p>\n"

    for title, pa, pb, pa_lbl, pb_lbl in topline_cfgs:
        body += f"<h3>{title}</h3>\n"
        body += _make_topline_df(lenderwise, overall, pa, pb, pa_lbl, pb_lbl).to_html(index=False)

    if unique_tof is not None and not unique_tof.empty:
        body += "<h3>Unique User TOF Summary:</h3>\n"
        body += unique_tof[["time_window","unique_users","unique_applications"]].to_html(index=False)

    body += ("<h3>Overall Summary: [** All Particulars are taken as per user "
             "instances- multiple journeys from same user is expected]</h3>\n")
    body += _make_funnel_df(overall, windows, wlabels, utypes).to_html(index=False)

    for lname in sorted(lenderwise["lender"].unique()):
        ldf = lenderwise[lenderwise["lender"]==lname]
        body += f"<h3>Lenderwise Summary - {lname}:</h3>\n"
        body += _make_funnel_df(ldf, windows, wlabels, utypes).to_html(index=False) + "\n"

    body += "</body>\n</html>"
    return body


###########################################################################
#  MAILER 1 — Hourly Open Funnel  (every run)
###########################################################################

def send_hourly_mail(**ctx):
    engine = get_engine()
    import pandas as pd
    now = datetime.now()

    overall    = pd.read_sql(f"SELECT * FROM {SCHEMA}.lr_overall", engine)
    lenderwise = pd.read_sql(f"SELECT * FROM {SCHEMA}.lr_lenderwise", engine)
    unique_tof = pd.read_sql(f"SELECT * FROM {SCHEMA}.lr_unique_tof WHERE time_window IN ('today','yesterday','mtd','lmtd')", engine)

    subj = f"Hourly Lending Summary - EMI Open Funnel BBK || {now.strftime('%d-%b-%Y %I:%M %p')}"
    body = _build_body(
        overall, lenderwise, unique_tof,
        windows=["today","mtd","lmtd"],
        wlabels={"today":"YTD","mtd":"MTD","lmtd":"LMTD"},
        utypes=[("ALL","Overall"),("OLD","Repeat"),("NEW","New")],
        topline_cfgs=[
            ("Today vs Yesterday Topline Summary:","today","yesterday","TTN","YTN"),
            ("MTD vs LMTD Topline Summary (Amounts are in ₹ Cr):","mtd","lmtd","MTD","LMTD"),
        ],
    )
    log.info("HOURLY OPEN — %s", subj)
    from airflow.utils.email import send_email
    send_email(to=EMAIL_TO, subject=subj, html_content=body)


###########################################################################
#  MAILER 2 — Daily Open Funnel  (9 AM IST)
###########################################################################

def send_daily_open_mail(**ctx):
    now = datetime.now()
    if now.hour != DAILY_MAIL_HOUR_IST:
        log.info("Skipping daily open mailer (hour %s != %s)", now.hour, DAILY_MAIL_HOUR_IST)
        return
    _do_send_daily_open()

def _do_send_daily_open():
    engine = get_engine()
    import pandas as pd
    now = datetime.now()

    overall    = pd.read_sql(f"SELECT * FROM {SCHEMA}.lr_overall", engine)
    lenderwise = pd.read_sql(f"SELECT * FROM {SCHEMA}.lr_lenderwise", engine)
    unique_tof = pd.read_sql(f"SELECT * FROM {SCHEMA}.lr_unique_tof WHERE time_window IN ('t_minus_1','t_minus_2','mtd','lmtd')", engine)

    t1 = (now - timedelta(days=1)).strftime("%d-%b-%Y")
    subj = f"Daily Lending Summary - EMI Open Funnel BBK || {t1}"
    body = _build_body(
        overall, lenderwise, unique_tof,
        windows=["t_minus_1","mtd","lmtd"],
        wlabels={"t_minus_1":"T-1","mtd":"MTD","lmtd":"LMTD"},
        utypes=[("ALL","Overall"),("OLD","Repeat"),("NEW","New")],
        topline_cfgs=[
            (f"T-1 Vs T-2 Topline Summary (Amounts are in ₹ Cr):","t_minus_1","t_minus_2","T-1","T-2"),
            ("MTD vs LMTD Topline Summary (Amounts are in ₹ Cr):","mtd","lmtd","MTD","LMTD"),
        ],
    )
    log.info("DAILY OPEN — %s", subj)
    from airflow.utils.email import send_email
    send_email(to=EMAIL_TO, subject=subj, html_content=body)


###########################################################################
#  MAILER 3 — Daily Closed Funnel  (9 AM IST)
###########################################################################

def send_closed_daily_mail(**ctx):
    now = datetime.now()
    if now.hour != DAILY_MAIL_HOUR_IST:
        log.info("Skipping daily closed mailer (hour %s != %s)", now.hour, DAILY_MAIL_HOUR_IST)
        return
    _do_send_closed_daily()

def _do_send_closed_daily():
    engine = get_engine()
    import pandas as pd
    now = datetime.now()

    overall    = pd.read_sql(f"SELECT * FROM {SCHEMA}.lrc_overall", engine)
    lenderwise = pd.read_sql(f"SELECT * FROM {SCHEMA}.lrc_lenderwise", engine)
    unique_tof = pd.read_sql(f"SELECT * FROM {SCHEMA}.lr_unique_tof WHERE time_window IN ('t_minus_1','t_minus_2','mtd','lmtd')", engine)

    t1 = (now - timedelta(days=1)).strftime("%d-%b-%Y")
    subj = f"Daily Lending Summary - EMI Closed Funnel BBK || {t1}"
    body = _build_body(
        overall, lenderwise, unique_tof,
        windows=["t_minus_1","mtd","lmtd"],
        wlabels={"t_minus_1":"T-1","mtd":"MTD","lmtd":"LMTD"},
        utypes=[("ALL","Overall"),("OLD","Repeat"),("NEW","New")],
        topline_cfgs=[
            (f"T-1 Vs T-2 Topline Summary (Amounts are in ₹ Cr):","t_minus_1","t_minus_2","T-1","T-2"),
            ("MTD vs LMTD Topline Summary (Amounts are in ₹ Cr):","mtd","lmtd","MTD","LMTD"),
        ],
    )
    log.info("DAILY CLOSED — %s", subj)
    from airflow.utils.email import send_email
    send_email(to=EMAIL_TO, subject=subj, html_content=body)


###########################################################################
#  FULL PIPELINE  (used by test tasks to run everything end-to-end)
###########################################################################

def _run_full_pipeline():
    """Run every step from scratch: lenders → base → steps → summaries."""
    log.info("PIPELINE — creating lender ref table")
    create_lenders()

    log.info("PIPELINE — creating base table")
    create_base()

    log.info("PIPELINE — creating step tables")
    create_address()
    create_journey()
    create_lender_det()
    create_offer()
    create_offer_accept()
    create_kyc()
    create_bank()
    create_nach()
    create_sanction()
    create_drawdown()

    log.info("PIPELINE — creating open funnel summaries")
    create_open_lenderwise()
    create_open_overall()

    log.info("PIPELINE — creating closed funnel summaries")
    create_closed_lenderwise()
    create_closed_overall()

    log.info("PIPELINE — creating unique TOF")
    create_unique_tof()

    log.info("PIPELINE — done")


###########################################################################
#  TEST TASKS — trigger these to force-fire daily emails right now
#
#  These run the FULL pipeline (base → steps → summaries) then send.
#  Safe to trigger standalone — no upstream dependency needed.
###########################################################################

def test_daily_open_mail(**ctx):
    log.info("TEST — full pipeline + daily open email")
    _run_full_pipeline()
    _do_send_daily_open()

def test_closed_daily_mail(**ctx):
    log.info("TEST — full pipeline + daily closed email")
    _run_full_pipeline()
    _do_send_closed_daily()


###########################################################################
#  DAG
###########################################################################

default_args = {"owner": "analytics", "retries": 2, "retry_delay": timedelta(minutes=5)}

dag = DAG(
    dag_id="lending_report",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["lending", "report"],
)

# ── Step tables (shared by open + closed) ────────────────────────────
t0  = PythonOperator(task_id="create_lenders",      python_callable=create_lenders,      dag=dag)
t1  = PythonOperator(task_id="create_base",          python_callable=create_base,          dag=dag)
t2  = PythonOperator(task_id="create_address",       python_callable=create_address,       dag=dag)
t3  = PythonOperator(task_id="create_journey",       python_callable=create_journey,       dag=dag)
t4  = PythonOperator(task_id="create_lender_det",    python_callable=create_lender_det,    dag=dag)
t5  = PythonOperator(task_id="create_offer",         python_callable=create_offer,         dag=dag)
t6  = PythonOperator(task_id="create_offer_accept",  python_callable=create_offer_accept,  dag=dag)
t7  = PythonOperator(task_id="create_kyc",           python_callable=create_kyc,           dag=dag)
t8  = PythonOperator(task_id="create_bank",          python_callable=create_bank,          dag=dag)
t9  = PythonOperator(task_id="create_nach",          python_callable=create_nach,          dag=dag)
t10 = PythonOperator(task_id="create_sanction",      python_callable=create_sanction,      dag=dag)
t11 = PythonOperator(task_id="create_drawdown",      python_callable=create_drawdown,      dag=dag)

# ── Open funnel summaries ────────────────────────────────────────────
t12 = PythonOperator(task_id="open_lenderwise",      python_callable=create_open_lenderwise,  dag=dag)
t13 = PythonOperator(task_id="open_overall",         python_callable=create_open_overall,     dag=dag)
t14 = PythonOperator(task_id="unique_tof",           python_callable=create_unique_tof,       dag=dag)

# ── Closed funnel summaries ──────────────────────────────────────────
t15 = PythonOperator(task_id="closed_lenderwise",    python_callable=create_closed_lenderwise, dag=dag)
t16 = PythonOperator(task_id="closed_overall",       python_callable=create_closed_overall,    dag=dag)

# ── Mailers ──────────────────────────────────────────────────────────
t17 = PythonOperator(task_id="send_hourly_open",     python_callable=send_hourly_mail,         dag=dag)
t18 = PythonOperator(task_id="send_daily_open",      python_callable=send_daily_open_mail,     dag=dag)
t19 = PythonOperator(task_id="send_daily_closed",    python_callable=send_closed_daily_mail,   dag=dag)

# ── Test tasks (trigger manually to force-fire daily emails) ─────────
t20 = PythonOperator(task_id="test_daily_open",      python_callable=test_daily_open_mail,     dag=dag)
t21 = PythonOperator(task_id="test_daily_closed",    python_callable=test_closed_daily_mail,   dag=dag)

# ── Wiring ───────────────────────────────────────────────────────────
#
#  lenders → base → [address, journey, lender_det, offer→offer_accept,
#                     kyc, bank, nach, sanction, drawdown]
#                          │
#                          ├──► open_lenderwise → open_overall ─┐
#                          │                                    ├──► send_hourly_open
#                          │    unique_tof ─────────────────────┤    send_daily_open
#                          │                                    │
#                          └──► closed_lenderwise → closed_overall → send_daily_closed

t0 >> t1
t1 >> [t2, t3, t4, t5, t7, t8, t9, t10, t11]
t5 >> t6

all_steps = [t2, t3, t4, t6, t7, t8, t9, t10, t11]

all_steps >> t12 >> t13
all_steps >> t14
all_steps >> t15 >> t16

[t13, t14] >> t17
[t13, t14] >> t18
[t16, t14] >> t19
