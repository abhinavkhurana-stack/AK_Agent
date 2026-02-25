"""
===============================================================================
  LENDING REPORT FRAMEWORK — CONFIGURATION
===============================================================================

  HOW TO ADD A NEW FLDG LENDER:
  ─────────────────────────────
  1. Copy any existing FLDG entry in LENDERS.
  2. Set a unique 'key' (lowercase, no spaces), partner_id, display_name,
     and eligible_partners_csv.
  3. Done. All standard funnel steps auto-apply.

  HOW TO ADD A DISTRIBUTION LENDER:
  ─────────────────────────────────
  1. Add an entry with type="DISTRIBUTION".
  2. Provide 'custom_sql' dict — override only the steps whose source
     tables differ from the standard FLDG tables.
  3. Steps not overridden will fall back to the FLDG template.

  HOW TO ADD / REMOVE / REORDER FUNNEL STEPS:
  ────────────────────────────────────────────
  1. Add or remove entries in FUNNEL_STEPS below.
  2. Change 'order' to reposition (lower = earlier in funnel).
  3. Set active=False to temporarily disable any step.

  OPEN vs CLOSED FUNNEL:
  ──────────────────────
  Each step has a 'depends_on' field:
    • "base"         → open funnel (step counted independently)
    • "<step_key>"   → closed funnel (step requires prior step)
  Change depends_on to switch between open and closed funnel per step.

===============================================================================
"""

# ── DATABASE ─────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": "data-analytics-mysql-prod.mbkinternal.in",
    "user": "analytics",
    "password": "vsn%400pl3TYujk23(o",
    "port": 3308,
    "database": "mobinew",
}

# ── TABLE PREFIX ─────────────────────────────────────────────────────────────
# Hourly tables → {TABLE_PREFIX}h_…   Daily tables → {TABLE_PREFIX}d_…
TABLE_PREFIX = "lr"

# ── AMOUNTS DIVISOR ──────────────────────────────────────────────────────────
# Amounts are divided by this to convert to Crores (₹).
AMOUNT_DIVISOR = 10_000_000  # 1 Cr

# ═════════════════════════════════════════════════════════════════════════════
#  LENDERS — add / remove entries here
# ═════════════════════════════════════════════════════════════════════════════
LENDERS = [
    {
        "key": "smfg",
        "partner_id": 4,
        "display_name": "SMFG",
        "type": "FLDG",
        "eligible_partners_csv": "4",
        "active": True,
    },
    {
        "key": "fullerton",
        "partner_id": 7,
        "display_name": "Fullerton",
        "type": "FLDG",
        "eligible_partners_csv": "7",
        "active": True,
    },

    # ── TEMPLATE: NEW FLDG LENDER ────────────────────────────────────────
    # {
    #     "key": "new_lender",
    #     "partner_id": 99,
    #     "display_name": "New Lender",
    #     "type": "FLDG",
    #     "eligible_partners_csv": "99",
    #     "active": True,
    # },

    # ── TEMPLATE: DISTRIBUTION LENDER ────────────────────────────────────
    # Data lives in separate schema / tables.  Override only the steps
    # whose SQL differs from the standard FLDG template.
    # {
    #     "key": "dist_partner",
    #     "partner_id": 50,
    #     "display_name": "Distribution Partner",
    #     "type": "DISTRIBUTION",
    #     "active": True,
    #     "custom_sql": {
    #         "basic_details": """
    #             SELECT DISTINCT
    #                 app_id   AS mbkloanid,
    #                 created  AS createdat,
    #                 user_id  AS memberid
    #             FROM dist_schema.applications
    #             WHERE created >= '{date_start}' AND created < '{date_end}'
    #         """,
    #         "offer": """
    #             SELECT DISTINCT app_id AS mbkloanid
    #             FROM dist_schema.offers o
    #             INNER JOIN {base_table} b ON o.app_id = b.mbkloanid
    #         """,
    #         "sanction": """
    #             SELECT app_id AS mbkloanid, SUM(sanction_amount) AS amount
    #             FROM dist_schema.sanctions s
    #             INNER JOIN {base_table} b ON s.app_id = b.mbkloanid
    #             GROUP BY s.app_id
    #         """,
    #         "drawdown": """
    #             SELECT app_id AS mbkloanid, SUM(disburse_amount) AS amount
    #             FROM dist_schema.disbursals d
    #             INNER JOIN {base_table} b ON d.app_id = b.mbkloanid
    #             WHERE d.status = 'SUCCESS'
    #             GROUP BY d.app_id
    #         """,
    #     },
    # },
]


# ═════════════════════════════════════════════════════════════════════════════
#  FUNNEL STEPS — add / remove / reorder here
# ═════════════════════════════════════════════════════════════════════════════
#
#  Fields per step:
#    key           – unique machine name (used in table names)
#    label         – display name in reports
#    order         – position in funnel (lower = earlier); gaps encouraged
#    depends_on    – "base" for open funnel; another step key for closed
#    has_amount    – True if the step also produces a monetary amount
#    amount_label  – display name for the amount column (if has_amount)
#    active        – False to skip this step entirely
#    fldg_sql      – SQL template for FLDG lenders
#
#  Available placeholders in fldg_sql:
#    {base_table}              – always the lender's base table
#    {join_table}              – resolved from depends_on
#    {partner_id}              – numeric lender partner ID
#    {eligible_partners_csv}   – CSV string for FIND_IN_SET
#
# ═════════════════════════════════════════════════════════════════════════════

FUNNEL_STEPS = [

    # ── 1. BASE (special: creates the date-filtered population) ──────────
    {
        "key": "basic_details",
        "label": "Basic Details",
        "order": 100,
        "depends_on": None,
        "has_amount": False,
        "active": True,
        "fldg_sql": None,  # handled by dedicated base-table builder
    },

    # ── 2. ADDRESS ───────────────────────────────────────────────────────
    {
        "key": "address",
        "label": "Address Details",
        "order": 200,
        "depends_on": "base",
        "has_amount": False,
        "active": True,
        "fldg_sql": """
            SELECT DISTINCT mpd.mbkloanid
            FROM lending.memberprofiledetails mpd
            INNER JOIN {base_table} b ON mpd.mbkloanid = b.mbkloanid
            WHERE mpd.PermanentPincode IS NOT NULL
              AND mpd.PermanentPincode > 0
        """,
    },

    # ── 3-9. JOURNEY STAGES ─────────────────────────────────────────────
    {
        "key": "lpa_run",
        "label": "LPA Run",
        "order": 300,
        "depends_on": "base",
        "has_amount": False,
        "active": True,
        "fldg_sql": """
            SELECT DISTINCT u.mbk_loan_id AS mbkloanid
            FROM lending.user_journey_status u
            INNER JOIN {join_table} j ON u.mbk_loan_id = j.mbkloanid
            WHERE u.user_journey_status_stage = 'PreLenderSanityChecks'
              AND FIND_IN_SET('{eligible_partners_csv}', u.eligible_partners) > 0
        """,
    },
    {
        "key": "lpa_pass",
        "label": "LPA Pass",
        "order": 310,
        "depends_on": "base",
        "has_amount": False,
        "active": True,
        "fldg_sql": """
            SELECT DISTINCT u.mbk_loan_id AS mbkloanid
            FROM lending.user_journey_status u
            INNER JOIN {join_table} j ON u.mbk_loan_id = j.mbkloanid
            WHERE u.user_journey_status_stage = 'PostLenderSanityChecks'
              AND FIND_IN_SET('{eligible_partners_csv}', u.eligible_partners) > 0
        """,
    },
    {
        "key": "pre_bre",
        "label": "Pre BRE",
        "order": 320,
        "depends_on": "base",
        "has_amount": False,
        "active": True,
        "fldg_sql": """
            SELECT DISTINCT u.mbk_loan_id AS mbkloanid
            FROM lending.user_journey_status u
            INNER JOIN {join_table} j ON u.mbk_loan_id = j.mbkloanid
            WHERE u.user_journey_status_stage = 'PreBreFraudRulesSuccess'
              AND FIND_IN_SET('{eligible_partners_csv}', u.eligible_partners) > 0
        """,
    },
    {
        "key": "bureau_pull",
        "label": "Bureau Pull",
        "order": 330,
        "depends_on": "base",
        "has_amount": False,
        "active": True,
        "fldg_sql": """
            SELECT DISTINCT u.mbk_loan_id AS mbkloanid
            FROM lending.user_journey_status u
            INNER JOIN {join_table} j ON u.mbk_loan_id = j.mbkloanid
            WHERE u.user_journey_status_stage = 'BureauPullSuccess'
              AND FIND_IN_SET('{eligible_partners_csv}', u.eligible_partners) > 0
        """,
    },
    {
        "key": "bre_success",
        "label": "BRE Success",
        "order": 340,
        "depends_on": "base",
        "has_amount": False,
        "active": True,
        "fldg_sql": """
            SELECT DISTINCT u.mbk_loan_id AS mbkloanid
            FROM lending.user_journey_status u
            INNER JOIN {join_table} j ON u.mbk_loan_id = j.mbkloanid
            WHERE u.user_journey_status_stage = 'BreSuccess'
              AND FIND_IN_SET('{eligible_partners_csv}', u.eligible_partners) > 0
        """,
    },
    {
        "key": "post_bre",
        "label": "Post BRE",
        "order": 350,
        "depends_on": "base",
        "has_amount": False,
        "active": True,
        "fldg_sql": """
            SELECT DISTINCT u.mbk_loan_id AS mbkloanid
            FROM lending.user_journey_status u
            INNER JOIN {join_table} j ON u.mbk_loan_id = j.mbkloanid
            WHERE u.user_journey_status_stage = 'PostBreFraudRuleSuccess'
              AND FIND_IN_SET('{eligible_partners_csv}', u.eligible_partners) > 0
        """,
    },
    {
        "key": "pan_validation",
        "label": "PAN Validation",
        "order": 360,
        "depends_on": "base",
        "has_amount": False,
        "active": True,
        "fldg_sql": """
            SELECT DISTINCT u.mbk_loan_id AS mbkloanid
            FROM lending.user_journey_status u
            INNER JOIN {join_table} j ON u.mbk_loan_id = j.mbkloanid
            WHERE u.user_journey_status_stage = 'PanKycValidationDone'
              AND FIND_IN_SET('{eligible_partners_csv}', u.eligible_partners) > 0
        """,
    },

    # ── 10. LENDER DETAILS ───────────────────────────────────────────────
    {
        "key": "lender_details",
        "label": "Lender Details",
        "order": 400,
        "depends_on": "base",
        "has_amount": False,
        "active": True,
        "fldg_sql": """
            SELECT DISTINCT lad.mbk_loan_id AS mbkloanid
            FROM lending.lender_additional_details lad
            INNER JOIN {base_table} b ON lad.mbk_loan_id = b.mbkloanid
            WHERE lad.lending_partner = {partner_id}
        """,
    },

    # ── 11. OFFER ────────────────────────────────────────────────────────
    {
        "key": "offer",
        "label": "Offer",
        "order": 500,
        "depends_on": "base",
        "has_amount": False,
        "active": True,
        "fldg_sql": """
            SELECT DISTINCT c.mbkloanid
            FROM lending.creditline c
            INNER JOIN {base_table} b ON c.mbkloanid = b.mbkloanid
            WHERE c.lendingpartnerid = {partner_id}
        """,
    },

    # ── 12. OFFER ACCEPTED ───────────────────────────────────────────────
    #   depends_on="offer" ensures we only count acceptances of THIS
    #   lender's offers (closed to the offer step for lender specificity).
    {
        "key": "offer_accepted",
        "label": "Offer Accepted",
        "order": 510,
        "depends_on": "offer",
        "has_amount": False,
        "active": True,
        "fldg_sql": """
            SELECT DISTINCT ua.mbkloanid
            FROM lending.useracceptancedetails ua
            INNER JOIN {join_table} j ON ua.mbkloanid = j.mbkloanid
            WHERE ua.stage = 'SAVE_OFFER'
        """,
    },

    # ── 13. KYC ──────────────────────────────────────────────────────────
    {
        "key": "kyc",
        "label": "KYC",
        "order": 600,
        "depends_on": "base",
        "has_amount": False,
        "active": True,
        "fldg_sql": """
            SELECT DISTINCT uki.mbk_loan_id AS mbkloanid
            FROM lending.user_kyc_info uki
            INNER JOIN {base_table} b ON uki.mbk_loan_id = b.mbkloanid
            WHERE uki.lending_partner = {partner_id}
        """,
    },

    # ── 14. BANK ─────────────────────────────────────────────────────────
    {
        "key": "bank",
        "label": "Bank",
        "order": 700,
        "depends_on": "base",
        "has_amount": False,
        "active": True,
        "fldg_sql": """
            SELECT DISTINCT bv.mbkloanid
            FROM lending.bankverification bv
            INNER JOIN {base_table} b ON bv.mbkloanid = b.mbkloanid
            WHERE bv.bankverified = 1
        """,
    },

    # ── 15. NACH ─────────────────────────────────────────────────────────
    {
        "key": "nach",
        "label": "NACH",
        "order": 800,
        "depends_on": "base",
        "has_amount": False,
        "active": True,
        "fldg_sql": """
            SELECT DISTINCT nr.mbkloanid
            FROM lending.NachRegistration nr
            INNER JOIN {base_table} b ON nr.mbkloanid = b.mbkloanid
            WHERE nr.status IN ('eMandateSuccess', 'pNachSuccess')
        """,
    },

    # ── 16. SANCTION ─────────────────────────────────────────────────────
    {
        "key": "sanction",
        "label": "Sanction",
        "order": 900,
        "depends_on": "base",
        "has_amount": True,
        "amount_label": "Sanction Amount",
        "active": True,
        "fldg_sql": """
            SELECT c.mbkloanid, SUM(c.sanctionedlineamount) AS amount
            FROM lending.creditline c
            INNER JOIN {base_table} b ON c.mbkloanid = b.mbkloanid
            WHERE c.lendingpartnerid = {partner_id}
              AND c.status LIKE '%%,11%%'
            GROUP BY c.mbkloanid
        """,
    },

    # ── 17. DRAWDOWN ─────────────────────────────────────────────────────
    {
        "key": "drawdown",
        "label": "Drawdown",
        "order": 1000,
        "depends_on": "base",
        "has_amount": True,
        "amount_label": "Drawdown Amount",
        "active": True,
        "fldg_sql": """
            SELECT d.mbkloanid, SUM(d.drawamount) AS amount
            FROM lending.drawdown d
            INNER JOIN {base_table} b ON d.mbkloanid = b.mbkloanid
            WHERE d.drawdownstatus IN (4, 17)
            GROUP BY d.mbkloanid
        """,
    },

    # ── TEMPLATE: NEW CUSTOM STEP ────────────────────────────────────────
    # {
    #     "key": "video_kyc",
    #     "label": "Video KYC",
    #     "order": 650,              # between KYC (600) and Bank (700)
    #     "depends_on": "base",      # "base" for open, step key for closed
    #     "has_amount": False,
    #     "active": True,
    #     "fldg_sql": """
    #         SELECT DISTINCT vk.mbk_loan_id AS mbkloanid
    #         FROM lending.video_kyc vk
    #         INNER JOIN {base_table} b ON vk.mbk_loan_id = b.mbkloanid
    #         WHERE vk.status = 'COMPLETED'
    #           AND vk.lending_partner = {partner_id}
    #     """,
    # },
]


# ═════════════════════════════════════════════════════════════════════════════
#  HELPERS  (no need to edit below)
# ═════════════════════════════════════════════════════════════════════════════

def get_active_lenders():
    """Return only active lenders, sorted by key."""
    return [l for l in LENDERS if l.get("active", True)]


def get_active_steps():
    """Return only active steps, sorted by order."""
    return sorted(
        [s for s in FUNNEL_STEPS if s.get("active", True)],
        key=lambda s: s["order"],
    )


def get_non_base_steps():
    """Active steps excluding the base (basic_details) step."""
    return [s for s in get_active_steps() if s["key"] != "basic_details"]
