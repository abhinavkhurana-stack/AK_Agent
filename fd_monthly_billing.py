"""
Fixed Deposit Monthly Billing Calculator (v4)
==============================================
Calculates monthly billing revenue per lender/partner from FD data.

Billing methodology:
  BANKS  (Unity, Suryoday, Shivalik):
      billing = amount × rate × active_days_in_month / 365
      Active window depends on fd_status:
        OPEN / MATURED / IN_PROGRESS → [created_at … maturity_at]
        WITHDRAW → [created_at … min(updated_at, maturity_at)]
      Both endpoints INCLUSIVE.  Billing is spread across every
      calendar month overlapping that window.

  NBFCs  (Bajaj Finance Ltd, Shriram Finance, Mahindra Finance):
      billing = amount × rate   (UPFRONT, all in booking month)
      Per agreement: "Commissions on the NBFCs FD would be paid upfront
      at the time of distribution of FD."

Rate tables sourced from:
    Blostem × MobiKwik FD Distribution Agreement (Annexure II A)

    Bajaj  – rates are GST-INCLUSIVE → payable = agreement_rate / 1.18
             Volume slab: Upto 2.5cr
    Shriram – rates are GST-EXCLUSIVE, Upto 5cr slab
    Mahindra – rates are GST-EXCLUSIVE, Upto 2cr slab
    Unity   → 0.50% flat
    Suryoday→ 0.35% flat
    Shivalik→ 0.15% flat
"""

import calendar
import warnings
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

GST_DIVISOR = 1.18

BANK_PARTNERS = {"Unity", "Suryoday", "Shivalik"}
NBFC_PARTNERS = {"Bajaj Finance Ltd", "Shriram Finance", "Mahindra Finance"}

FLAT_RATES = {
    "Unity": 0.50 / 100,
    "Suryoday": 0.35 / 100,
    "Shivalik": 0.15 / 100,
}

BAJAJ_RATES_INCL_GST = {
    (12, 14): 0.47,
    (15, 15): 0.43,
    (16, 23): 0.47,
    (24, 35): 0.68,
    (36, 43): 0.98,
    (44, 44): 0.89,
    (45, 47): 0.98,
    (48, 60): 1.32,
}

SHRIRAM_RATES = {
    12: 0.26, 18: 0.26,
    24: 0.51, 30: 0.51,
    36: 1.02, 42: 1.02,
    50: 1.11, 60: 1.19,
}

MAHINDRA_RATES = {
    12: 0.10, 24: 0.34,
    36: 0.68, 48: 0.77,
    60: 0.85,
}


def _bajaj_rate(tenure_months):
    for (lo, hi), pct in BAJAJ_RATES_INCL_GST.items():
        if lo <= tenure_months <= hi:
            return (pct / 100) / GST_DIVISOR
    if tenure_months < 12:
        return (0.47 / 100) / GST_DIVISOR
    return (1.32 / 100) / GST_DIVISOR


def _nearest_key_rate(table, tenure):
    if tenure in table:
        return table[tenure] / 100
    keys = sorted(table.keys())
    best = min(keys, key=lambda k: (abs(k - tenure), k))
    return table[best] / 100


def get_rate(partner, period_months):
    period = int(round(period_months)) if pd.notna(period_months) and period_months > 0 else 0
    if partner in FLAT_RATES:
        return FLAT_RATES[partner]
    if partner == "Bajaj Finance Ltd":
        return _bajaj_rate(period if period > 0 else 12)
    if partner == "Shriram Finance":
        return _nearest_key_rate(SHRIRAM_RATES, period if period > 0 else 12)
    if partner == "Mahindra Finance":
        return _nearest_key_rate(MAHINDRA_RATES, period if period > 0 else 12)
    return 0


def monthly_breakdown(start_date, end_date_incl):
    """Yield (year, month, days) for every calendar month the FD overlaps."""
    end_excl = end_date_incl + timedelta(days=1)
    if end_excl <= start_date:
        return
    cur = start_date.replace(day=1)
    while cur < end_excl:
        y, m = cur.year, cur.month
        month_first = date(y, m, 1)
        month_end_excl = date(y, m, calendar.monthrange(y, m)[1]) + timedelta(days=1)
        active_start = max(start_date, month_first)
        active_end_excl = min(end_excl, month_end_excl)
        days = (active_end_excl - active_start).days
        if days > 0:
            yield y, m, days
        if m == 12:
            cur = date(y + 1, 1, 1)
        else:
            cur = date(y, m + 1, 1)


def load_partner_bank_data(partner_file):
    """Load partner bank data (Unity/Suryoday) Jul-Dec 2025 from the partner Excel.

    Uses the Summary sheet for billing totals (MobiKwik payable) and detail
    sheets for FD counts.
    """
    xls = pd.ExcelFile(partner_file)
    bank_partner_data = {}

    summary = pd.read_excel(xls, "Summary", header=None)
    months = ["2025-07", "2025-08", "2025-09", "2025-10", "2025-11", "2025-12"]
    partner_summary_rows = {
        "Suryoday": 1,
        "Unity": 2,
    }
    for partner, row_idx in partner_summary_rows.items():
        for i, ml in enumerate(months):
            val = summary.iloc[row_idx, 4 + i]
            try:
                val = float(val)
            except (ValueError, TypeError):
                val = 0
            bank_partner_data[(partner, ml)] = {"billing": round(val, 2), "fd_count": 0}

    detail_sheet_map = {
        "Unity-July": ("Unity", "2025-07"),
        "Unity-August": ("Unity", "2025-08"),
        "Unity-September": ("Unity", "2025-09"),
        "Unity-October": ("Unity", "2025-10"),
        "Unity-November": ("Unity", "2025-11"),
        "Unity-December": ("Unity", "2025-12"),
        "Suryoday-July": ("Suryoday", "2025-07"),
        "Suryoday-August": ("Suryoday", "2025-08"),
        "Suryoday-September": ("Suryoday", "2025-09"),
        "Suryoday-October": ("Suryoday", "2025-10"),
        "Suryoday-November": ("Suryoday", "2025-11"),
        "Suryoday-December": ("Suryoday", "2025-12"),
    }
    for sheet_name, (partner, month_label) in detail_sheet_map.items():
        if sheet_name not in xls.sheet_names:
            continue
        df = pd.read_excel(xls, sheet_name)
        if (partner, month_label) in bank_partner_data:
            bank_partner_data[(partner, month_label)]["fd_count"] = len(df)
        else:
            bank_partner_data[(partner, month_label)] = {
                "billing": 0, "fd_count": len(df),
            }

    xls.close()
    return bank_partner_data


def load_partner_nbfc_data(partner_file, nbfc_jan_jul_file):
    """Load partner NBFC data from both files.

    Jan-Jul file: Bajaj amounts are GST-inclusive → divide by 1.18.
                  Shriram/Mahindra are GST-exclusive → use as-is.
    Aug-Dec file: All amounts are already ex-GST (Bajaj rates pre-divided).
    """
    nbfc_partner_data = {}

    xls_jj = pd.ExcelFile(nbfc_jan_jul_file)
    mbk = pd.read_excel(xls_jj, "MBK_NBFC")
    col_map = {1: "2025-01", 2: "2025-02", 3: "2025-03", 4: "2025-04",
               5: "2025-05", 6: "2025-06", 7: "2025-07"}

    for idx, row in mbk.iterrows():
        partner_name = row.iloc[0]
        if pd.isna(partner_name):
            continue
        partner_name = str(partner_name).strip()
        if partner_name not in NBFC_PARTNERS:
            continue
        is_gst_inclusive = (partner_name == "Bajaj Finance Ltd")
        for col_idx in range(1, 8):
            month_label = col_map[col_idx]
            val = row.iloc[col_idx]
            try:
                val = float(val)
            except (ValueError, TypeError):
                val = 0
            if is_gst_inclusive and val > 0:
                val = val / GST_DIVISOR
            nbfc_partner_data[(partner_name, month_label)] = {
                "billing": round(val, 2),
            }
    xls_jj.close()

    xls_ad = pd.ExcelFile(partner_file)

    nbfc_ao = pd.read_excel(xls_ad, "NBFC Aug-Oct 2025")
    month_num_col = "Month.1" if "Month.1" in nbfc_ao.columns else "Month"
    for (issuer, m_num), grp in nbfc_ao.groupby(["Issuer", month_num_col]):
        m_num = int(m_num)
        ml = f"2025-{m_num:02d}"
        total = grp["Commission Payable"].sum()
        issuer = str(issuer).strip()
        nbfc_partner_data[(issuer, ml)] = {"billing": round(total, 2)}

    nbfc_nov = pd.read_excel(xls_ad, "NBFC_November")
    nbfc_nov = nbfc_nov.dropna(subset=["Partner Name"])
    for issuer, grp in nbfc_nov.groupby("Issuer"):
        total = grp["Payabales without GST"].sum()
        nbfc_partner_data[(str(issuer).strip(), "2025-11")] = {"billing": round(total, 2)}

    nbfc_dec = pd.read_excel(xls_ad, "NBFC_December")
    nbfc_dec = nbfc_dec.dropna(subset=["Partner Name"])
    for issuer, grp in nbfc_dec.groupby("Issuer"):
        total = grp["Payable without GST"].sum()
        nbfc_partner_data[(str(issuer).strip(), "2025-12")] = {"billing": round(total, 2)}

    xls_ad.close()
    return nbfc_partner_data


def load_partner_nbfc_fd_counts(partner_file, nbfc_jan_jul_file):
    """Load FD counts from partner NBFC data."""
    counts = {}

    xls_jj = pd.ExcelFile(nbfc_jan_jul_file)
    fd_detail = pd.read_excel(xls_jj, "Fixed Deposit")
    for (issuer, month), grp in fd_detail.groupby(["Issuer", "Month"]):
        ml = f"2025-{int(month):02d}"
        counts[(str(issuer).strip(), ml)] = len(grp)
    xls_jj.close()

    xls_ad = pd.ExcelFile(partner_file)
    nbfc_ao = pd.read_excel(xls_ad, "NBFC Aug-Oct 2025")
    month_num_col = "Month.1" if "Month.1" in nbfc_ao.columns else "Month"
    for (issuer, m_num), grp in nbfc_ao.groupby(["Issuer", month_num_col]):
        ml = f"2025-{int(m_num):02d}"
        counts[(str(issuer).strip(), ml)] = len(grp)

    nbfc_nov = pd.read_excel(xls_ad, "NBFC_November").dropna(subset=["Partner Name"])
    for issuer, grp in nbfc_nov.groupby("Issuer"):
        counts[(str(issuer).strip(), "2025-11")] = len(grp)

    nbfc_dec = pd.read_excel(xls_ad, "NBFC_December").dropna(subset=["Partner Name"])
    for issuer, grp in nbfc_dec.groupby("Issuer"):
        counts[(str(issuer).strip(), "2025-12")] = len(grp)

    xls_ad.close()
    return counts


def main():
    ref_file = "extracted/ref/FD_Monthly_Billing/FD_Monthly_Ref.xlsx"
    partner_file = "Mobikwik_November_Banks-Jul-Dec_NBFC-Aug-Dec_030226.xlsx"
    nbfc_jan_jul_file = "NBFC Jan to July'25.xlsx"

    print("Loading FD data from reference Working Sheet...")
    cols_needed = [
        "transaction_id", "Partner", "fd_status", "amount",
        "billing_rate", "investment_period", "interest_rate",
        "created_at_ist", "maturity_at_ist", "updated_at_ist",
    ]
    df = pd.read_excel(ref_file, "Working Sheet", usecols=cols_needed)
    raw_count = len(df)
    print(f"Loaded {raw_count:,} FDs")

    df["created_at_ist"] = pd.to_datetime(df["created_at_ist"])
    df["maturity_at_ist"] = pd.to_datetime(df["maturity_at_ist"])
    df["updated_at_ist"] = pd.to_datetime(df["updated_at_ist"])
    df["investment_period"] = df["investment_period"].fillna(0)

    dup_count = raw_count - df["transaction_id"].nunique()
    df = df.drop_duplicates(subset="transaction_id", keep="first")

    bad_mask = df["maturity_at_ist"].isna() | (df["maturity_at_ist"] < df["created_at_ist"])
    fallback_count = bad_mask.sum()
    fallback_txn_ids = set(df.loc[bad_mask, "transaction_id"])
    df.loc[bad_mask, "maturity_at_ist"] = df.loc[bad_mask, "updated_at_ist"]

    still_bad = df["maturity_at_ist"].isna() | (df["maturity_at_ist"] < df["created_at_ist"])
    valid = df[~still_bad].copy()

    valid["billing_rate_new"] = valid.apply(
        lambda r: get_rate(r["Partner"], r["investment_period"]), axis=1
    )

    print(f"Unique FDs: {len(df):,} (removed {dup_count} duplicates)")
    print(f"Fallback: {fallback_count} used updated_at_ist as maturity")
    print(f"Processing: {len(valid):,} FDs")
    print(f"Partners: {valid['Partner'].value_counts().to_dict()}")

    rate_changes = valid[
        (valid["billing_rate_new"] - valid["billing_rate"]).abs() > 1e-7
    ]
    if len(rate_changes) > 0:
        print(f"\nRate corrections applied to {len(rate_changes)} FDs:")
        for p in rate_changes["Partner"].unique():
            sub = rate_changes[rate_changes["Partner"] == p]
            for _, r in sub.drop_duplicates(
                subset=["investment_period", "billing_rate", "billing_rate_new"]
            ).iterrows():
                print(
                    f"  {p} {int(r['investment_period'])}mo: "
                    f"{r['billing_rate']:.6f} → {r['billing_rate_new']:.6f}"
                )

    TARGET_END = date(2026, 2, 28)

    print("\nComputing monthly billing...")
    withdraw_count = (valid["fd_status"] == "WITHDRAW").sum()
    print(f"  WITHDRAW FDs: {withdraw_count} (will use withdrawal date as end)")
    rows = []
    for _, row in valid.iterrows():
        created = row["created_at_ist"].date()
        maturity = row["maturity_at_ist"].date()
        partner = row["Partner"]
        amount = row["amount"]
        rate = row["billing_rate_new"]
        txn_id = row["transaction_id"]
        status = row["fd_status"]
        is_nbfc = partner in NBFC_PARTNERS

        if status == "WITHDRAW":
            withdrawn_at = row["updated_at_ist"].date()
            effective_end = min(withdrawn_at, maturity)
        else:
            effective_end = maturity

        if is_nbfc:
            booking_month = f"{created.year}-{created.month:02d}"
            billing_amount = round(amount * rate, 4)
            rows.append({
                "partner": partner,
                "year": created.year,
                "month": created.month,
                "month_label": booking_month,
                "transaction_id": txn_id,
                "fd_status": status,
                "amount": amount,
                "billing_rate": rate,
                "active_days": 1,
                "billing_amount": billing_amount,
            })
        else:
            end_date = min(effective_end, TARGET_END)
            for y, m, days in monthly_breakdown(created, end_date):
                ml = f"{y}-{m:02d}"
                rows.append({
                    "partner": partner,
                    "year": y,
                    "month": m,
                    "month_label": ml,
                    "transaction_id": txn_id,
                    "fd_status": status,
                    "amount": amount,
                    "billing_rate": rate,
                    "active_days": days,
                    "billing_amount": round(amount * rate * days / 365, 4),
                })

    detail = pd.DataFrame(rows)
    print(f"Generated {len(detail):,} monthly line items")

    summary = (
        detail.groupby(["partner", "month_label"])
        .agg(
            fd_count=("transaction_id", "nunique"),
            total_amount=("amount", "sum"),
            total_active_days=("active_days", "sum"),
            total_billing=("billing_amount", "sum"),
        )
        .reset_index()
        .sort_values(["partner", "month_label"])
    )
    summary["total_billing"] = summary["total_billing"].round(2)

    grand = (
        summary.groupby("partner")
        .agg(
            months_spanned=("month_label", "nunique"),
            total_billing=("total_billing", "sum"),
        )
        .reset_index()
        .sort_values("total_billing", ascending=False)
    )
    grand["total_billing"] = grand["total_billing"].round(2)

    all_rated = set(FLAT_RATES) | NBFC_PARTNERS
    billable = summary[summary["partner"].isin(all_rated)]
    pivot = billable.pivot_table(
        index="month_label",
        columns="partner",
        values="total_billing",
        aggfunc="sum",
        fill_value=0,
    )
    pivot["Grand Total"] = pivot.sum(axis=1)
    pivot = pivot.round(2)

    print("Building Working Sheet...")
    all_months = sorted(detail["month_label"].unique())
    days_wide = detail.pivot_table(
        index="transaction_id",
        columns="month_label",
        values="active_days",
        aggfunc="sum",
        fill_value=0,
    )
    days_wide.columns = [f"Days_{c}" for c in days_wide.columns]
    bill_wide = detail.pivot_table(
        index="transaction_id",
        columns="month_label",
        values="billing_amount",
        aggfunc="sum",
        fill_value=0,
    )
    bill_wide.columns = [f"Billing_{c}" for c in bill_wide.columns]
    fd_totals = (
        detail.groupby("transaction_id")
        .agg(total_active_days=("active_days", "sum"), total_billing=("billing_amount", "sum"))
        .round(4)
    )

    source_cols = [
        "transaction_id", "Partner", "fd_status", "interest_rate",
        "investment_period", "created_at_ist", "maturity_at_ist", "updated_at_ist", "amount",
    ]
    available_cols = [c for c in source_cols if c in valid.columns]
    source = valid[available_cols].set_index("transaction_id")
    source["billing_rate"] = valid["billing_rate_new"].values
    source["used_fallback"] = source.index.isin(fallback_txn_ids)
    source["days_diff"] = (
        valid.set_index("transaction_id")["maturity_at_ist"]
        - valid.set_index("transaction_id")["created_at_ist"]
    ).dt.days

    working = source.join(fd_totals).join(days_wide).join(bill_wide).reset_index()
    static_cols = [
        "transaction_id", "Partner", "fd_status", "amount", "billing_rate",
        "investment_period", "interest_rate", "created_at_ist", "maturity_at_ist",
        "updated_at_ist", "used_fallback", "days_diff", "total_active_days", "total_billing",
    ]
    month_cols = []
    for m in all_months:
        month_cols += [f"Days_{m}", f"Billing_{m}"]
    existing = [c for c in static_cols + month_cols if c in working.columns]
    working = working[existing]

    print("Loading partner data for comparison...")
    bank_partner = load_partner_bank_data(partner_file)
    nbfc_partner = load_partner_nbfc_data(partner_file, nbfc_jan_jul_file)
    nbfc_fd_counts = load_partner_nbfc_fd_counts(partner_file, nbfc_jan_jul_file)

    print("Building Bank Comparison tab...")
    bank_comp_rows = []
    for partner in ["Unity", "Suryoday"]:
        for month in ["2025-07", "2025-08", "2025-09", "2025-10", "2025-11", "2025-12"]:
            our_row = summary[
                (summary["partner"] == partner) & (summary["month_label"] == month)
            ]
            our_bill = our_row["total_billing"].values[0] if len(our_row) else 0
            our_fds = int(our_row["fd_count"].values[0]) if len(our_row) else 0
            pdata = bank_partner.get((partner, month), {})
            ptr_bill = pdata.get("billing", 0)
            ptr_fds = pdata.get("fd_count", 0)
            diff = round(our_bill - ptr_bill, 2)
            diff_pct = round((our_bill / ptr_bill - 1) * 100, 1) if ptr_bill else 0
            bank_comp_rows.append({
                "Partner": partner,
                "Month": month,
                "Partner_Billing": round(ptr_bill, 2),
                "Partner_FD_Count": ptr_fds,
                "Our_Billing": round(our_bill, 2),
                "Our_FD_Count": our_fds,
                "Billing_Diff": diff,
                "Billing_Diff_Pct": diff_pct,
                "FD_Count_Diff": our_fds - ptr_fds,
                "Note": "",
            })
    bank_comp = pd.DataFrame(bank_comp_rows)

    print("Building NBFC Comparison tab...")
    nbfc_comp_rows = []
    nbfc_months = [f"2025-{m:02d}" for m in range(1, 13)]
    for partner in ["Shriram Finance", "Bajaj Finance Ltd", "Mahindra Finance"]:
        for month in nbfc_months:
            pdata = nbfc_partner.get((partner, month))
            if pdata is None:
                continue
            ptr_bill = pdata.get("billing", 0)
            ptr_fds = nbfc_fd_counts.get((partner, month), 0)
            our_row = summary[
                (summary["partner"] == partner) & (summary["month_label"] == month)
            ]
            our_bill = our_row["total_billing"].values[0] if len(our_row) else 0
            our_fds = int(our_row["fd_count"].values[0]) if len(our_row) else 0
            diff = round(our_bill - ptr_bill, 2)
            diff_pct = round((our_bill / ptr_bill - 1) * 100, 1) if ptr_bill else 0

            note = ""
            if abs(diff) > 1:
                if our_fds != ptr_fds:
                    note = f"FD count mismatch: ours={our_fds}, partner={ptr_fds}"
                else:
                    note = "Rate or amount difference"

            nbfc_comp_rows.append({
                "Partner": partner,
                "Month": month,
                "Partner_Billing": round(ptr_bill, 2),
                "Partner_FD_Count": ptr_fds,
                "Our_Billing": round(our_bill, 2),
                "Our_FD_Count": our_fds,
                "Billing_Diff": diff,
                "Billing_Diff_Pct": diff_pct,
                "FD_Count_Diff": our_fds - ptr_fds,
                "Note": note,
            })
    nbfc_comp = pd.DataFrame(nbfc_comp_rows)

    rate_ref_rows = [
        {"Partner": "Unity", "Tenure": "All", "Rate_Pct": 0.50,
         "Rate_Type": "Flat", "GST_Treatment": "N/A", "Source": "Agreement"},
        {"Partner": "Suryoday", "Tenure": "All", "Rate_Pct": 0.35,
         "Rate_Type": "Flat", "GST_Treatment": "N/A", "Source": "Agreement"},
        {"Partner": "Shivalik", "Tenure": "All", "Rate_Pct": 0.15,
         "Rate_Type": "Flat", "GST_Treatment": "N/A", "Source": "Estimated"},
    ]

    for tenure, pct in sorted(BAJAJ_RATES_INCL_GST.items(), key=lambda x: x[0][0]):
        lo, hi = tenure
        tstr = f"{lo} months" if lo == hi else f"{lo}-{hi} months"
        rate_excl = round(pct / GST_DIVISOR, 4)
        rate_ref_rows.append({
            "Partner": "Bajaj Finance Ltd",
            "Tenure": tstr,
            "Rate_Pct": rate_excl,
            "Rate_Type": "Upfront (excl GST)",
            "GST_Treatment": f"Agreement {pct}% incl GST / 1.18",
            "Source": "Agreement Annexure II A (Upto 2.5cr)",
        })

    for tenure in sorted(SHRIRAM_RATES):
        rate_ref_rows.append({
            "Partner": "Shriram Finance",
            "Tenure": f"{tenure} months",
            "Rate_Pct": SHRIRAM_RATES[tenure],
            "Rate_Type": "Upfront (excl GST)",
            "GST_Treatment": "Agreement rate is excl GST",
            "Source": "Agreement Annexure II A (Upto 5cr)",
        })

    for tenure in sorted(MAHINDRA_RATES):
        rate_ref_rows.append({
            "Partner": "Mahindra Finance",
            "Tenure": f"{tenure} months",
            "Rate_Pct": MAHINDRA_RATES[tenure],
            "Rate_Type": "Upfront (excl GST)",
            "GST_Treatment": "Agreement rate is excl GST",
            "Source": "Agreement Annexure II A (Upto 2cr)",
        })

    rate_ref = pd.DataFrame(rate_ref_rows)

    out_file = "FD_Monthly_Billing_v4.xlsx"
    print(f"\nWriting output to {out_file}...")
    with pd.ExcelWriter(out_file, engine="openpyxl") as writer:
        pivot.rename_axis("Month").to_excel(writer, sheet_name="Combined Summary")

        partner_order = ["Unity", "Suryoday", "Shivalik",
                         "Bajaj Finance Ltd", "Shriram Finance", "Mahindra Finance"]
        for pname in partner_order:
            pdata = summary[summary["partner"] == pname]
            if pdata.empty:
                continue
            sheet = pname.replace(" ", "")[:28]
            pdata.drop(columns=["partner"]).to_excel(writer, sheet_name=sheet, index=False)

        summary.to_excel(writer, sheet_name="All Partners Summary", index=False)
        grand.to_excel(writer, sheet_name="Grand Totals", index=False)
        bank_comp.to_excel(writer, sheet_name="Bank Comparison", index=False)
        nbfc_comp.to_excel(writer, sheet_name="NBFC Comparison", index=False)
        rate_ref.to_excel(writer, sheet_name="Rate Reference", index=False)
        working.to_excel(writer, sheet_name="Working Sheet", index=False)
        detail.to_excel(writer, sheet_name="Detail", index=False)

    print(f"Saved → {out_file}")

    print("\n" + "=" * 65)
    print("GRAND TOTAL BILLING PER PARTNER")
    print("=" * 65)
    for _, r in grand.iterrows():
        print(f"  {r['partner']:20s}  Billing: ₹{r['total_billing']:>14,.2f}")

    print(f"\n{'=' * 90}")
    print("BANK COMPARISON: Our vs Partner (Jul-Dec 2025)")
    print(f"{'=' * 90}")
    print(
        bank_comp[
            ["Partner", "Month", "Partner_Billing", "Our_Billing",
             "Billing_Diff", "Billing_Diff_Pct", "FD_Count_Diff"]
        ].to_string(index=False)
    )

    print(f"\n{'=' * 90}")
    print("NBFC COMPARISON: Our vs Partner (Jan-Dec 2025)")
    print(f"{'=' * 90}")
    if not nbfc_comp.empty:
        print(
            nbfc_comp[
                ["Partner", "Month", "Partner_Billing", "Our_Billing",
                 "Billing_Diff", "Billing_Diff_Pct", "FD_Count_Diff", "Note"]
            ].to_string(index=False)
        )

    bank_months_range = sorted(
        set(ml for (_, ml) in bank_partner.keys())
    )
    nbfc_months_range = sorted(
        set(ml for (_, ml) in nbfc_partner.keys())
    )
    print(f"\nBank comparison months: {bank_months_range}")
    print(f"NBFC comparison months: {nbfc_months_range}")
    print(f"Billing extended through: {TARGET_END}")


if __name__ == "__main__":
    main()
