"""
Fixed Deposit Monthly Billing Calculator
=========================================
Calculates monthly billing revenue per lender/partner from FD data.

Methodology  (matches the manual Excel calculation in FD_data_new_v3)
--------------------------------------------------------------------------
For each FD the "active window" is:

    [created_at_ist  …  maturity_at_ist]      ← both dates INCLUSIVE

Every calendar month that overlaps with that window is billed:

    billing = amount × rate × active_days_in_month / 365

where active_days_in_month = min(maturity, month_end) − max(created, month_start) + 1

Rules applied uniformly to ALL fd_status values (OPEN, MATURED, WITHDRAW,
IN_PROGRESS) — no special-casing by status.  Only the two dates matter.

Billing rates (annual) — from partner records:
  Unity             → 0.50 %  (flat)
  Suryoday          → 0.35 %  (flat)
  Bajaj Finance Ltd → tenure-dependent (0.3644% – 0.5763%)
  Shriram Finance   → tenure-dependent (0.2600% – 1.0200%)
  Shivalik          → 0.15 %
  Mahindra Finance  → 0.10 %
"""

import calendar
import warnings
from datetime import date, timedelta

import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")

FLAT_RATES = {
    "Unity": 0.50 / 100,
    "Suryoday": 0.35 / 100,
    "Shivalik": 0.15 / 100,
    "Mahindra Finance": 0.10 / 100,
}

TENURE_RATES = {
    "Bajaj Finance Ltd": {
        12: 0.3983 / 100,
        15: 0.3644 / 100,
        18: 0.3983 / 100,
        24: 0.5763 / 100,
    },
    "Shriram Finance": {
        12: 0.2600 / 100,
        18: 0.2600 / 100,
        36: 1.0200 / 100,
    },
}

TENURE_DEFAULTS = {
    "Bajaj Finance Ltd": 0.3983 / 100,
    "Shriram Finance": 0.2600 / 100,
}


def get_rate(partner, period_months):
    if partner in FLAT_RATES:
        return FLAT_RATES[partner]
    if partner in TENURE_RATES:
        rates = TENURE_RATES[partner]
        period = int(round(period_months)) if pd.notna(period_months) else 0
        if period in rates:
            return rates[period]
        closest = min(rates.keys(), key=lambda k: abs(k - period)) if period > 0 else min(rates.keys())
        return rates[closest]
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


def main():
    df = pd.read_csv("FD_base.csv")
    raw_count = len(df)

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

    print(f"Loaded {raw_count:,} rows → {len(df):,} unique FDs (removed {dup_count} duplicates)")
    print(f"Fallback: {fallback_count} records used updated_at_ist as maturity")
    print(f"Processing: {len(valid):,} FDs")
    print(f"Status: {valid['fd_status'].value_counts().to_dict()}")
    print(f"Partners: {valid['Partner'].value_counts().to_dict()}\n")

    rows = []
    for _, row in valid.iterrows():
        created = row["created_at_ist"].date()
        maturity = row["maturity_at_ist"].date()
        partner = row["Partner"]
        amount = row["amount"]
        rate = get_rate(partner, row["investment_period"])
        txn_id = row["transaction_id"]

        for y, m, days in monthly_breakdown(created, maturity):
            rows.append({
                "partner": partner,
                "year": y,
                "month": m,
                "month_label": f"{y}-{m:02d}",
                "transaction_id": txn_id,
                "fd_status": row["fd_status"],
                "amount": amount,
                "billing_rate": rate,
                "active_days": days,
                "billing_amount": round(amount * rate * days / 365, 4),
            })

    detail = pd.DataFrame(rows)
    print(f"Generated {len(detail):,} monthly line items\n")

    summary = (
        detail.groupby(["partner", "month_label"])
        .agg(fd_count=("transaction_id", "nunique"),
             total_amount=("amount", "sum"),
             total_active_days=("active_days", "sum"),
             total_billing=("billing_amount", "sum"))
        .reset_index()
        .sort_values(["partner", "month_label"])
    )
    summary["total_billing"] = summary["total_billing"].round(2)

    grand = (
        summary.groupby("partner")
        .agg(months_spanned=("month_label", "nunique"),
             total_billing=("total_billing", "sum"))
        .reset_index()
        .sort_values("total_billing", ascending=False)
    )
    grand["total_billing"] = grand["total_billing"].round(2)

    all_rated = set(FLAT_RATES) | set(TENURE_RATES)
    billable = summary[summary["partner"].isin(all_rated)]
    pivot = billable.pivot_table(
        index="month_label", columns="partner",
        values="total_billing", aggfunc="sum", fill_value=0,
    )
    pivot["Grand Total"] = pivot.sum(axis=1)
    pivot = pivot.round(2)

    # ── Build Working Sheet ──
    print("Building Working Sheet…")
    all_months = sorted(detail["month_label"].unique())
    days_wide = detail.pivot_table(
        index="transaction_id", columns="month_label",
        values="active_days", aggfunc="sum", fill_value=0,
    )
    days_wide.columns = [f"Days_{c}" for c in days_wide.columns]
    bill_wide = detail.pivot_table(
        index="transaction_id", columns="month_label",
        values="billing_amount", aggfunc="sum", fill_value=0,
    )
    bill_wide.columns = [f"Billing_{c}" for c in bill_wide.columns]
    fd_totals = detail.groupby("transaction_id").agg(
        total_active_days=("active_days", "sum"),
        total_billing=("billing_amount", "sum")).round(4)

    source_cols = ["transaction_id", "Partner", "fd_status", "interest_rate",
                   "investment_period", "created_at_ist", "maturity_at_ist",
                   "updated_at_ist", "amount", "maturity_amount", "interest_payout"]
    source = valid[source_cols].set_index("transaction_id")
    source["billing_rate"] = valid.apply(
        lambda r: get_rate(r["Partner"], r["investment_period"]), axis=1).values
    source["used_fallback"] = source.index.isin(fallback_txn_ids)
    source["days_diff"] = (valid.set_index("transaction_id")["maturity_at_ist"]
                           - valid.set_index("transaction_id")["created_at_ist"]).dt.days

    working = source.join(fd_totals).join(days_wide).join(bill_wide).reset_index()
    static_cols = ["transaction_id", "Partner", "fd_status", "amount",
                   "billing_rate", "investment_period", "interest_rate",
                   "created_at_ist", "maturity_at_ist", "updated_at_ist",
                   "used_fallback", "days_diff", "total_active_days", "total_billing"]
    month_cols = []
    for m in all_months:
        month_cols += [f"Days_{m}", f"Billing_{m}"]
    existing = [c for c in static_cols + month_cols if c in working.columns]
    working = working[existing]

    # ── Build Reconciliation tab ──
    print("Building Reconciliation tab…")
    partner_summary = {
        "Unity": {
            "2025-07": 27637.79, "2025-08": 24899.12, "2025-09": 25468.18,
            "2025-10": 27853.70, "2025-11": 25128.10, "2025-12": 25806.36,
        },
        "Suryoday": {
            "2025-07": 32860.66, "2025-08": 40016.90, "2025-09": 43816.32,
            "2025-10": 49610.09, "2025-11": 51582.34, "2025-12": 56352.80,
        },
    }
    partner_fd_counts = {
        "Unity": {"2025-07": 1486, "2025-08": 1329, "2025-09": 1228,
                   "2025-10": 1256, "2025-11": 1185, "2025-12": 1197},
        "Suryoday": {"2025-07": 3742, "2025-08": 4474, "2025-09": 4703,
                      "2025-10": 4916, "2025-11": 5125, "2025-12": 5039},
    }
    recon_rows = []
    for partner in ["Unity", "Suryoday"]:
        for month in ["2025-07", "2025-08", "2025-09", "2025-10", "2025-11", "2025-12"]:
            our_row = summary[(summary["partner"] == partner) & (summary["month_label"] == month)]
            our_bill = our_row["total_billing"].values[0] if len(our_row) else 0
            our_fds = int(our_row["fd_count"].values[0]) if len(our_row) else 0
            ptr_bill = partner_summary[partner].get(month, 0)
            ptr_fds = partner_fd_counts[partner].get(month, 0)
            recon_rows.append({
                "Partner": partner,
                "Month": month,
                "Partner_Billing": round(ptr_bill, 2),
                "Partner_FD_Count": ptr_fds,
                "Our_Billing": round(our_bill, 2),
                "Our_FD_Count": our_fds,
                "Billing_Diff": round(our_bill - ptr_bill, 2),
                "Billing_Diff_Pct": round((our_bill / ptr_bill - 1) * 100, 1) if ptr_bill else 0,
                "FD_Count_Diff": our_fds - ptr_fds,
                "Note": "Our calc includes WITHDRAW FDs through maturity; partner excludes them"
            })
    recon_df = pd.DataFrame(recon_rows)

    # ── NBFC rate reference ──
    rate_ref_rows = [
        {"Partner": "Unity", "Tenure": "All", "Rate_Pct": 0.50, "Source": "Provided"},
        {"Partner": "Suryoday", "Tenure": "All", "Rate_Pct": 0.35, "Source": "Provided"},
        {"Partner": "Bajaj Finance Ltd", "Tenure": "12 months", "Rate_Pct": 0.3983, "Source": "Partner file"},
        {"Partner": "Bajaj Finance Ltd", "Tenure": "15 months", "Rate_Pct": 0.3644, "Source": "Partner file"},
        {"Partner": "Bajaj Finance Ltd", "Tenure": "18 months", "Rate_Pct": 0.3983, "Source": "Partner file"},
        {"Partner": "Bajaj Finance Ltd", "Tenure": "24 months", "Rate_Pct": 0.5763, "Source": "Partner file"},
        {"Partner": "Shriram Finance", "Tenure": "18 months", "Rate_Pct": 0.2600, "Source": "Partner file"},
        {"Partner": "Shriram Finance", "Tenure": "36 months", "Rate_Pct": 1.0200, "Source": "Partner file"},
        {"Partner": "Mahindra Finance", "Tenure": "12 months", "Rate_Pct": 0.1000, "Source": "Partner file"},
        {"Partner": "Shivalik", "Tenure": "All", "Rate_Pct": 0.15, "Source": "Estimated"},
    ]
    rate_ref = pd.DataFrame(rate_ref_rows)

    # ── Write Excel ──
    out_file = "FD_Monthly_Billing.xlsx"
    with pd.ExcelWriter(out_file, engine="openpyxl") as writer:
        pivot.rename_axis("Month").to_excel(writer, sheet_name="Combined Summary")
        for pname in list(FLAT_RATES) + list(TENURE_RATES):
            pdata = summary[summary["partner"] == pname]
            if pdata.empty:
                continue
            sheet = pname.replace(" ", "")[:28]
            pdata.drop(columns=["partner"]).to_excel(writer, sheet_name=sheet, index=False)
        summary.to_excel(writer, sheet_name="All Partners Summary", index=False)
        grand.to_excel(writer, sheet_name="Grand Totals", index=False)
        recon_df.to_excel(writer, sheet_name="Reconciliation", index=False)
        rate_ref.to_excel(writer, sheet_name="Rate Reference", index=False)
        working.to_excel(writer, sheet_name="Working Sheet", index=False)
        detail.to_excel(writer, sheet_name="Detail", index=False)

    print(f"\nSaved → {out_file}")

    # ── Console summary ──
    print("\n" + "=" * 65)
    print("GRAND TOTAL BILLING PER PARTNER")
    print("=" * 65)
    for _, r in grand.iterrows():
        print(f"  {r['partner']:20s}  Billing: ₹{r['total_billing']:>14,.2f}")

    print(f"\n{'=' * 90}")
    print("RECONCILIATION: Our Fresh vs Partner (Jul-Dec 2025)")
    print(f"{'=' * 90}")
    print(recon_df[["Partner", "Month", "Partner_Billing", "Our_Billing",
                     "Billing_Diff", "Billing_Diff_Pct", "FD_Count_Diff"]].to_string(index=False))


if __name__ == "__main__":
    main()
