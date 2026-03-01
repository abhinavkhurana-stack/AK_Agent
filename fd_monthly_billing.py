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

Records skipped:
  • maturity_at_ist is null   (mostly IN_PROGRESS without a maturity yet)
  • maturity_at_ist < created_at_ist  (bad data — ~14-179 records)

Billing rates (annual):
  Unity             → 0.50 %
  Suryoday          → 0.35 %
  Bajaj Finance Ltd → 0.58 %
  Shriram Finance   → 0.35 %
  Shivalik          → 0.15 %
  Mahindra Finance  → not provided
"""

import calendar
import warnings
from datetime import date, timedelta

import pandas as pd

warnings.filterwarnings("ignore")

BILLING_RATES = {
    "Unity": 0.50 / 100,
    "Suryoday": 0.35 / 100,
    "Bajaj Finance Ltd": 0.58 / 100,
    "Shriram Finance": 0.35 / 100,
    "Shivalik": 0.15 / 100,
}

REPORT_CUTOFF = date(2026, 3, 1)


def monthly_breakdown(start_date, end_date_incl):
    """Yield (year, month, days) for every calendar month the FD overlaps.

    Both start_date and end_date_incl are INCLUSIVE.
    """
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

    dup_count = raw_count - df["transaction_id"].nunique()
    df = df.drop_duplicates(subset="transaction_id", keep="first")

    # For null/bad maturity: fall back to updated_at_ist
    bad_mask = df["maturity_at_ist"].isna() | (df["maturity_at_ist"] < df["created_at_ist"])
    fallback_count = bad_mask.sum()
    fallback_txn_ids = set(df.loc[bad_mask, "transaction_id"])
    df.loc[bad_mask, "maturity_at_ist"] = df.loc[bad_mask, "updated_at_ist"]

    # After fallback, drop any still-invalid (updated also bad)
    still_bad = df["maturity_at_ist"].isna() | (df["maturity_at_ist"] < df["created_at_ist"])
    skipped_count = still_bad.sum()
    valid = df[~still_bad].copy()

    print(f"Loaded {raw_count:,} rows → {len(df):,} unique FDs (removed {dup_count} duplicates)")
    print(f"Fallback: {fallback_count} records used updated_at_ist as maturity (null/bad maturity)")
    if skipped_count:
        print(f"Skipped: {skipped_count} records still invalid after fallback")
    print(f"Processing: {len(valid):,} FDs")
    print(f"Status: {valid['fd_status'].value_counts().to_dict()}")
    print(f"Partners: {valid['Partner'].value_counts().to_dict()}\n")

    rows = []

    for _, row in valid.iterrows():
        created = row["created_at_ist"].date()
        maturity = row["maturity_at_ist"].date()

        partner = row["Partner"]
        amount = row["amount"]
        rate = BILLING_RATES.get(partner, 0)
        txn_id = row["transaction_id"]

        for y, m, days in monthly_breakdown(created, maturity):
            billing_amount = amount * rate * days / 365
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
                "billing_amount": round(billing_amount, 4),
            })

    detail = pd.DataFrame(rows)
    print(f"Generated {len(detail):,} monthly line items\n")

    # ── Summary: per Partner per Month ──
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

    # ── Grand totals per partner ──
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

    # ── Combined pivot ──
    billable = summary[summary["partner"].isin(BILLING_RATES.keys())]
    pivot = billable.pivot_table(
        index="month_label",
        columns="partner",
        values="total_billing",
        aggfunc="sum",
        fill_value=0,
    )
    pivot["Grand Total"] = pivot.sum(axis=1)
    pivot = pivot.round(2)

    # ── Build per-FD Working Sheet (one row per FD, month columns) ──
    print("Building Working Sheet (per-FD calculation trace)…")

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

    fd_totals = (
        detail.groupby("transaction_id")
        .agg(total_active_days=("active_days", "sum"),
             total_billing=("billing_amount", "sum"))
        .round(4)
    )

    source_cols = [
        "transaction_id", "Partner", "fd_status", "interest_rate",
        "created_at_ist", "maturity_at_ist", "updated_at_ist",
        "amount", "maturity_amount", "interest_payout",
    ]
    source = valid[source_cols].set_index("transaction_id")
    source["billing_rate"] = source["Partner"].map(BILLING_RATES).fillna(0)
    source["used_fallback"] = source.index.isin(fallback_txn_ids)
    source["days_diff"] = (
        valid.set_index("transaction_id")["maturity_at_ist"]
        - valid.set_index("transaction_id")["created_at_ist"]
    ).dt.days

    working = source.join(fd_totals).join(days_wide).join(bill_wide)
    working = working.reset_index()

    # Interleave Days and Billing columns per month for readability
    static_cols = [
        "transaction_id", "Partner", "fd_status", "amount",
        "billing_rate", "interest_rate",
        "created_at_ist", "maturity_at_ist", "updated_at_ist",
        "used_fallback", "days_diff",
        "total_active_days", "total_billing",
    ]
    month_cols = []
    for m in all_months:
        month_cols.append(f"Days_{m}")
        month_cols.append(f"Billing_{m}")

    ordered_cols = static_cols + month_cols
    existing = [c for c in ordered_cols if c in working.columns]
    working = working[existing]

    print(f"Working Sheet: {len(working):,} rows × {len(existing)} columns "
          f"({len(all_months)} months)\n")

    # ── Write single Excel workbook with multiple tabs ──
    out_file = "FD_Monthly_Billing.xlsx"
    with pd.ExcelWriter(out_file, engine="openpyxl") as writer:

        # Tab 1: Combined Summary (pivot by month with both lenders)
        pivot_out = pivot.copy()
        pivot_out.index.name = "Month"
        pivot_out.to_excel(writer, sheet_name="Combined Summary")

        # Per-partner monthly tabs for each rated partner
        for pname in BILLING_RATES:
            pdata = summary[summary["partner"] == pname]
            if pdata.empty:
                continue
            sheet = pname.replace(" ", "")[:28]  # Excel 31-char limit
            pdata.drop(columns=["partner"]).to_excel(
                writer, sheet_name=sheet, index=False
            )

        # Tab 4: All Partners Summary (including those without rates)
        summary.to_excel(writer, sheet_name="All Partners Summary", index=False)

        # Tab 5: Grand Totals
        grand.to_excel(writer, sheet_name="Grand Totals", index=False)

        # Tab 6: Working Sheet (per-FD calculation trace)
        working.to_excel(writer, sheet_name="Working Sheet", index=False)

        # Tab 7: Detail (per-FD × month long-format line items)
        detail.to_excel(writer, sheet_name="Detail", index=False)

    print(f"Saved  → {out_file}")
    print(f"  Tabs: Combined Summary | Unity Monthly | Suryoday Monthly |")
    print(f"        All Partners Summary | Grand Totals | Working Sheet | Detail")

    # ── Console summary ──
    print("\n" + "=" * 65)
    print("GRAND TOTAL BILLING PER PARTNER")
    print("=" * 65)
    for _, r in grand.iterrows():
        rate_str = f"{BILLING_RATES.get(r['partner'], 0) * 100:.2f}%"
        if r["partner"] not in BILLING_RATES:
            rate_str += " (rate TBD)"
        print(f"  {r['partner']:20s}  Rate: {rate_str:16s}  Billing: ₹{r['total_billing']:>14,.2f}")

    for partner_name in BILLING_RATES:
        partner_data = summary[summary["partner"] == partner_name].copy()
        if partner_data.empty:
            continue
        rate_pct = BILLING_RATES[partner_name] * 100
        print(f"\n{'=' * 90}")
        print(f"  MONTHLY BILLING — {partner_name.upper()} (Rate: {rate_pct:.2f}%)")
        print(f"{'=' * 90}")
        print(f"  {'Month':<10} {'FD Count':>10} {'Total Amount (₹)':>20} {'Active Days':>14} {'Billing (₹)':>16}")
        print(f"  {'-'*10} {'-'*10} {'-'*20} {'-'*14} {'-'*16}")
        total_billing = 0
        for _, r in partner_data.iterrows():
            print(f"  {r['month_label']:<10} {r['fd_count']:>10,} {r['total_amount']:>20,.0f} {r['total_active_days']:>14,} {r['total_billing']:>16,.2f}")
            total_billing += r["total_billing"]
        print(f"  {'-'*10} {'-'*10} {'-'*20} {'-'*14} {'-'*16}")
        print(f"  {'TOTAL':<10} {'':>10} {'':>20} {'':>14} {total_billing:>16,.2f}")

    print(f"\n{'=' * 65}")
    print("COMBINED MONTHLY SNAPSHOT (Unity + Suryoday)")
    print("=" * 65)
    print(pivot.to_string())

    print(f"\n  Unity total     : ₹{pivot.get('Unity', pd.Series([0])).sum():>14,.2f}")
    print(f"  Suryoday total  : ₹{pivot.get('Suryoday', pd.Series([0])).sum():>14,.2f}")
    print(f"  Combined total  : ₹{pivot['Grand Total'].sum():>14,.2f}")

    unrated = set(valid["Partner"].unique()) - set(BILLING_RATES.keys())
    if unrated:
        print(f"\n⚠  No billing rate defined for: {', '.join(sorted(unrated))}")
        print("   Their billing shows as ₹0.  Provide rates to include them.")


if __name__ == "__main__":
    main()
