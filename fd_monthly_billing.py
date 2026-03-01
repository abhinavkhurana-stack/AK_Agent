"""
Fixed Deposit Monthly Billing Calculator
=========================================
Calculates monthly billing revenue per lender/partner from FD data.

Approach
--------
For each FD, determine the "active window" — the date range during which the
FD principal was deployed with the partner.  Then, for every calendar month
that overlaps with that window, compute:

    billing = amount × billing_rate × days_in_month / 365

where `days_in_month` is the number of days the FD was active *in that
particular month*.

Active-window rules (start-date inclusive, end-date EXCLUSIVE):
  • Start  = date(created_at_ist)           — first day principal is deployed
  • End depends on fd_status and uses maturity_at_ist from the data:
      MATURED      → maturity_at_ist
      WITHDRAW     → min(maturity_at_ist, updated_at_ist)
      OPEN         → min(maturity_at_ist, report_cutoff + 1 day)
      IN_PROGRESS  → min(maturity_at_ist, report_cutoff + 1 day)

  Fallback when maturity_at_ist is null or invalid (< created_at_ist):
      MATURED  (period > 0) → min(created + period months, updated_at_ist)
      MATURED  (period = 0) → updated_at_ist
      WITHDRAW              → updated_at_ist
      OPEN / IN_PROGRESS    → report_cutoff + 1 day

  The min() for MATURED handles premature closures.  The min() for WITHDRAW
  caps billing at maturity even when withdrawal was processed later.

Data quality:
  • 393 exact-duplicate rows in source are deduplicated on transaction_id.
  • 179 records with clearly invalid maturity dates (years 2001-2012) fall
    back to investment_period / updated_at_ist.

Billing rates (annual, as provided):
  Unity    → 0.50 %
  Suryoday → 0.35 %
"""

import calendar
import warnings
from datetime import date, timedelta

import pandas as pd
from dateutil.relativedelta import relativedelta

warnings.filterwarnings("ignore")

BILLING_RATES = {
    "Unity": 0.50 / 100,
    "Suryoday": 0.35 / 100,
}

REPORT_CUTOFF = date(2026, 3, 1)


def compute_end_date(row, created_date):
    """Return the exclusive end-date for billing.

    Primary source: maturity_at_ist from the data.
    Fallback: calculated from investment_period / updated_at_ist when maturity
    is missing or clearly invalid (before creation date).
    """
    status = row["fd_status"]
    period = row["investment_period"]
    updated = pd.to_datetime(row["updated_at_ist"]).date()

    mat_raw = row["maturity_at_ist"]
    has_valid_maturity = pd.notna(mat_raw)
    if has_valid_maturity:
        maturity = pd.to_datetime(mat_raw).date()
        if maturity <= created_date:
            has_valid_maturity = False

    cutoff_end = REPORT_CUTOFF + timedelta(days=1)

    if has_valid_maturity:
        if status == "MATURED":
            return maturity
        if status == "WITHDRAW":
            return min(maturity, updated)
        if status in ("OPEN", "IN_PROGRESS"):
            return min(maturity, cutoff_end)
    else:
        if status == "MATURED":
            if period > 0:
                return min(created_date + relativedelta(months=int(period)), updated)
            return updated
        if status == "WITHDRAW":
            return updated
        if status in ("OPEN", "IN_PROGRESS"):
            if period > 0:
                calc_mat = created_date + relativedelta(months=int(period))
                return min(calc_mat, cutoff_end)
            return cutoff_end

    return None


def monthly_breakdown(start_date, end_date_excl):
    """Yield (year, month, days) for every calendar month the FD overlaps."""
    if end_date_excl <= start_date:
        return

    cur = start_date.replace(day=1)
    while cur < end_date_excl:
        y, m = cur.year, cur.month
        month_first = date(y, m, 1)
        month_last = date(y, m, calendar.monthrange(y, m)[1])
        month_end_excl = month_last + timedelta(days=1)

        active_start = max(start_date, month_first)
        active_end_excl = min(end_date_excl, month_end_excl)
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
    df["updated_at_ist"] = pd.to_datetime(df["updated_at_ist"])
    df["investment_period"] = df["investment_period"].fillna(0)

    dup_count = raw_count - df["transaction_id"].nunique()
    df = df.drop_duplicates(subset="transaction_id", keep="first")

    print(f"Loaded {raw_count:,} rows → {len(df):,} unique FDs (removed {dup_count} duplicates)")
    print(f"Status: {df['fd_status'].value_counts().to_dict()}")
    print(f"Partners: {df['Partner'].value_counts().to_dict()}\n")

    rows = []
    skipped = 0

    for idx, row in df.iterrows():
        created = row["created_at_ist"].date()
        end_excl = compute_end_date(row, created)

        if end_excl is None or end_excl <= created:
            skipped += 1
            continue

        partner = row["Partner"]
        amount = row["amount"]
        rate = BILLING_RATES.get(partner, 0)
        txn_id = row["transaction_id"]

        for y, m, days in monthly_breakdown(created, end_excl):
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
                "billing_amount": round(billing_amount, 2),
            })

    if skipped:
        print(f"Skipped {skipped} records with invalid date ranges (end <= start)\n")

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
    summary.to_csv("FD_Monthly_Billing_Summary.csv", index=False)
    print("Saved  → FD_Monthly_Billing_Summary.csv  (partner × month summary)")

    detail.to_csv("FD_Monthly_Billing_Detail.csv", index=False)
    print("Saved  → FD_Monthly_Billing_Detail.csv   (per-FD monthly breakdown)")

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

    print("\n" + "=" * 65)
    print("GRAND TOTAL BILLING PER PARTNER")
    print("=" * 65)
    for _, r in grand.iterrows():
        rate_str = f"{BILLING_RATES.get(r['partner'], 0) * 100:.2f}%"
        if r["partner"] not in BILLING_RATES:
            rate_str += " (rate TBD)"
        print(f"  {r['partner']:20s}  Rate: {rate_str:16s}  Billing: ₹{r['total_billing']:>14,.2f}")

    # ── Monthly QC for Unity + Suryoday ──
    for partner_name in ["Unity", "Suryoday"]:
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
        for _, row in partner_data.iterrows():
            print(f"  {row['month_label']:<10} {row['fd_count']:>10,} {row['total_amount']:>20,.0f} {row['total_active_days']:>14,} {row['total_billing']:>16,.2f}")
            total_billing += row["total_billing"]
        print(f"  {'-'*10} {'-'*10} {'-'*20} {'-'*14} {'-'*16}")
        print(f"  {'TOTAL':<10} {'':>10} {'':>20} {'':>14} {total_billing:>16,.2f}")

    # ── Combined pivot ──
    print(f"\n{'=' * 65}")
    print("COMBINED MONTHLY SNAPSHOT (Unity + Suryoday)")
    print("=" * 65)
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
    print(pivot.to_string())

    print(f"\n  Unity total     : ₹{pivot.get('Unity', pd.Series([0])).sum():>14,.2f}")
    print(f"  Suryoday total  : ₹{pivot.get('Suryoday', pd.Series([0])).sum():>14,.2f}")
    print(f"  Combined total  : ₹{pivot['Grand Total'].sum():>14,.2f}")

    # ── Partners without rates ──
    unrated = set(df["Partner"].unique()) - set(BILLING_RATES.keys())
    if unrated:
        print(f"\n⚠  No billing rate defined for: {', '.join(sorted(unrated))}")
        print("   Their billing shows as ₹0.  Provide rates to include them.")


if __name__ == "__main__":
    main()
