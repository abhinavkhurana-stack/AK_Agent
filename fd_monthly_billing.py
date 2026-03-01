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
  • End depends on fd_status:
      MATURED (period > 0)  → min(created + period months, updated_at_ist)
      MATURED (period == 0) → date(updated_at_ist)   (flexi / 7-day FDs)
      WITHDRAW              → date(updated_at_ist)    (premature closure)
      OPEN                  → min(maturity, report_cutoff + 1 day)
      IN_PROGRESS           → EXCLUDED (FD not yet live)

  The min() for MATURED handles ~90 FDs marked MATURED well before their
  contractual maturity (premature bank-side closure).

Using an *exclusive* end-date means:
  – No extra day is counted (the end-date itself is the first day the FD no
    longer exists).
  – No eligible day is missed (every day from start up to end-1 is counted).

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
    """Return the exclusive end-date for billing (first day the FD is no longer active).

    For MATURED FDs with period > 0, we take the *earlier* of the calculated
    maturity date and updated_at_ist.  This correctly handles premature closures
    (FD marked MATURED well before calculated maturity) while still using the
    contractual maturity for normal FDs (updated_at is at or after maturity).
    """
    status = row["fd_status"]
    period = row["investment_period"]
    updated = pd.to_datetime(row["updated_at_ist"]).date()

    if status == "IN_PROGRESS":
        return None

    if status == "MATURED":
        if period > 0:
            maturity = created_date + relativedelta(months=int(period))
            return min(maturity, updated)
        return updated

    if status == "WITHDRAW":
        return updated

    if status == "OPEN":
        if period > 0:
            maturity = created_date + relativedelta(months=int(period))
            return min(maturity, REPORT_CUTOFF + timedelta(days=1))
        return REPORT_CUTOFF + timedelta(days=1)

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
    df = pd.read_csv("FD_Base.csv")
    print(f"Loaded {len(df):,} FD records")

    df["created_at_ist"] = pd.to_datetime(df["created_at_ist"])
    df["updated_at_ist"] = pd.to_datetime(df["updated_at_ist"])
    df["investment_period"] = df["investment_period"].fillna(0)

    excluded_in_progress = (df["fd_status"] == "IN_PROGRESS").sum()
    df = df[df["fd_status"] != "IN_PROGRESS"].copy()
    print(f"Excluded {excluded_in_progress} IN_PROGRESS FDs (not yet live)")
    print(f"Processing {len(df):,} active/closed FDs\n")

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

    # ── Detail: every FD × month row ──
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

    print("\n" + "=" * 65)
    print("MONTHLY BILLING SNAPSHOT (Unity + Suryoday)")
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
