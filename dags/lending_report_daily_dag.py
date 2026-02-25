"""
===============================================================================
  DAILY LENDING REPORT DAG
===============================================================================
Schedule : once daily at 06:00 UTC
Windows  : T-1, T-2, MTD, LMTD

Output tables (prefix lrd_):
  Per lender : lrd_{lender}_summary, lrd_{lender}_funnel
  Cross      : lrd_overall_summary, lrd_overall_funnel,
               lrd_unique_tof, lrd_topline_comparison

To add a new lender or step, edit  lending_report/config.py — this DAG
auto-scales from configuration.
===============================================================================
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from lending_report.config import get_active_lenders, get_non_base_steps
from lending_report.db import execute_statements
from lending_report.query_builder import (
    build_base_table,
    build_cleanup,
    build_lender_funnel,
    build_lender_summary,
    build_overall_funnel,
    build_overall_summary,
    build_step_table,
    build_topline_comparison,
    build_unique_tof,
    get_daily_windows,
)

REPORT_TYPE = "daily"


# ═════════════════════════════════════════════════════════════════════════════
#  TASK CALLABLES
# ═════════════════════════════════════════════════════════════════════════════

def _run_base(lender_cfg: dict, **ctx):
    windows = get_daily_windows(ctx["execution_date"])
    stmts = build_base_table(REPORT_TYPE, lender_cfg, windows)
    execute_statements(stmts)


def _run_step(lender_cfg: dict, step_cfg: dict, **ctx):
    stmts = build_step_table(REPORT_TYPE, lender_cfg, step_cfg)
    execute_statements(stmts)


def _run_summary(lender_cfg: dict, **ctx):
    windows = get_daily_windows(ctx["execution_date"])
    stmts = build_lender_summary(REPORT_TYPE, lender_cfg, windows)
    execute_statements(stmts)


def _run_funnel(lender_cfg: dict, **ctx):
    stmts = build_lender_funnel(REPORT_TYPE, lender_cfg)
    execute_statements(stmts)


def _run_overall(**ctx):
    stmts = build_overall_summary(REPORT_TYPE)
    execute_statements(stmts)


def _run_overall_funnel(**ctx):
    stmts = build_overall_funnel(REPORT_TYPE)
    execute_statements(stmts)


def _run_unique_tof(**ctx):
    windows = get_daily_windows(ctx["execution_date"])
    stmts = build_unique_tof(REPORT_TYPE, windows)
    execute_statements(stmts)


def _run_topline(**ctx):
    stmts = build_topline_comparison(REPORT_TYPE)
    execute_statements(stmts)


def _run_cleanup(**ctx):
    stmts = build_cleanup(REPORT_TYPE)
    execute_statements(stmts)


# ═════════════════════════════════════════════════════════════════════════════
#  DAG DEFINITION
# ═════════════════════════════════════════════════════════════════════════════

default_args = {
    "owner": "analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lending_report_daily",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    default_args=default_args,
    tags=["lending", "daily"],
) as dag:

    lenders = get_active_lenders()
    steps = get_non_base_steps()

    # ── Per-lender task chains ───────────────────────────────────────
    all_funnel_tasks = []

    for lender in lenders:
        lk = lender["key"]

        base_task = PythonOperator(
            task_id=f"{lk}_base",
            python_callable=_run_base,
            op_kwargs={"lender_cfg": lender},
            provide_context=True,
        )

        step_tasks: dict[str, PythonOperator] = {}
        for step in steps:
            sk = step["key"]
            t = PythonOperator(
                task_id=f"{lk}_{sk}",
                python_callable=_run_step,
                op_kwargs={"lender_cfg": lender, "step_cfg": step},
                provide_context=True,
            )
            step_tasks[sk] = t

            dep_on = step.get("depends_on") or "base"
            if dep_on == "base":
                base_task >> t
            elif dep_on in step_tasks:
                step_tasks[dep_on] >> t
            else:
                base_task >> t

        summary_task = PythonOperator(
            task_id=f"{lk}_summary",
            python_callable=_run_summary,
            op_kwargs={"lender_cfg": lender},
            provide_context=True,
        )

        funnel_task = PythonOperator(
            task_id=f"{lk}_funnel",
            python_callable=_run_funnel,
            op_kwargs={"lender_cfg": lender},
            provide_context=True,
        )

        list(step_tasks.values()) >> summary_task >> funnel_task
        all_funnel_tasks.append(funnel_task)

    # ── Cross-lender aggregation ─────────────────────────────────────
    overall_task = PythonOperator(
        task_id="overall_summary",
        python_callable=_run_overall,
        provide_context=True,
    )

    overall_funnel_task = PythonOperator(
        task_id="overall_funnel",
        python_callable=_run_overall_funnel,
        provide_context=True,
    )

    unique_tof_task = PythonOperator(
        task_id="unique_tof",
        python_callable=_run_unique_tof,
        provide_context=True,
    )

    topline_task = PythonOperator(
        task_id="topline_comparison",
        python_callable=_run_topline,
        provide_context=True,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup",
        python_callable=_run_cleanup,
        provide_context=True,
    )

    # ── Dependency wiring ────────────────────────────────────────────
    all_funnel_tasks >> overall_task >> overall_funnel_task
    all_funnel_tasks >> unique_tof_task
    overall_funnel_task >> topline_task >> cleanup_task
    unique_tof_task >> cleanup_task
