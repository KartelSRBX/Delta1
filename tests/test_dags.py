"""DAG integrity tests. They need Airflow installed, so the main test job skips them and a
dedicated CI job (with Airflow's constraints) runs them."""

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag

DAGS = Path(__file__).parents[1] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_and_tasks(dagbag):
    expected = {
        "delta1_fx_rates": {"fetch", "validate", "load", "reload_dictionary"},
        "delta1_provider_reconciliation": {
            "wait_for_reports",
            "load_reports",
            "reconcile",
            "summarize",
        },
        "delta1_daily_marts": {
            f"{mart}.{t}"
            for mart in ("player_daily", "provider_daily")
            for t in ("build", "audit", "publish")
        },
    }
    assert set(dagbag.dag_ids) == set(expected)
    for dag_id, tasks in expected.items():
        dag = dagbag.get_dag(dag_id)
        assert set(dag.task_ids) == tasks, dag_id
        assert dag.catchup is False
        assert "delta1" in dag.tags
        for task in dag.tasks:
            assert task.retries >= 1, f"{dag_id}.{task.task_id} has no retries"


def test_task_order(dagbag):
    recon = dagbag.get_dag("delta1_provider_reconciliation")
    assert recon.get_task("wait_for_reports").downstream_task_ids == {"load_reports"}
    assert recon.get_task("load_reports").downstream_task_ids == {"reconcile"}
    assert recon.get_task("reconcile").downstream_task_ids == {"summarize"}

    marts = dagbag.get_dag("delta1_daily_marts")
    for mart in ("player_daily", "provider_daily"):
        build = marts.get_task(f"{mart}.build")
        assert build.downstream_task_ids == {f"{mart}.audit", f"{mart}.publish"}
        assert marts.get_task(f"{mart}.audit").downstream_task_ids == {f"{mart}.publish"}


def test_reconciliation_runs_once_per_report_window(dagbag):
    from delta1_common import REPORT_WINDOW_MINUTES

    dag = dagbag.get_dag("delta1_provider_reconciliation")
    start = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
    interval = dag.timetable.infer_manual_data_interval(run_after=start)
    assert interval.end - interval.start == timedelta(minutes=REPORT_WINDOW_MINUTES)


def test_marts_close_out_previous_day_at_midnight():
    from delta1_daily_marts import target_day

    assert target_day(datetime(2026, 11, 3, 0, 0, tzinfo=UTC)) == date(2026, 11, 2)
    assert target_day(datetime(2026, 11, 2, 0, 15, tzinfo=UTC)) == date(2026, 11, 2)
