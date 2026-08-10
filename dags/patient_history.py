"""Captures a version of every patient whenever raw moves, and never backfills.

Separate from health_warehouse because the snapshot accumulates where every
other model rebuilds, so `--full-refresh` discards its history rather than
mending it. Triggered by an asset because that is the real dependency: it runs
when raw has moved, not when the clock says so. The README records what that
costs under a backfill.
"""

from datetime import timedelta

from airflow.sdk import dag
from cosmos import DbtTaskGroup, LoadMode, ProjectConfig, RenderConfig, TestBehavior
from warehouse import OPERATOR_ARGS, PROFILE, PROJECT, SNAPSHOT_SELECTOR, WAREHOUSE_LANDED


@dag(
    schedule=[WAREHOUSE_LANDED],
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["health-warehouse"],
)
def patient_history():
    """Snapshot the patients as they stand, then rebuild the dimension over the history."""
    DbtTaskGroup(
        group_id="snapshot",
        project_config=ProjectConfig(
            dbt_project_path=PROJECT,
            manifest_path=PROJECT / "target" / "manifest.json",
        ),
        profile_config=PROFILE,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            # The complement of what health_warehouse excludes: the snapshot,
            # dim_patient, and the tests that span them.
            select=[SNAPSHOT_SELECTOR],
            test_behavior=TestBehavior.AFTER_ALL,
        ),
        operator_args=OPERATOR_ARGS,
    )


patient_history()
