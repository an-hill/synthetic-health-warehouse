"""Lands one date window and builds the warehouse over it, once per month.

Airflow's data interval is a half-open date range exactly as `land_window`
takes, so the DAG does no date arithmetic of its own. The snapshot is built by
dags/patient_history.py instead, on a schedule that does not backfill.
"""

from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from cosmos import DbtTaskGroup, LoadMode, RenderConfig, TestBehavior
from cosmos.operators.local import DbtSourceLocalOperator
from warehouse import (
    OPERATOR_ARGS,
    PROFILE,
    PROJECT,
    PROJECT_CONFIG,
    SNAPSHOT_SELECTOR,
    WAREHOUSE,
    WAREHOUSE_LANDED,
)


@dag(
    # Not the bare string "@monthly", which Airflow 3 resolves to a
    # CronTriggerTimetable whose interval is timedelta(0), handing the loader a
    # window with start equal to end.
    schedule=CronDataIntervalTimetable("@monthly", timezone="UTC"),
    start_date=datetime(2025, 9, 1, tzinfo=UTC),
    # Bounds the logical date, which this timetable sets to the interval start,
    # so this admits the window ending 2025-12-01, where the payer history stops.
    end_date=datetime(2025, 11, 1, tzinfo=UTC),
    catchup=True,
    # A guard against a hand-run backfill rather than something the schedule
    # needs, since only one interval is ever available at a time. Measured at 3:
    # raw and every model come out identical, and only the snapshot loses as-of
    # dates.
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["health-warehouse"],
)
def health_warehouse():
    """Land the interval, check the sources are fresh, then build the models over it."""

    # In the pool because it writes the warehouse, not because it runs dbt: the
    # lock DuckDB takes lives in the database file, so two loaders creating one
    # can both acquire it and the second to commit drops the first's window.
    # The outlet is here because raw is the snapshot's only dependency.
    @task(pool="warehouse", outlets=[WAREHOUSE_LANDED])
    def land(data_interval_start=None, data_interval_end=None) -> dict[str, int]:
        from loader.land import land_window

        # Airflow types a data interval as optional, since a run can exist
        # without one. This DAG has a timetable, so its runs always have one.
        report = land_window(data_interval_start.date(), data_interval_end.date(), database=WAREHOUSE)  # ty: ignore[unresolved-attribute]
        return report.rows

    # A precondition: after the build it would tell you nothing you can act on.
    freshness = DbtSourceLocalOperator(
        task_id="source_freshness",
        project_dir=PROJECT,
        profile_config=PROFILE,
        env={"DBT_WAREHOUSE_PATH": str(WAREHOUSE)},
        pool="warehouse",
    )

    build = DbtTaskGroup(
        group_id="build",
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            # Cosmos forwards this to the test task as well as dropping the
            # models, so dim_patient's tests leave with the model rather than
            # running here against a table this DAG never builds.
            exclude=[SNAPSHOT_SELECTOR],
            # One test task after all the models, not one per model: tests here
            # deliberately span models, and Cosmos would otherwise run each
            # against the model it points at, before its siblings exist.
            test_behavior=TestBehavior.AFTER_ALL,
        ),
        # env belongs here, not in default_args, where Cosmos ignores it.
        operator_args=OPERATOR_ARGS,
    )

    land() >> freshness >> build


health_warehouse()
