"""Lands one date window and builds the warehouse over it, once per month.

Airflow's data interval is a half-open date range exactly as `land_window`
takes, so the DAG does no date arithmetic of its own.
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from cosmos import DbtTaskGroup, LoadMode, ProfileConfig, ProjectConfig, RenderConfig, TestBehavior
from cosmos.operators.local import DbtSourceLocalOperator

PROJECT = Path("/usr/local/airflow/transform")

# include/ is bind-mounted; anywhere else in the image is discarded on rebuild.
WAREHOUSE = Path("/usr/local/airflow/include/warehouse.duckdb")

profile = ProfileConfig(
    profile_name="health_warehouse",
    target_name="dev",
    profiles_yml_filepath=PROJECT / "profiles.yml",
)


@dag(
    # Not the bare string "@monthly", which Airflow 3 resolves to a
    # CronTriggerTimetable whose interval is timedelta(0), handing the loader a
    # window with start equal to end.
    schedule=CronDataIntervalTimetable("@monthly", timezone="UTC"),
    start_date=datetime(2025, 9, 1, tzinfo=UTC),
    # Bounds the logical date, which this timetable sets to the interval start,
    # so this admits the window ending 2025-12-01: the last boundary inside the
    # payer history. A live source would carry no end date at all.
    end_date=datetime(2025, 11, 1, tzinfo=UTC),
    catchup=True,
    # Not what the pool gives. The pool admits one writer at a time and says
    # nothing about the order they arrive in, so intervals would otherwise land
    # out of order and leave patients_current describing the wrong date.
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["health-warehouse"],
)
def health_warehouse():
    """Land the interval, check the sources are fresh, then build every model."""

    # In the pool because it writes the warehouse, not because it runs dbt. The
    # lock DuckDB takes lives in the database file, so two loaders opening one
    # that does not exist yet can both acquire it, and the second to commit
    # leaves a file the first one's window is missing from, under a green task.
    @task(pool="warehouse")
    def land(data_interval_start=None, data_interval_end=None) -> dict[str, int]:
        from loader.land import land_window

        # Airflow types a data interval as optional, since a run can exist
        # without one. This DAG has a timetable, so its runs always have one.
        report = land_window(data_interval_start.date(), data_interval_end.date(), database=WAREHOUSE)  # ty: ignore[unresolved-attribute]
        return report.rows

    # A precondition: run after the build, freshness tells you nothing you can act on.
    freshness = DbtSourceLocalOperator(
        task_id="source_freshness",
        project_dir=PROJECT,
        profile_config=profile,
        env={"DBT_WAREHOUSE_PATH": str(WAREHOUSE)},
        pool="warehouse",
    )

    build = DbtTaskGroup(
        group_id="build",
        project_config=ProjectConfig(
            dbt_project_path=PROJECT,
            manifest_path=PROJECT / "target" / "manifest.json",
        ),
        profile_config=profile,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            # One test task after all the models, not one per model. Cosmos
            # attaches a test to the model it points at, so dim_provider's
            # relationships test would run before fct_encounter exists, and
            # tests here deliberately span models. Only the tests collapse:
            # model-level runs, retries, and logs are unaffected.
            test_behavior=TestBehavior.AFTER_ALL,
        ),
        # env belongs here, not in default_args, where Cosmos ignores it. It
        # runs dbt from a temporary copy of the project, so a relative path
        # would silently create an empty database inside that copy.
        operator_args={"pool": "warehouse", "env": {"DBT_WAREHOUSE_PATH": str(WAREHOUSE)}},
    )

    land() >> freshness >> build


health_warehouse()
