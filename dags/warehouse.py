"""Configuration both DAGs need, and the asset that connects them."""

from pathlib import Path

from airflow.sdk import Asset
from cosmos import ProfileConfig

# dags/ sits directly under the Airflow home in the image and under the
# repository root in a checkout, so deriving the root rather than naming it is
# what lets the DAGs be parsed by a test outside the container.
ROOT = Path(__file__).resolve().parent.parent

PROJECT = ROOT / "transform"

# include/ is bind-mounted; anywhere else in the image is discarded on rebuild.
WAREHOUSE = ROOT / "include" / "warehouse.duckdb"

# What patient_history waits for. The snapshot reads a state rather than a
# range, so it runs because raw moved, not because the clock did.
WAREHOUSE_LANDED = Asset(name="warehouse_raw", uri="duckdb://include/warehouse.duckdb?layer=raw")

PROFILE = ProfileConfig(
    profile_name="health_warehouse",
    target_name="dev",
    profiles_yml_filepath=PROJECT / "profiles.yml",
)

# Cosmos runs dbt from a temporary copy of the project, so a relative warehouse
# path would silently create an empty database inside that copy.
OPERATOR_ARGS = {"pool": "warehouse", "env": {"DBT_WAREHOUSE_PATH": str(WAREHOUSE)}}

# Everything the snapshot owns: itself, dim_patient, and their tests. The loader
# DAG excludes this selector and the snapshot DAG selects it, so the two are
# complements and no node is built twice.
SNAPSHOT_SELECTOR = "snap_patient+"
