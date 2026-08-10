"""Configuration both DAGs need, and the asset that connects them."""

from pathlib import Path

from airflow.sdk import Asset
from cosmos import ProfileConfig

PROJECT = Path("/usr/local/airflow/transform")

# include/ is bind-mounted; anywhere else in the image is discarded on rebuild.
WAREHOUSE = Path("/usr/local/airflow/include/warehouse.duckdb")

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
