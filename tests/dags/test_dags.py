"""Tests that both DAGs parse, and that the selector splitting them covers the project.

Cosmos renders these from `transform/target/manifest.json`, so a `dbt parse` has
to have run first. `make test-dags` does it.
"""

import json
import sys
from pathlib import Path

import pytest

DAGS = Path("dags")
MANIFEST = Path("transform/target/manifest.json")

# The airflow dependency group carries cosmos and airflow together, and the
# loader stays testable without either, so `make test` skips this file.
pytest.importorskip("cosmos")

# Cosmos renders the DAGs from the manifest rather than from the project, so
# without one every DAG fails to import and each assertion below fails for a
# reason that has nothing to do with what it checks.
if not MANIFEST.exists():
    pytest.skip(f"{MANIFEST} is absent; `make test-dags` builds it first", allow_module_level=True)

# Cosmos names each task for the node it builds and the dbt command it runs.
BUILD_SUFFIXES = ("_run", "_snapshot")

BUILT = ("model", "snapshot")


@pytest.fixture(scope="session")
def manifest() -> dict:
    """The dbt graph, which is where the two DAGs should divide."""
    return json.loads(MANIFEST.read_text())


@pytest.fixture(scope="session")
def dagbag():
    """Both DAGs, loaded as the scheduler loads them.

    DagBag does not put the dag folder on `sys.path` and the scheduler does, so
    without this `warehouse.py` is unimportable from the two DAGs beside it and
    every DAG fails to load for a reason that exists only in the test.
    """
    from airflow.models import DagBag

    sys.path.insert(0, str(DAGS.resolve()))
    return DagBag(dag_folder=str(DAGS))


def built_nodes(dag) -> set[str]:
    """The dbt nodes a DAG builds, read back off the task ids Cosmos generated."""
    names = set()
    for task in dag.tasks:
        task_name = task.task_id.rpartition(".")[2]
        for suffix in BUILD_SUFFIXES:
            if task_name.endswith(suffix):
                names.add(task_name.removesuffix(suffix))
    return names


def model_parents(manifest: dict) -> dict[str, set[str]]:
    """Each model and snapshot mapped to the models and snapshots it reads, by name.

    Sources are left out: raw is written by the loader task rather than built by
    either DAG, so an edge to one crosses no boundary.
    """
    return {
        node["name"]: {
            manifest["nodes"][parent]["name"]
            for parent in manifest["parent_map"].get(uid, [])
            if manifest["nodes"].get(parent, {}).get("resource_type") in BUILT
        }
        for uid, node in manifest["nodes"].items()
        if node["resource_type"] in BUILT
    }


class TestTheDagSplit:
    """One Cosmos selector divides the dbt project between two DAGs, and nothing at runtime checks it.

    A selector that stops matching does not fail: the loader DAG simply builds a
    node the snapshot DAG also builds, or neither builds it and the warehouse is
    quietly missing a model.
    """

    def test_both_dags_parse(self, dagbag) -> None:
        assert dagbag.import_errors == {}
        assert sorted(dagbag.dags) == ["health_warehouse", "patient_history"]

    def test_the_two_dags_partition_the_project(self, dagbag, manifest) -> None:
        """Every model and the snapshot is built by exactly one of the two DAGs."""
        loader = built_nodes(dagbag.dags["health_warehouse"])
        snapshot = built_nodes(dagbag.dags["patient_history"])
        expected = {n["name"] for n in manifest["nodes"].values() if n["resource_type"] in BUILT}

        assert loader, "the loader DAG builds nothing, so the selector matched everything"
        assert loader & snapshot == set(), f"built by both DAGs: {loader & snapshot}"
        assert loader | snapshot == expected, f"built by neither: {expected - loader - snapshot}"

    def test_neither_dag_reads_a_relation_the_other_builds(self, dagbag, manifest) -> None:
        """A partition says every node is built once, and nothing about which DAG builds it.

        The two DAGs share an asset and nothing else, so a dbt edge crossing
        between them is ordered by whichever happens to take the pool first. On
        a cold warehouse that reads as the snapshot failing on a relation the
        loader DAG has not built yet, then going green on a retry that captures
        a window too late.
        """
        parents = model_parents(manifest)
        for dag_id, other_id in (("health_warehouse", "patient_history"), ("patient_history", "health_warehouse")):
            built = built_nodes(dagbag.dags[dag_id])
            elsewhere = built_nodes(dagbag.dags[other_id])
            crossing = {(node, parent) for node in built for parent in parents.get(node, set()) & elsewhere}

            assert crossing == set(), f"{dag_id} reads what {other_id} builds: {sorted(crossing)}"

    def test_each_dag_runs_its_tests_once_after_its_own_models(self, dagbag) -> None:
        """One test task per DAG, which is what lets the selector reach the tests as well as the models.

        Per-model tests would run each against the model it points at, so a
        conformed dimension's relationships test would run before the fact
        referencing it exists.
        """
        for name, dag in dagbag.dags.items():
            tests = [t.task_id for t in dag.tasks if t.task_id.endswith("transform_test")]
            assert len(tests) == 1, f"{name} has {len(tests)} test tasks"

    def test_only_the_land_task_emits_the_asset(self, dagbag) -> None:
        """Both DAGs read one asset constant, so equality between them proves nothing; where it sits does.

        On `operator_args` the outlet is global to the task group and every model
        Cosmos generates emits it, releasing the snapshot before raw has moved.
        """
        emitting = {t.task_id for t in dagbag.dags["health_warehouse"].tasks if t.outlets}
        awaited = {asset.name for asset in dagbag.dags["patient_history"].timetable.asset_condition.objects}

        assert emitting == {"land"}, f"tasks emitting an asset: {sorted(emitting)}"
        assert {a.name for a in dagbag.dags["health_warehouse"].get_task("land").outlets} == awaited
