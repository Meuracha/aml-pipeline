"""
DAG Structure Tests — AML Pipeline
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

airflow = pytest.importorskip(
    "airflow", reason="airflow not installed locally — run in Docker"
)


class TestDAGImports:

    def test_bronze_dag_importable(self):
        try:
            import aml_bronze_dag

            assert hasattr(aml_bronze_dag, "dag")
        except Exception as e:
            pytest.fail(f"Cannot import aml_bronze_dag: {e}")

    def test_silver_dag_importable(self):
        try:
            import aml_silver_dag

            assert hasattr(aml_silver_dag, "dag")
        except Exception as e:
            pytest.fail(f"Cannot import aml_silver_dag: {e}")

    def test_gold_dag_importable(self):
        try:
            import aml_gold_dag

            assert hasattr(aml_gold_dag, "dag")
        except Exception as e:
            pytest.fail(f"Cannot import aml_gold_dag: {e}")

    def test_ml_dag_importable(self):
        try:
            import aml_ml_dag

            assert hasattr(aml_ml_dag, "dag")
        except Exception as e:
            pytest.fail(f"Cannot import aml_ml_dag: {e}")


class TestDAGStructure:

    @pytest.fixture(autouse=True)
    def import_dags(self):
        try:
            import aml_bronze_dag
            import aml_gold_dag
            import aml_ml_dag
            import aml_silver_dag

            self.bronze_dag = aml_bronze_dag.dag
            self.silver_dag = aml_silver_dag.dag
            self.gold_dag = aml_gold_dag.dag
            self.ml_dag = aml_ml_dag.dag
        except Exception as e:
            pytest.skip(f"DAG import failed: {e}")

    # ── dag_id ──────────────────────────────────────────────────────────────
    def test_bronze_dag_id(self):
        assert self.bronze_dag.dag_id == "aml_bronze_ingestion"

    def test_silver_dag_id(self):
        assert self.silver_dag.dag_id == "aml_silver_transform"

    def test_gold_dag_id(self):
        assert self.gold_dag.dag_id == "aml_gold_transform"

    def test_ml_dag_id(self):
        assert self.ml_dag.dag_id == "aml_ml_pipeline"

    # ── task count ──────────────────────────────────────────────────────────
    def test_bronze_has_tasks(self):
        assert len(self.bronze_dag.tasks) > 0

    def test_silver_has_tasks(self):
        assert len(self.silver_dag.tasks) > 0

    def test_gold_has_tasks(self):
        assert len(self.gold_dag.tasks) > 0

    # ── bronze task ids ─────────────────────────────────────────────────────
    def test_bronze_task_ids(self):
        task_ids = {t.task_id for t in self.bronze_dag.tasks}
        assert {"upload_to_bronze", "validate_bronze", "route_dead_letter"}.issubset(
            task_ids
        )

    def test_bronze_task_order(self):
        task_ids = [t.task_id for t in self.bronze_dag.topological_sort()]
        assert task_ids.index("upload_to_bronze") < task_ids.index("validate_bronze")
        assert task_ids.index("validate_bronze") < task_ids.index("route_dead_letter")

    # ── silver task ids ─────────────────────────────────────────────────────
    def test_silver_task_ids(self):
        task_ids = {t.task_id for t in self.silver_dag.tasks}
        assert {
            "read_transform_load",
            "validate_staging",
            "promote_to_silver",
            "create_indexes",
        }.issubset(task_ids)

    def test_silver_task_order(self):
        task_ids = [t.task_id for t in self.silver_dag.topological_sort()]
        assert task_ids.index("read_transform_load") < task_ids.index(
            "validate_staging"
        )
        assert task_ids.index("validate_staging") < task_ids.index("promote_to_silver")
        assert task_ids.index("promote_to_silver") < task_ids.index("create_indexes")

    # ── gold task ids ───────────────────────────────────────────────────────
    def test_gold_task_ids(self):
        task_ids = {t.task_id for t in self.gold_dag.tasks}
        assert {
            "feature_engineering",
            "validate_gold",
            "promote_to_gold",
            "generate_alerts",
        }.issubset(task_ids)

    def test_gold_task_order(self):
        task_ids = [t.task_id for t in self.gold_dag.topological_sort()]
        assert task_ids.index("feature_engineering") < task_ids.index("validate_gold")
        assert task_ids.index("validate_gold") < task_ids.index("promote_to_gold")
        assert task_ids.index("promote_to_gold") < task_ids.index("generate_alerts")

    # ── ml task ids ─────────────────────────────────────────────────────────
    def test_ml_dag_has_all_tasks(self):
        task_ids = {t.task_id for t in self.ml_dag.tasks}
        assert {
            "prepare_dataset",
            "train_model",
            "evaluate_model",
            "register_model",
        }.issubset(task_ids)

    def test_ml_dag_task_order(self):
        task_ids = [t.task_id for t in self.ml_dag.topological_sort()]
        assert task_ids.index("prepare_dataset") < task_ids.index("train_model")
        assert task_ids.index("train_model") < task_ids.index("evaluate_model")
        assert task_ids.index("evaluate_model") < task_ids.index("register_model")

    # ── general ─────────────────────────────────────────────────────────────
    def test_no_dag_cycles(self):
        for dag in [self.bronze_dag, self.silver_dag, self.gold_dag, self.ml_dag]:
            dag.topological_sort()

    def test_ml_dag_timeout_configured(self):
        for task in self.ml_dag.tasks:
            assert task.execution_timeout is not None

    def test_dags_not_paused(self):
        for dag in [self.bronze_dag, self.silver_dag, self.gold_dag, self.ml_dag]:
            assert not dag.is_paused_upon_creation


if __name__ == "__main__":
    import importlib

    dag_files = ["aml_bronze_dag", "aml_silver_dag", "aml_gold_dag", "aml_ml_dag"]
    failed = False
    for module_name in dag_files:
        try:
            mod = importlib.import_module(module_name)
            assert hasattr(mod, "dag")
            print(f"✅ {module_name} — OK")
        except Exception as e:
            print(f"❌ {module_name} — FAILED: {e}")
            failed = True
    sys.exit(1 if failed else 0)
