"""
tests/test_dag_structure.py
-----------------------------
Tests de estructura de los DAGs del pipeline de e-commerce.

Verifica que los DAGs se cargan sin errores, tienen los task IDs esperados,
y las dependencias entre tasks son correctas.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dags import dag_1_ingesta  # noqa: E402
from dags import dag_2_transform  # noqa: E402
from dags import dag_3_bq_load  # noqa: E402
from dags import dag_4_metricas  # noqa: E402
from dags import dag_5_reporte  # noqa: E402


def _task_ids(dag):
    return {t.task_id for t in dag.tasks}


def _downstream_ids(dag, task_id):
    return {t.task_id for t in dag.get_task(task_id).downstream_list}


class TestDag1Ingesta:

    def test_dag_id(self):
        assert dag_1_ingesta.dag.dag_id == "dag_1_ingesta"

    def test_tasks_existen(self):
        esperados = {"decide_data_source", "move_incoming_to_raw", "generate_fake_data",
                     "join", "trigger_dag_2_transform"}
        assert _task_ids(dag_1_ingesta.dag) == esperados

    def test_decide_bifurca_a_dos_ramas(self):
        downstream = _downstream_ids(dag_1_ingesta.dag, "decide_data_source")
        assert downstream == {"move_incoming_to_raw", "generate_fake_data"}

    def test_join_dispara_trigger(self):
        assert _downstream_ids(dag_1_ingesta.dag, "join") == {"trigger_dag_2_transform"}


class TestDag2Transform:

    def test_dag_id(self):
        assert dag_2_transform.dag.dag_id == "dag_2_transform"

    def test_tasks_existen(self):
        esperados = {"transform_orders", "validate_row_count", "trigger_dag_3_bq_load"}
        assert _task_ids(dag_2_transform.dag) == esperados

    def test_cadena_secuencial(self):
        assert _downstream_ids(dag_2_transform.dag, "transform_orders") == {"validate_row_count"}
        assert _downstream_ids(dag_2_transform.dag, "validate_row_count") == {"trigger_dag_3_bq_load"}

    def test_no_tiene_schedule(self):
        assert dag_2_transform.dag.schedule is None


class TestDag3BqLoad:

    def test_dag_id(self):
        assert dag_3_bq_load.dag.dag_id == "dag_3_bq_load"

    def test_tasks_existen(self):
        esperados = {"delete_existing_orders", "load_orders_to_bq",
                     "load_order_items_to_bq", "trigger_dag_4_metricas"}
        assert _task_ids(dag_3_bq_load.dag) == esperados

    def test_delete_antes_de_cargas_paralelas(self):
        downstream = _downstream_ids(dag_3_bq_load.dag, "delete_existing_orders")
        assert downstream == {"load_orders_to_bq", "load_order_items_to_bq"}

    def test_cargas_convergen_en_trigger(self):
        assert _downstream_ids(dag_3_bq_load.dag, "load_orders_to_bq") == {"trigger_dag_4_metricas"}
        assert _downstream_ids(dag_3_bq_load.dag, "load_order_items_to_bq") == {"trigger_dag_4_metricas"}


class TestDag4Metricas:

    def test_dag_id(self):
        assert dag_4_metricas.dag.dag_id == "dag_4_metricas"

    def test_tasks_existen(self):
        esperados = {"delete_existing_metrics", "run_metrics_query",
                     "save_metrics_to_variable", "trigger_dag_5_reporte"}
        assert _task_ids(dag_4_metricas.dag) == esperados

    def test_cadena_secuencial(self):
        assert _downstream_ids(dag_4_metricas.dag, "delete_existing_metrics") == {"run_metrics_query"}
        assert _downstream_ids(dag_4_metricas.dag, "run_metrics_query") == {"save_metrics_to_variable"}
        assert _downstream_ids(dag_4_metricas.dag, "save_metrics_to_variable") == {"trigger_dag_5_reporte"}


class TestDag5Reporte:

    def test_dag_id(self):
        assert dag_5_reporte.dag.dag_id == "dag_5_reporte"

    def test_tasks_existen(self):
        esperados = {"format_report", "send_report_email"}
        assert _task_ids(dag_5_reporte.dag) == esperados

    def test_format_antes_de_send(self):
        assert _downstream_ids(dag_5_reporte.dag, "format_report") == {"send_report_email"}


class TestAllDagsCommon:

    DAGS = [
        dag_1_ingesta.dag,
        dag_2_transform.dag,
        dag_3_bq_load.dag,
        dag_4_metricas.dag,
        dag_5_reporte.dag,
    ]

    def test_todos_tienen_tag_ecommerce(self):
        for dag in self.DAGS:
            assert "ecommerce" in dag.tags, f"{dag.dag_id} no tiene tag 'ecommerce'"

    def test_todos_tienen_tag_etl(self):
        for dag in self.DAGS:
            assert "etl" in dag.tags, f"{dag.dag_id} no tiene tag 'etl'"

    def test_todos_tienen_retries(self):
        for dag in self.DAGS:
            assert dag.default_args.get("retries") == 2, f"{dag.dag_id} no tiene retries=2"

    def test_todos_tienen_doc_md_en_tasks(self):
        for dag in self.DAGS:
            for task in dag.tasks:
                assert task.doc_md, f"{dag.dag_id}.{task.task_id} no tiene doc_md"
