"""
tests/test_dag_1_ingesta.py
-----------------------------
Tests unitarios de las funciones Python de dag_1_ingesta.py.

No requieren conexión real a GCP ni a Airflow: GCSHook y Variable se
mockean para aislar la lógica de negocio (decisión de rama y generación
de órdenes).
"""

import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dags import dag_1_ingesta  # noqa: E402


@patch("dags.dag_1_ingesta.Variable")
@patch("dags.dag_1_ingesta.GCSHook")
def test_decide_data_source_con_archivos_nuevos(mock_gcs_hook_cls, mock_variable):
    """Si hay archivos en incoming/, debe elegir la rama de mover a raw/."""
    mock_variable.get.return_value = "mi-bucket"
    mock_hook = mock_gcs_hook_cls.return_value
    mock_hook.list.return_value = ["incoming/order_1.json", "incoming/"]

    context = {"ti": MagicMock()}
    resultado = dag_1_ingesta._decide_data_source(**context)

    assert resultado == "move_incoming_to_raw"
    context["ti"].xcom_push.assert_called_once_with(
        key="incoming_files", value=["incoming/order_1.json"]
    )


@patch("dags.dag_1_ingesta.Variable")
@patch("dags.dag_1_ingesta.GCSHook")
def test_decide_data_source_sin_archivos_nuevos(mock_gcs_hook_cls, mock_variable):
    """Si incoming/ está vacío (o solo el objeto carpeta), debe generar datos falsos."""
    mock_variable.get.return_value = "mi-bucket"
    mock_hook = mock_gcs_hook_cls.return_value
    mock_hook.list.return_value = ["incoming/"]

    context = {"ti": MagicMock()}
    resultado = dag_1_ingesta._decide_data_source(**context)

    assert resultado == "generate_fake_data"
    context["ti"].xcom_push.assert_not_called()


@patch("dags.dag_1_ingesta.generar_orden")
@patch("dags.dag_1_ingesta.Variable")
@patch("dags.dag_1_ingesta.GCSHook")
def test_generar_y_subir_ordenes_sube_la_cantidad_configurada(
    mock_gcs_hook_cls, mock_variable, mock_generar_orden
):
    """Debe subir exactamente 'fake_orders_count' órdenes a raw/."""
    mock_variable.get.side_effect = lambda key, default_var=None: {
        "gcs_bucket": "mi-bucket",
        "fake_orders_count": "3",
    }.get(key, default_var)

    mock_generar_orden.side_effect = [{"order_id": f"id-{i}"} for i in range(3)]
    mock_hook = mock_gcs_hook_cls.return_value

    dag_1_ingesta._generar_y_subir_ordenes()

    assert mock_hook.upload.call_count == 3
    primera_llamada = mock_hook.upload.call_args_list[0].kwargs
    assert primera_llamada["bucket_name"] == "mi-bucket"
    assert primera_llamada["object_name"] == "raw/order_id-0.json"
    assert primera_llamada["mime_type"] == "application/json"


@patch("dags.dag_1_ingesta.generar_orden")
@patch("dags.dag_1_ingesta.Variable")
@patch("dags.dag_1_ingesta.GCSHook")
def test_generar_y_subir_ordenes_continua_si_falla_una_subida(
    mock_gcs_hook_cls, mock_variable, mock_generar_orden
):
    """Si una subida individual falla, no debe interrumpir el resto del lote."""
    mock_variable.get.side_effect = lambda key, default_var=None: {
        "gcs_bucket": "mi-bucket",
        "fake_orders_count": "2",
    }.get(key, default_var)

    mock_generar_orden.side_effect = [{"order_id": "ok"}, {"order_id": "fail"}]
    mock_hook = mock_gcs_hook_cls.return_value
    mock_hook.upload.side_effect = [None, Exception("boom")]

    dag_1_ingesta._generar_y_subir_ordenes()

    assert mock_hook.upload.call_count == 2
