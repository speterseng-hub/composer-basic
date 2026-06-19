"""
tests/test_transform.py
-------------------------
Tests unitarios de la lógica de transformación de dag_2_transform.py.

No requieren conexión real a GCP ni a Airflow: GCSHook y Variable se
mockean para aislar la lógica de negocio.
"""

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dags.dag_2_transform import _orden_es_valida, _transformar_ordenes  # noqa: E402


def _orden_completa(**overrides):
    """Genera una orden válida base, con posibilidad de sobreescribir campos."""
    orden = {
        "order_id": "ORD-001",
        "customer_id": "CUST-001",
        "fecha": "2026-06-17T10:00:00",
        "estado": "pending",
        "ciudad": "Santiago",
        "pais": "Chile",
        "items": [
            {"product_id": "P1", "nombre": "Widget", "cantidad": 2, "precio_unitario": 10.0}
        ],
    }
    orden.update(overrides)
    return orden


class TestOrdenEsValida:

    def test_orden_valida(self):
        assert _orden_es_valida(_orden_completa()) is True

    def test_falta_campo_requerido(self):
        orden = _orden_completa()
        del orden["ciudad"]
        assert _orden_es_valida(orden) is False

    def test_estado_invalido(self):
        assert _orden_es_valida(_orden_completa(estado="inexistente")) is False

    def test_items_vacio(self):
        assert _orden_es_valida(_orden_completa(items=[])) is False

    def test_items_no_es_lista(self):
        assert _orden_es_valida(_orden_completa(items="no-lista")) is False

    def test_cantidad_cero(self):
        items = [{"product_id": "P1", "nombre": "X", "cantidad": 0, "precio_unitario": 10.0}]
        assert _orden_es_valida(_orden_completa(items=items)) is False

    def test_precio_negativo(self):
        items = [{"product_id": "P1", "nombre": "X", "cantidad": 1, "precio_unitario": -5.0}]
        assert _orden_es_valida(_orden_completa(items=items)) is False

    def test_todos_los_estados_validos(self):
        for estado in ["pending", "shipped", "delivered", "cancelled"]:
            assert _orden_es_valida(_orden_completa(estado=estado)) is True


class TestTransformarOrdenes:

    def _jsonl(self, ordenes):
        return "\n".join(json.dumps(o, ensure_ascii=False) for o in ordenes)

    @patch("dags.dag_2_transform.GCSHook")
    def test_archivo_no_encontrado_lanza_excepcion(self, mock_gcs_hook_cls):
        mock_hook = mock_gcs_hook_cls.return_value
        mock_hook.list.return_value = []

        with pytest.raises(Exception, match="No se encontró el archivo JSONL"):
            _transformar_ordenes(ds="2026-06-17")

    @patch("dags.dag_2_transform.GCSHook")
    def test_json_malformado_no_crashea(self, mock_gcs_hook_cls):
        mock_hook = mock_gcs_hook_cls.return_value
        mock_hook.list.return_value = ["raw/orders_2026-06-17.jsonl"]
        validas = [_orden_completa(order_id=f"ORD-{i}") for i in range(20)]
        contenido = self._jsonl(validas) + "\n{json-roto"
        mock_hook.download.return_value = contenido.encode("utf-8")

        _transformar_ordenes(ds="2026-06-17")

        assert mock_hook.upload.call_count >= 1

    @patch("dags.dag_2_transform.GCSHook")
    def test_archivo_vacio_lanza_excepcion(self, mock_gcs_hook_cls):
        mock_hook = mock_gcs_hook_cls.return_value
        mock_hook.list.return_value = ["raw/orders_2026-06-17.jsonl"]
        mock_hook.download.return_value = b""

        with pytest.raises(Exception, match="no contiene líneas válidas"):
            _transformar_ordenes(ds="2026-06-17")

    @patch("dags.dag_2_transform.GCSHook")
    def test_orden_invalida_va_a_rejected(self, mock_gcs_hook_cls):
        mock_hook = mock_gcs_hook_cls.return_value
        mock_hook.list.return_value = ["raw/orders_2026-06-17.jsonl"]

        validas = [_orden_completa(order_id=f"GOOD-{i}") for i in range(20)]
        orden_mala = _orden_completa(order_id="BAD-1", estado="inexistente")
        mock_hook.download.return_value = self._jsonl(validas + [orden_mala]).encode("utf-8")

        _transformar_ordenes(ds="2026-06-17")

        rejected_uploads = [
            c for c in mock_hook.upload.call_args_list
            if "rejected/" in str(c)
        ]
        assert len(rejected_uploads) == 1
        assert "BAD-1" in str(rejected_uploads[0])

    @patch("dags.dag_2_transform.GCSHook")
    def test_orden_sin_order_id_rechazada_usa_fallback(self, mock_gcs_hook_cls):
        mock_hook = mock_gcs_hook_cls.return_value
        mock_hook.list.return_value = ["raw/orders_2026-06-17.jsonl"]

        validas = [_orden_completa(order_id=f"GOOD-{i}") for i in range(20)]
        orden_sin_id = {"estado": "inexistente", "customer_id": "C1", "fecha": "2026-06-17",
                        "ciudad": "X", "pais": "Y", "items": []}
        mock_hook.download.return_value = self._jsonl(validas + [orden_sin_id]).encode("utf-8")

        _transformar_ordenes(ds="2026-06-17")

        rejected_uploads = [
            c for c in mock_hook.upload.call_args_list
            if "rejected/" in str(c)
        ]
        assert len(rejected_uploads) == 1
        assert "unknown_" in str(rejected_uploads[0])

    @patch("dags.dag_2_transform.GCSHook")
    def test_parquet_generado_correctamente(self, mock_gcs_hook_cls):
        mock_hook = mock_gcs_hook_cls.return_value
        mock_hook.list.return_value = ["raw/orders_2026-06-17.jsonl"]

        ordenes = [_orden_completa(order_id=f"ORD-{i}") for i in range(3)]
        mock_hook.download.return_value = self._jsonl(ordenes).encode("utf-8")

        _transformar_ordenes(ds="2026-06-17")

        parquet_uploads = [
            c for c in mock_hook.upload.call_args_list
            if "processed/" in str(c)
        ]
        assert len(parquet_uploads) == 2
        filenames = [str(c) for c in parquet_uploads]
        assert any("orders.parquet" in f for f in filenames)
        assert any("order_items.parquet" in f for f in filenames)
