"""
dags/dag_2_transform.py
-------------------------
DAG de transformación de órdenes de e-commerce.

No tiene schedule propio: es disparado por la última task de
`dag_1_ingesta` (`trigger_dag_2_transform`) en cuanto hay datos nuevos en
`raw/`. Lee los JSONs crudos, los valida y separa en `orders` y
`order_items`, escribe Parquet particionado por fecha en `processed/`,
manda los registros inválidos a `rejected/`, valida el row count resultante
y dispara `dag_3_bq_load`.
"""

from datetime import datetime, timedelta
import json
import os
import sys
import tempfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.gcp_config import (  # noqa: E402
    GCS_PROCESSED_PREFIX,
    GCS_RAW_PREFIX,
    GCS_REJECTED_PREFIX,
    MIN_VALID_RECORDS_PCT,
    ORDER_STATUSES,
)


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": [Variable.get("alert_email", default_var="speterseng@gmail.com")],
}


def _orden_es_valida(orden: dict) -> bool:
    """
    Valida la estructura básica de una orden: campos requeridos, estado
    dentro de `ORDER_STATUSES`, al menos un item, y cantidades/precios
    positivos en cada item.
    """
    campos_requeridos = {"order_id", "customer_id", "fecha", "estado", "items", "ciudad", "pais"}
    if not campos_requeridos.issubset(orden.keys()):
        return False

    if orden["estado"] not in ORDER_STATUSES:
        return False

    items = orden["items"]
    if not isinstance(items, list) or not items:
        return False

    for item in items:
        if item.get("cantidad", 0) <= 0 or item.get("precio_unitario", 0) <= 0:
            return False

    return True


def _transformar_ordenes(**context) -> None:
    """
    Lee los JSONs de `raw/`, separa las órdenes válidas en filas de
    `orders` y `order_items`, sube los rechazados a `rejected/{{ds}}/` y
    escribe los Parquet resultantes en `processed/{{ds}}/`. Falla si el
    porcentaje de registros válidos cae por debajo de
    `MIN_VALID_RECORDS_PCT`.
    """
    bucket = Variable.get("gcs_bucket")
    fecha = context["ds"]
    hook = GCSHook(gcp_conn_id="google_cloud_default")

    archivos = [
        obj for obj in hook.list(bucket_name=bucket, prefix=GCS_RAW_PREFIX)
        if obj.endswith(".json")
    ]

    if not archivos:
        raise AirflowException(f"No se encontraron archivos JSON en 'gs://{bucket}/{GCS_RAW_PREFIX}'.")

    filas_orders = []
    filas_items = []
    total = 0
    rechazadas = 0

    for objeto in archivos:
        contenido = hook.download(bucket_name=bucket, object_name=objeto)
        orden = json.loads(contenido)
        total += 1

        if not _orden_es_valida(orden):
            rechazadas += 1
            hook.upload(
                bucket_name=bucket,
                object_name=f"{GCS_REJECTED_PREFIX}{fecha}/{os.path.basename(objeto)}",
                data=contenido,
                mime_type="application/json",
            )
            continue

        total_amount = sum(item["cantidad"] * item["precio_unitario"] for item in orden["items"])
        filas_orders.append({
            "order_id": orden["order_id"],
            "customer_id": orden["customer_id"],
            "fecha": orden["fecha"],
            "estado": orden["estado"],
            "ciudad": orden["ciudad"],
            "pais": orden["pais"],
            "total_amount": total_amount,
        })

        for item in orden["items"]:
            filas_items.append({
                "order_id": orden["order_id"],
                "product_id": item["product_id"],
                "nombre": item["nombre"],
                "cantidad": item["cantidad"],
                "precio_unitario": item["precio_unitario"],
                "subtotal": item["cantidad"] * item["precio_unitario"],
            })

    pct_validas = 100.0 * (total - rechazadas) / total
    print(f"Órdenes procesadas: {total}, rechazadas: {rechazadas} ({100 - pct_validas:.2f}%).")

    if pct_validas < MIN_VALID_RECORDS_PCT:
        raise AirflowException(
            f"Porcentaje de registros válidos ({pct_validas:.2f}%) por debajo del "
            f"mínimo configurado ({MIN_VALID_RECORDS_PCT}%)."
        )

    orders_df = pd.DataFrame(filas_orders)
    items_df = pd.DataFrame(filas_items)

    for nombre_archivo, df in (("orders.parquet", orders_df), ("order_items.parquet", items_df)):
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            pq.write_table(pa.Table.from_pandas(df, preserve_index=False), tmp.name)
            tmp_path = tmp.name

        try:
            hook.upload(
                bucket_name=bucket,
                object_name=f"{GCS_PROCESSED_PREFIX}{fecha}/{nombre_archivo}",
                filename=tmp_path,
            )
        finally:
            os.remove(tmp_path)


def _validar_row_count(**context) -> None:
    """
    Cuenta las filas de los archivos Parquet generados en `processed/{{ds}}/`
    y falla si el total es menor al mínimo configurado en la Variable
    `min_processed_rows`.
    """
    bucket = Variable.get("gcs_bucket")
    min_rows = int(Variable.get("min_processed_rows", default_var=1))
    fecha = context["ds"]
    prefix = f"{GCS_PROCESSED_PREFIX}{fecha}/"

    hook = GCSHook(gcp_conn_id="google_cloud_default")
    archivos = [
        obj for obj in hook.list(bucket_name=bucket, prefix=prefix)
        if obj.endswith(".parquet")
    ]

    if not archivos:
        raise AirflowException(
            f"No se encontraron archivos Parquet en 'gs://{bucket}/{prefix}'."
        )

    total_rows = 0
    for objeto in archivos:
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            hook.download(bucket_name=bucket, object_name=objeto, filename=tmp.name)
            total_rows += pq.ParquetFile(tmp.name).metadata.num_rows

    print(f"Total de filas procesadas para '{fecha}': {total_rows}")

    if total_rows < min_rows:
        raise AirflowException(
            f"Row count ({total_rows}) por debajo del mínimo configurado "
            f"({min_rows}) para 'gs://{bucket}/{prefix}'."
        )


with DAG(
    dag_id="dag_2_transform",
    description="Transforma órdenes de raw/ a Parquet en processed/.",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "etl", "transform"],
    doc_md=__doc__,
) as dag:

    transform_orders = PythonOperator(
        task_id="transform_orders",
        python_callable=_transformar_ordenes,
        doc_md="""
        ### Transformar órdenes
        Lee los JSONs de `raw/`, valida cada orden, separa los datos en
        `orders` y `order_items`, manda los rechazados a `rejected/{{ ds }}/`
        y escribe Parquet en `processed/{{ ds }}/`. Falla si el porcentaje
        de registros válidos cae por debajo de `MIN_VALID_RECORDS_PCT`.
        """,
    )

    validate_row_count = PythonOperator(
        task_id="validate_row_count",
        python_callable=_validar_row_count,
        doc_md="""
        ### Validar row count
        Suma las filas de los Parquet generados en `processed/{{ ds }}/` y
        falla si el total es menor que la Variable `min_processed_rows`.
        """,
    )

    trigger_dag_3_bq_load = TriggerDagRunOperator(
        task_id="trigger_dag_3_bq_load",
        trigger_dag_id="dag_3_bq_load",
        doc_md="""
        ### Disparar dag_3_bq_load
        Una vez validado el output de la transformación, dispara la carga a
        BigQuery.
        """,
    )

    transform_orders >> validate_row_count >> trigger_dag_3_bq_load
