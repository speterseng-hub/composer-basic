"""
dags/dag_1_ingesta.py
----------------------
DAG de ingesta diaria para el pipeline de e-commerce.

Verifica si hay archivos nuevos en GCS bajo el prefijo `incoming/`. Si los
hay, los mueve a `raw/` para que `dag_2_transform` los procese. Si no hay
archivos nuevos, genera órdenes sintéticas con Faker y las sube directamente
a `raw/`, garantizando que siempre haya datos disponibles para el resto del
pipeline.
"""
#%%
from datetime import datetime, timedelta
import json
import os
import sys

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

# Permite importar el generador de órdenes desde scripts/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.gcp_config import (  # noqa: E402
    DEFAULT_ORDER_COUNT,
    GCS_INCOMING_PREFIX,
    GCS_RAW_PREFIX,
)
from scripts.generate_fake_orders import generar_orden  # noqa: E402
#%%

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["{{ var.value.alert_email }}"],
}


def _decide_data_source(**context) -> str:
    """
    Revisa si hay archivos nuevos en `incoming/`. Devuelve el task_id de la
    rama a ejecutar: mover los archivos existentes o generar datos falsos.
    """
    bucket = Variable.get("gcs_bucket")
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    archivos = hook.list(bucket_name=bucket, prefix=GCS_INCOMING_PREFIX)
    # El propio prefijo puede listarse como un objeto "carpeta" vacío
    archivos = [f for f in archivos if not f.endswith("/")]

    if archivos:
        context["ti"].xcom_push(key="incoming_files", value=archivos)
        return "move_incoming_to_raw"
    return "generate_fake_data"


def _generar_y_subir_ordenes(**context) -> None:
    """
    Genera órdenes sintéticas con Faker y las sube como un único archivo
    JSONL a `raw/` en GCS (una orden por línea).
    """
    bucket = Variable.get("gcs_bucket")
    count = int(Variable.get("fake_orders_count", default_var=DEFAULT_ORDER_COUNT))
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    ds = context["ds"]

    ordenes = [generar_orden() for _ in range(count)]
    jsonl = "\n".join(json.dumps(o, ensure_ascii=False) for o in ordenes)
    nombre_archivo = f"{GCS_RAW_PREFIX}orders_{ds}.jsonl"

    hook.upload(
        bucket_name=bucket,
        object_name=nombre_archivo,
        data=jsonl,
        mime_type="application/jsonlines",
    )
    print(f"Se generaron y subieron {count} órdenes en 'gs://{bucket}/{nombre_archivo}'.")


with DAG(
    dag_id="dag_1_ingesta",
    description="Ingesta diaria de órdenes de e-commerce a GCS raw/.",
    default_args=default_args,
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "etl", "ingesta"],
    doc_md=__doc__,
) as dag:

    decide_data_source = BranchPythonOperator(
        task_id="decide_data_source",
        python_callable=_decide_data_source,
        doc_md="""
        ### Decidir origen de datos
        Revisa el prefijo `incoming/` en GCS. Si hay archivos nuevos, la
        siguiente tarea los mueve a `raw/`. Si no hay nada, se generan
        órdenes sintéticas con Faker.
        """,
    )

    move_incoming_to_raw = GCSToGCSOperator(
        task_id="move_incoming_to_raw",
        source_bucket="{{ var.value.gcs_bucket }}",
        source_object=f"{GCS_INCOMING_PREFIX}*",
        destination_bucket="{{ var.value.gcs_bucket }}",
        destination_object=GCS_RAW_PREFIX,
        move_object=True,
        doc_md="""
        ### Mover archivos a raw/
        Mueve (copia y borra el origen) los archivos encontrados en
        `incoming/` hacia `raw/`.
        """,
    )

    generate_fake_data = PythonOperator(
        task_id="generate_fake_data",
        python_callable=_generar_y_subir_ordenes,
        doc_md="""
        ### Generar órdenes sintéticas
        No se encontraron archivos nuevos en `incoming/`. Se generan
        órdenes de e-commerce con Faker y se suben directamente a `raw/`.
        """,
    )

    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
        doc_md="""
        ### Punto de unión
        Marca el fin de la ingesta, sin importar qué rama se haya ejecutado.
        """,
    )

    trigger_dag_2_transform = TriggerDagRunOperator(
        task_id="trigger_dag_2_transform",
        trigger_dag_id="dag_2_transform",
        doc_md="""
        ### Disparar dag_2_transform
        Una vez que `raw/` tiene datos nuevos (movidos o generados), dispara
        inmediatamente `dag_2_transform` en lugar de esperar a su propio
        schedule.
        """,
    )

    decide_data_source >> [move_incoming_to_raw, generate_fake_data] >> join >> trigger_dag_2_transform