"""
dags/dag_3_bq_load.py
-----------------------
DAG de carga a BigQuery del pipeline de e-commerce.

No tiene schedule propio: es disparado por `dag_2_transform`
(`trigger_dag_3_bq_load`) una vez que `processed/{{ ds }}/` tiene los
Parquet validados. Carga `orders.parquet` y `order_items.parquet` a sus
respectivas tablas en BigQuery (en paralelo) y, al terminar ambas cargas,
dispara `dag_4_metricas`.
"""

from datetime import datetime, timedelta
import json
import os
import sys

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.gcp_config import (  # noqa: E402
    BQ_LOCATION,
    BQ_TABLE_ORDER_ITEMS_REF,
    BQ_TABLE_ORDERS_REF,
    GCS_PROCESSED_PREFIX,
)

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": [Variable.get("alert_email", default_var="speterseng@gmail.com")],
}

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")

with open(os.path.join(SQL_DIR, "schema_orders.json"), encoding="utf-8") as f:
    SCHEMA_ORDERS = json.load(f)

with open(os.path.join(SQL_DIR, "schema_order_items.json"), encoding="utf-8") as f:
    SCHEMA_ORDER_ITEMS = json.load(f)


with DAG(
    dag_id="dag_3_bq_load",
    description="Carga los Parquet de processed/ a las tablas orders y order_items en BigQuery.",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "etl", "bq_load"],
    doc_md=__doc__,
) as dag:

    load_orders_to_bq = GCSToBigQueryOperator(
        task_id="load_orders_to_bq",
        bucket="{{ var.value.gcs_bucket }}",
        source_objects=[f"{GCS_PROCESSED_PREFIX}{{{{ ds }}}}/orders.parquet"],
        destination_project_dataset_table=BQ_TABLE_ORDERS_REF,
        source_format="PARQUET",
        schema_fields=SCHEMA_ORDERS,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        location=BQ_LOCATION,
        doc_md="""
        ### Cargar orders a BigQuery
        Carga `processed/{{ ds }}/orders.parquet` a la tabla `orders`
        (append) usando el schema definido en `sql/schema_orders.json`.
        """,
    )

    load_order_items_to_bq = GCSToBigQueryOperator(
        task_id="load_order_items_to_bq",
        bucket="{{ var.value.gcs_bucket }}",
        source_objects=[f"{GCS_PROCESSED_PREFIX}{{{{ ds }}}}/order_items.parquet"],
        destination_project_dataset_table=BQ_TABLE_ORDER_ITEMS_REF,
        source_format="PARQUET",
        schema_fields=SCHEMA_ORDER_ITEMS,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        location=BQ_LOCATION,
        doc_md="""
        ### Cargar order_items a BigQuery
        Carga `processed/{{ ds }}/order_items.parquet` a la tabla
        `order_items` (append) usando el schema definido en
        `sql/schema_order_items.json`.
        """,
    )

    trigger_dag_4_metricas = TriggerDagRunOperator(
        task_id="trigger_dag_4_metricas",
        trigger_dag_id="dag_4_metricas",
        doc_md="""
        ### Disparar dag_4_metricas
        Una vez cargadas ambas tablas, dispara el cálculo de métricas
        diarias.
        """,
    )

    [load_orders_to_bq, load_order_items_to_bq] >> trigger_dag_4_metricas
