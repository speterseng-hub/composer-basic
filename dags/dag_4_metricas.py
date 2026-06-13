"""
dags/dag_4_metricas.py
------------------------
DAG de métricas diarias del pipeline de e-commerce.

No tiene schedule propio: es disparado por `dag_3_bq_load`
(`trigger_dag_4_metricas`) una vez cargadas las tablas `orders` y
`order_items`. Ejecuta `sql/metricas_diarias.sql` sobre `orders` para
`{{ ds }}`, inserta el resultado en `daily_metrics`, guarda esas métricas en
la Variable `daily_metrics_latest` y dispara `dag_5_reporte`.
"""

from datetime import datetime, timedelta
import json
import os
import sys

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.gcp_config import (  # noqa: E402
    BQ_DATASET,
    BQ_LOCATION,
    BQ_TABLE_DAILY_METRICS,
    BQ_TABLE_DAILY_METRICS_REF,
    BQ_TABLE_ORDERS_REF,
    GCP_PROJECT_ID,
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

with open(os.path.join(SQL_DIR, "metricas_diarias.sql"), encoding="utf-8") as f:
    SQL_METRICAS_DIARIAS = f.read()


def _guardar_metricas_en_variable(**context) -> None:
    """
    Consulta la fila de `daily_metrics` correspondiente a `{{ ds }}` y la
    guarda como JSON en la Variable `daily_metrics_latest`, para que
    `dag_5_reporte` la lea sin volver a consultar BigQuery.
    """
    fecha = context["ds"]
    hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)

    query = f"SELECT * FROM `{BQ_TABLE_DAILY_METRICS_REF}` WHERE fecha = @fecha"
    df = hook.get_pandas_df(
        sql=query,
        parameters=[{"name": "fecha", "parameterType": {"type": "DATE"}, "parameterValue": {"value": fecha}}],
        dialect="standard",
    )

    if df.empty:
        raise ValueError(f"No se encontraron métricas en '{BQ_TABLE_DAILY_METRICS_REF}' para '{fecha}'.")

    metricas = df.iloc[0].to_dict()
    metricas["fecha"] = str(metricas["fecha"])

    Variable.set("daily_metrics_latest", json.dumps(metricas, ensure_ascii=False))
    print(f"Métricas guardadas en 'daily_metrics_latest': {metricas}")


with DAG(
    dag_id="dag_4_metricas",
    description="Calcula y guarda las métricas diarias de órdenes.",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "etl", "metricas"],
    doc_md=__doc__,
) as dag:

    run_metrics_query = BigQueryInsertJobOperator(
        task_id="run_metrics_query",
        configuration={
            "query": {
                "query": SQL_METRICAS_DIARIAS,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BQ_DATASET,
                    "tableId": BQ_TABLE_DAILY_METRICS,
                },
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
                "queryParameters": [
                    {"name": "fecha", "parameterType": {"type": "DATE"}, "parameterValue": {"value": "{{ ds }}"}},
                ],
            }
        },
        params={"orders_table": BQ_TABLE_ORDERS_REF},
        location=BQ_LOCATION,
        doc_md="""
        ### Calcular métricas diarias
        Ejecuta `sql/metricas_diarias.sql` sobre `orders` para `{{ ds }}` e
        inserta el resultado (append) en `daily_metrics`.
        """,
    )

    save_metrics_to_variable = PythonOperator(
        task_id="save_metrics_to_variable",
        python_callable=_guardar_metricas_en_variable,
        doc_md="""
        ### Guardar métricas en Variable
        Lee la fila de `daily_metrics` para `{{ ds }}` y la guarda como JSON
        en la Variable `daily_metrics_latest`.
        """,
    )

    trigger_dag_5_reporte = TriggerDagRunOperator(
        task_id="trigger_dag_5_reporte",
        trigger_dag_id="dag_5_reporte",
        doc_md="""
        ### Disparar dag_5_reporte
        Una vez guardadas las métricas, dispara el envío del reporte por
        email.
        """,
    )

    run_metrics_query >> save_metrics_to_variable >> trigger_dag_5_reporte