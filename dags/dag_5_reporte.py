"""
dags/dag_5_reporte.py
------------------------
DAG de reporte diario del pipeline de e-commerce.

No tiene schedule propio: es disparado por `dag_4_metricas`
(`trigger_dag_5_reporte`) una vez guardada la Variable
`daily_metrics_latest`. Formatea esas métricas como HTML y envía el reporte
por email a los destinatarios de la Variable `report_email_list`.
"""

from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.models import Variable
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["{{ var.value.alert_email }}"],
}


def _formatear_reporte(**context) -> str:
    """
    Lee la Variable `daily_metrics_latest` y devuelve un HTML simple con las
    métricas diarias, para ser enviado por `send_report_email`.
    También empuja la lista de destinatarios a XCom para `send_report_email`.
    """
    metricas = json.loads(Variable.get("daily_metrics_latest"))
    email_list = json.loads(Variable.get("report_email_list", default_var='["ops@empresa.com"]'))
    context["ti"].xcom_push(key="email_list", value=email_list)

    return f"""
    <h2>Reporte diario de órdenes - {metricas['fecha']}</h2>
    <table border="1" cellpadding="6" cellspacing="0">
      <tr><td><b>Total de órdenes</b></td><td>{metricas['total_orders']}</td></tr>
      <tr><td><b>Revenue total</b></td><td>{metricas['total_revenue']:.2f}</td></tr>
      <tr><td><b>Ticket promedio</b></td><td>{metricas['avg_order_value']:.2f}</td></tr>
      <tr><td><b>Pendientes</b></td><td>{metricas['orders_pending']}</td></tr>
      <tr><td><b>Enviadas</b></td><td>{metricas['orders_shipped']}</td></tr>
      <tr><td><b>Entregadas</b></td><td>{metricas['orders_delivered']}</td></tr>
      <tr><td><b>Canceladas</b></td><td>{metricas['orders_cancelled']}</td></tr>
    </table>
    """


with DAG(
    dag_id="dag_5_reporte",
    description="Envía por email el reporte diario de métricas de órdenes.",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "etl", "reporte"],
    doc_md=__doc__,
) as dag:

    format_report = PythonOperator(
        task_id="format_report",
        python_callable=_formatear_reporte,
        doc_md="""
        ### Formatear reporte
        Lee la Variable `daily_metrics_latest` y genera el HTML del reporte
        diario.
        """,
    )

    send_report_email = EmailOperator(
        task_id="send_report_email",
        to="{{ ti.xcom_pull(task_ids='format_report', key='email_list') }}",
        subject="Reporte diario de órdenes - {{ ds }}",
        html_content="{{ ti.xcom_pull(task_ids='format_report') }}",
        doc_md="""
        ### Enviar reporte por email
        Envía el HTML generado por `format_report` a los destinatarios de
        la Variable `report_email_list`.
        """,
    )

    format_report >> send_report_email
