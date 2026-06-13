"""
config/gcp_config.py
--------------------
Constantes centralizadas del proyecto. Todos los valores sensibles se leen
desde variables de entorno para evitar credenciales en el código fuente.

Los DAGs y scripts importan desde aquí en lugar de hardcodear valores.
"""

import os

# ---------------------------------------------------------------------------
# Proyecto GCP
# ---------------------------------------------------------------------------

# ID del proyecto en Google Cloud Platform
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "mi-proyecto-gcp")

# Región principal para los servicios (Dataflow, Composer)
GCP_REGION = os.getenv("GCP_REGION", "us-central1")

# Zona dentro de la región (usada por algunos recursos de Dataflow)
GCP_ZONE = os.getenv("GCP_ZONE", "us-central1-a")

# Ruta al archivo de credenciales de la cuenta de servicio (solo desarrollo local)
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

# ---------------------------------------------------------------------------
# Google Cloud Storage
# ---------------------------------------------------------------------------

# Bucket principal del proyecto
GCS_BUCKET = os.getenv("GCS_BUCKET", "ecommerce-pipeline-bucket")

# Prefijos de carpetas dentro del bucket
GCS_INCOMING_PREFIX = "incoming/"       # Archivos nuevos pendientes de mover a raw/
GCS_RAW_PREFIX = "raw/"                  # JSONs crudos de órdenes
GCS_PROCESSED_PREFIX = "processed/"     # Parquet transformados
GCS_REJECTED_PREFIX = "rejected/"       # Registros que fallaron validación

# Paths completos (gs://...)
GCS_INCOMING_PATH = f"gs://{GCS_BUCKET}/{GCS_INCOMING_PREFIX}"
GCS_RAW_PATH = f"gs://{GCS_BUCKET}/{GCS_RAW_PREFIX}"
GCS_PROCESSED_PATH = f"gs://{GCS_BUCKET}/{GCS_PROCESSED_PREFIX}"
GCS_REJECTED_PATH = f"gs://{GCS_BUCKET}/{GCS_REJECTED_PREFIX}"

# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------

# Dataset principal donde viven todas las tablas del pipeline
BQ_DATASET = os.getenv("BQ_DATASET", "ecommerce_dw")

# Nombres de tablas
BQ_TABLE_ORDERS = "orders"
BQ_TABLE_ORDER_ITEMS = "order_items"
BQ_TABLE_DAILY_METRICS = "daily_metrics"

# Referencias completas project.dataset.table
BQ_TABLE_ORDERS_REF = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_ORDERS}"
BQ_TABLE_ORDER_ITEMS_REF = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_ORDER_ITEMS}"
BQ_TABLE_DAILY_METRICS_REF = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_DAILY_METRICS}"

# Ubicación geográfica del dataset
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

# ---------------------------------------------------------------------------
# Pub/Sub
# ---------------------------------------------------------------------------

# Topic al que el generador publica los eventos de órdenes
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC", "ecommerce-orders")

# Nombre completo del topic (projects/PROJECT/topics/TOPIC)
PUBSUB_TOPIC_FULL = f"projects/{GCP_PROJECT_ID}/topics/{PUBSUB_TOPIC}"

# Subscription para consumir los mensajes
PUBSUB_SUBSCRIPTION = os.getenv("PUBSUB_SUBSCRIPTION", "ecommerce-orders-sub")

# ---------------------------------------------------------------------------
# Email / notificaciones
# ---------------------------------------------------------------------------

# Email que aparece como remitente en los reportes diarios
EMAIL_FROM = os.getenv("EMAIL_FROM", "pipeline@empresa.com")

# Email de alertas operacionales (fallos de DAG)
EMAIL_ALERT = os.getenv("EMAIL_ALERT", "ops@empresa.com")

# Lista de destinatarios del reporte diario (separados por coma)
EMAIL_REPORT_LIST = os.getenv("EMAIL_REPORT_LIST", "ops@empresa.com").split(",")

# ---------------------------------------------------------------------------
# Pipeline / negocio
# ---------------------------------------------------------------------------

# Estados válidos para una orden de e-commerce
ORDER_STATUSES = ["pending", "shipped", "delivered", "cancelled"]

# Número de registros que genera el script Faker por defecto
DEFAULT_ORDER_COUNT = int(os.getenv("DEFAULT_ORDER_COUNT", "50000"))

# Umbral mínimo de registros válidos (porcentaje, 0-100)
# Si la tasa de rechazo supera este umbral, la transformación falla
MIN_VALID_RECORDS_PCT = float(os.getenv("MIN_VALID_RECORDS_PCT", "95.0"))