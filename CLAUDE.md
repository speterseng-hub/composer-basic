# Pipeline E-commerce — Documentación para Claude Code

## Descripción del proyecto

Pipeline de datos end-to-end para procesar órdenes de e-commerce en GCP.
Los eventos de órdenes se generan con Faker (o llegan desde Pub/Sub), se almacenan como JSON crudo en GCS,
se transforman a Parquet con un PythonOperator, se cargan a BigQuery y se reportan por email diariamente.

## Arquitectura

```
Pub/Sub / Faker
      │
      ▼
  GCS raw/          ← JSONs de órdenes crudas
      │
      ▼
  Transformación    ← dag_2_transform (PythonOperator: pandas + pyarrow)
      │
      ▼
  GCS processed/    ← Parquet particionado por fecha
      │
      ▼
  BigQuery          ← tablas: orders, order_items, daily_metrics
      │
      ▼
  Email reporte     ← DAG 5, vía Airflow EmailOperator
```

### Flujo de DAGs

| DAG | Trigger | Responsabilidad |
|-----|---------|-----------------|
| `dag_1_ingesta` | Diario 06:00 UTC | Sensor GCS → genera datos si no hay → mueve a GCS raw/ |
| `dag_2_transform` | Disparado por dag_1 | Lee JSONs de raw/ → valida y separa en orders/order_items → escribe Parquet en processed/ → valida row count |
| `dag_3_bq_load` | Disparado por dag_2 | Carga orders.parquet y order_items.parquet de GCS processed/ → BigQuery |
| `dag_4_metricas` | Disparado por dag_3 | Corre SQL de métricas → inserta en daily_metrics → guarda en Variable `daily_metrics_latest` |
| `dag_5_reporte` | Disparado por dag_4 | Lee `daily_metrics_latest` → formatea HTML → envía email |

## Estructura de carpetas

```
composer/
├── CLAUDE.md                  ← este archivo
├── README.md                  ← guía de setup para humanos
├── requirements.txt           ← dependencias Python
├── config/
│   └── gcp_config.py          ← constantes del proyecto (env vars)
├── dags/
│   ├── dag_1_ingesta.py
│   ├── dag_2_transform.py
│   ├── dag_3_bq_load.py
│   ├── dag_4_metricas.py
│   └── dag_5_reporte.py
├── scripts/
│   └── generate_fake_orders.py ← generador de datos con Faker
├── sql/
│   ├── schema_orders.json
│   ├── schema_order_items.json
│   ├── schema_daily_metrics.json
│   └── metricas_diarias.sql
└── tests/
    ├── test_transform.py       ← unit tests de la transformación de órdenes
    └── test_dag_structure.py   ← tests de estructura de DAGs
```

## Convenciones de código

- **Idioma:** docstrings y comentarios en español. Nombres de variables y funciones en inglés (snake_case).
- **Configuración:** toda constante va en `config/gcp_config.py`. Los DAGs leen parámetros con `Variable.get()`.
- **Operadores:** se usan operadores clásicos de Airflow (no TaskFlow API / @task decorator).
- **Manejo de errores:** bloques `try/except` explícitos en operadores Python; nunca ignorar excepciones silenciosamente.
- **Default args:** definidos una sola vez por DAG, con `retries=2`, `retry_delay=timedelta(minutes=5)`.
- **Tags:** todos los DAGs llevan `tags=['ecommerce', 'etl']` más tags específicos.
- **Doc MD:** cada task tiene `doc_md` explicando su propósito.

## Variables de Airflow requeridas

Estas variables deben existir en Airflow antes de ejecutar los DAGs:

| Variable | Descripción | Ejemplo |
|----------|-------------|---------|
| `gcp_project_id` | ID del proyecto GCP | `mi-proyecto-gcp` |
| `gcs_bucket` | Bucket principal | `ecommerce-pipeline-bucket` |
| `bq_dataset` | Dataset de BigQuery | `ecommerce_dw` |
| `report_email_list` | Emails destinatarios (JSON list) | `["ops@empresa.com"]` |
| `alert_email` | Email para alertas de fallo | `ops@empresa.com` |
| `min_processed_rows` | Mínimo de filas esperadas en `processed/` para que dag_2_transform valide OK | `1` |
| `fake_orders_count` | Cantidad de órdenes a generar en dag_1_ingesta si no hay datos en `incoming/` | `100` |

## Variables de entorno requeridas (para desarrollo local)

```bash
GCP_PROJECT_ID=mi-proyecto-gcp
GCS_BUCKET=ecommerce-pipeline-bucket
BQ_DATASET=ecommerce_dw
GOOGLE_APPLICATION_CREDENTIALS=/ruta/a/service-account.json
```

## Cómo correr cada componente

### Generador de datos (modo local)
```bash
python scripts/generate_fake_orders.py --local --count 100 --output-dir /tmp/orders
```

### Generador de datos (modo Pub/Sub)
```bash
python scripts/generate_fake_orders.py --count 100
```

### Tests
```bash
pytest tests/ -v
```

## Dependencias clave

- `apache-airflow-providers-google>=10.0.0` — operadores GCS, BQ
- `pandas>=2.0.0` — transformación de órdenes
- `faker>=20.0.0` — generación de datos sintéticos
- `pyarrow>=14.0.0` — escritura de Parquet
- `google-cloud-pubsub>=2.18.0` — publicación de eventos