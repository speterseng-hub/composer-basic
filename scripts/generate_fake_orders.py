"""
scripts/generate_fake_orders.py
--------------------------------
Genera órdenes de e-commerce sintéticas usando Faker.

Cada orden tiene la forma:
{
    "order_id": "<uuid4>",
    "customer_id": "CUST-000123",
    "fecha": "2026-06-09T14:32:10",
    "estado": "pending" | "shipped" | "delivered" | "cancelled",
    "items": [
        {"product_id": "SKU-001", "nombre": "...", "cantidad": 2, "precio_unitario": 19.99},
        ...
    ],
    "ciudad": "Buenos Aires",
    "pais": "Argentina"
}

Modos de ejecución:
- Modo normal: publica cada orden como JSON a un topic de Pub/Sub
  (usado por dag_1_ingesta.py cuando no hay archivos nuevos en GCS raw/).
- Modo --local: guarda cada orden como un archivo .json en disco,
  útil para probar el pipeline sin necesitar credenciales de GCP.

Uso:
    python scripts/generate_fake_orders.py --count 100
    python scripts/generate_fake_orders.py --local --count 100 --output-dir /tmp/orders
"""

import argparse
import json
import os
import random
import sys
import uuid
from datetime import datetime, timedelta

from faker import Faker

# Permite ejecutar el script tanto desde la raíz del proyecto como desde scripts/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.gcp_config import (  # noqa: E402
    DEFAULT_ORDER_COUNT,
    ORDER_STATUSES,
    PUBSUB_TOPIC_FULL,
)

fake = Faker(["es_AR", "es_MX", "es_ES"])

# Catálogo de productos fijo. Cada orden elige items de aquí para mantener
# el dataset coherente entre ejecuciones (mismos product_id y precios).
PRODUCT_CATALOG = [
    {"product_id": "SKU-001", "nombre": "Auriculares Bluetooth", "precio_unitario": 29.99},
    {"product_id": "SKU-002", "nombre": "Smartwatch Deportivo", "precio_unitario": 89.50},
    {"product_id": "SKU-003", "nombre": "Mochila Antirrobo", "precio_unitario": 45.00},
    {"product_id": "SKU-004", "nombre": "Cargador Inalámbrico", "precio_unitario": 19.99},
    {"product_id": "SKU-005", "nombre": "Zapatillas Running", "precio_unitario": 65.00},
    {"product_id": "SKU-006", "nombre": "Termo de Acero Inoxidable", "precio_unitario": 22.50},
    {"product_id": "SKU-007", "nombre": "Teclado Mecánico", "precio_unitario": 75.00},
    {"product_id": "SKU-008", "nombre": "Mouse Inalámbrico", "precio_unitario": 15.99},
    {"product_id": "SKU-009", "nombre": "Lámpara LED de Escritorio", "precio_unitario": 32.00},
    {"product_id": "SKU-010", "nombre": "Funda para Notebook", "precio_unitario": 18.50},
    {"product_id": "SKU-011", "nombre": "Parlante Portátil", "precio_unitario": 39.99},
    {"product_id": "SKU-012", "nombre": "Silla Ergonómica", "precio_unitario": 220.00},
    {"product_id": "SKU-013", "nombre": "Monitor 24 pulgadas", "precio_unitario": 159.00},
    {"product_id": "SKU-014", "nombre": "Webcam HD", "precio_unitario": 27.99},
    {"product_id": "SKU-015", "nombre": "Botella Deportiva", "precio_unitario": 9.99},
]

# Pesos de probabilidad para cada estado de orden (suman 1.0)
ESTADO_WEIGHTS = {
    "pending": 0.20,
    "shipped": 0.30,
    "delivered": 0.40,
    "cancelled": 0.10,
}


def generar_items() -> list:
    """Genera entre 1 y 5 items aleatorios tomados del catálogo de productos."""
    cantidad_items = random.randint(1, 5)
    productos_elegidos = random.sample(PRODUCT_CATALOG, k=cantidad_items)

    items = []
    for producto in productos_elegidos:
        items.append({
            "product_id": producto["product_id"],
            "nombre": producto["nombre"],
            "cantidad": random.randint(1, 4),
            "precio_unitario": producto["precio_unitario"],
        })
    return items


def generar_orden() -> dict:
    """Genera una orden de e-commerce sintética completa."""
    estados = list(ESTADO_WEIGHTS.keys())
    pesos = list(ESTADO_WEIGHTS.values())

    ayer = (datetime.now() - timedelta(days=1)).date()
    inicio_ayer = datetime.combine(ayer, datetime.min.time())
    fin_ayer = datetime.combine(ayer, datetime.max.time())

    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"CUST-{random.randint(1, 50000):06d}",
        "fecha": fake.date_time_between_dates(datetime_start=inicio_ayer, datetime_end=fin_ayer).isoformat(),
        "estado": random.choices(estados, weights=pesos, k=1)[0],
        "items": generar_items(),
        "ciudad": fake.city(),
        "pais": fake.country(),
    }


def guardar_local(ordenes: list, output_dir: str) -> None:
    """Guarda todas las órdenes como un único archivo JSONL en output_dir."""
    try:
        os.makedirs(output_dir, exist_ok=True)
    except OSError as e:
        print(f"Error creando el directorio de salida '{output_dir}': {e}")
        sys.exit(1)

    fecha = ordenes[0]["fecha"][:10] if ordenes else datetime.now().strftime("%Y-%m-%d")
    ruta_archivo = os.path.join(output_dir, f"orders_{fecha}.jsonl")
    try:
        with open(ruta_archivo, "w", encoding="utf-8") as f:
            for orden in ordenes:
                f.write(json.dumps(orden, ensure_ascii=False) + "\n")
    except OSError as e:
        print(f"Error escribiendo el archivo '{ruta_archivo}': {e}")
        sys.exit(1)

    print(f"Se guardaron {len(ordenes)} órdenes en '{ruta_archivo}'.")


def publicar_pubsub(ordenes: list, topic_path: str) -> None:
    """Publica cada orden como mensaje JSON en el topic de Pub/Sub indicado."""
    try:
        from google.cloud import pubsub_v1
    except ImportError:
        print(
            "No se pudo importar 'google.cloud.pubsub_v1'. "
            "Instalá 'google-cloud-pubsub' o usá el modo --local."
        )
        sys.exit(1)

    try:
        publisher = pubsub_v1.PublisherClient()
    except Exception as e:
        print(f"Error inicializando el cliente de Pub/Sub: {e}")
        sys.exit(1)

    publicadas = 0
    fallidas = 0

    for orden in ordenes:
        try:
            payload = json.dumps(orden, ensure_ascii=False).encode("utf-8")
            future = publisher.publish(topic_path, data=payload)
            future.result(timeout=30)
            publicadas += 1
        except Exception as e:
            print(f"Error publicando orden '{orden['order_id']}': {e}")
            fallidas += 1

    print(f"Publicación finalizada: {publicadas} ok, {fallidas} fallidas, topic='{topic_path}'.")


def main():
    parser = argparse.ArgumentParser(
        description="Genera órdenes de e-commerce sintéticas con Faker."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=DEFAULT_ORDER_COUNT,
        help=f"Cantidad de órdenes a generar (default: {DEFAULT_ORDER_COUNT}).",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Si se especifica, guarda las órdenes como JSON en disco en lugar de publicarlas en Pub/Sub.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="local_orders",
        help="Directorio de salida cuando se usa --local (default: ./local_orders).",
    )

    args = parser.parse_args()

    print(f"Generando {args.count} órdenes...")
    ordenes = [generar_orden() for _ in range(args.count)]

    if args.local:
        guardar_local(ordenes, args.output_dir)
    else:
        publicar_pubsub(ordenes, PUBSUB_TOPIC_FULL)


if __name__ == "__main__":
    main()