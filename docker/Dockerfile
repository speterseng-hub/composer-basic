# Imagen de Airflow extendida con los providers/paquetes que usan los DAGs
# del pipeline (apache-airflow-providers-google, Faker).
FROM apache/airflow:3.2.2-python3.12

COPY docker/requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt