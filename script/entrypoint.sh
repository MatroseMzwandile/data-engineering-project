#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
    pip install --user -r /opt/airflow/requirements.txt
fi

airflow db migrate

airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com || true

exec airflow webserver