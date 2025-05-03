#!/bin/bash

echo "Waiting for Airflow to be initialized..."

while [ ! -f /opt/airflow/init/airflow_initialized.flag ]; do
  echo "Still waiting for init..."
  sleep 2
done

echo "Airflow is initialized. Starting webserver..."
exec airflow webserver

