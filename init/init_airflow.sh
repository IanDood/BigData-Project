#!/bin/bash

echo "Initializing Airflow DB..."

# Wait for PostgreSQL to be available
until airflow db check; do
  echo "Waiting for PostgreSQL to be available..."
  sleep 2
done

# Initialize DB
airflow db init

# Create admin user (if not already created)
airflow users list | grep -q ian
if [ $? -ne 0 ]; then
  airflow users create \
    --username ian \
    --firstname Ian \
    --lastname Romero \
    --role Admin \
    --email ian.romero@edu.uag.mx \
    --password admin
fi

# Mark initialization complete
touch /opt/airflow/init/airflow_initialized.flag

echo "Airflow initialization complete."

