#!/bin/bash

TARGET_DIR="/opt/airflow/nba_data"

# Set file permissions to readable and writable
find "$TARGET_DIR" -type f -exec chmod 666 {} \;

# Set directory permissions to readable, writable, and executable
find "$TARGET_DIR" -type d -exec chmod 777 {} \;

