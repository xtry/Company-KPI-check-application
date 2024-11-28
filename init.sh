#!/bin/bash

# Set the path to the pg_data folder (relative path for current directory)
PG_DATA_DIR="./pg_data"

# Check if the folder exists
if [ -d "$PG_DATA_DIR" ]; then
  echo "Folder $PG_DATA_DIR exists. Deleting..."
  rm -rf "$PG_DATA_DIR"  # Delete the directory and its contents
else
  echo "Folder $PG_DATA_DIR does not exist."
fi


mkdir -p ./airflow/logs/scheduler
chmod 777 -R ./airflow/logs

mkdir -p ./dbt/logs
chmod 777 -R ./dbt
