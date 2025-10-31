#!/bin/bash

set -e

echo "=== Dota 2 ETL Analytics Pipeline ==="

# Create directories
mkdir -p data/{raw,processed,insights} logs dags notebooks/output tests src config airflow_home
mkdir -p minio-data postgres-data hdfs-data/{namenode,datanode}

# Set permissions
chmod -R 755 data logs airflow_home minio-data postgres-data hdfs-data

# Build container
echo "Building container image..."
podman build -t dota-pipeline:latest .

# Create pod with all ports
echo "Creating pod..."
podman pod create --name dota-pod \
  -p 127.0.0.1:8080:8080 \
  -p 127.0.0.1:8888:8888 \
  -p 127.0.0.1:4040:4040 \
  -p 127.0.0.1:9000:9000 \
  -p 127.0.0.1:9001:9001 \
  -p 127.0.0.1:5432:5432 \
  -p 127.0.0.1:9870:9870

# Start MinIO (S3-compatible storage)
echo "Starting MinIO..."
podman run -d --name minio --pod dota-pod \
  -e MINIO_ROOT_USER=admin \
  -e MINIO_ROOT_PASSWORD=password123 \
  -v ./minio-data:/data:Z \
  quay.io/minio/minio:latest server /data --console-address ":9001"

# Start PostgreSQL
echo "Starting PostgreSQL..."
podman run -d --name postgres --pod dota-pod \
  -e POSTGRES_PASSWORD=password123 \
  -e POSTGRES_USER=dota \
  -e POSTGRES_DB=dota_pipeline \
  -v ./postgres-data:/var/lib/postgresql/data:Z \
  postgres:15

# Wait for services
sleep 5

# Initialize MinIO bucket
echo "Initializing MinIO bucket..."
podman exec minio mc alias set local http://localhost:9000 admin password123
podman exec minio mc mb local/dota-pipeline --ignore-existing

# Initialize Airflow DB
echo "Initializing Airflow database..."
podman run --rm --pod dota-pod \
  -v ./dags:/opt/airflow/dags:Z \
  -v ./notebooks:/opt/airflow/notebooks:Z \
  -v ./data:/opt/airflow/data:Z \
  -v ./logs:/opt/airflow/logs:Z \
  -v ./airflow_home:/opt/airflow/airflow_home:Z \
  -v ./tests:/opt/airflow/tests:Z \
  -v ./src:/opt/airflow/src:Z \
  -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow_home/airflow.db \
  -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
  -e AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0 \
  dota-pipeline:latest airflow db init

# Create admin user
echo "Creating Airflow admin user..."
podman run --rm --pod dota-pod \
  -v ./dags:/opt/airflow/dags:Z \
  -v ./notebooks:/opt/airflow/notebooks:Z \
  -v ./data:/opt/airflow/data:Z \
  -v ./logs:/opt/airflow/logs:Z \
  -v ./airflow_home:/opt/airflow/airflow_home:Z \
  -v ./tests:/opt/airflow/tests:Z \
  -v ./src:/opt/airflow/src:Z \
  -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow_home/airflow.db \
  -e AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0 \
  dota-pipeline:latest \
  airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

echo "Starting services..."

# Start Jupyter Lab
podman run -d --name jupyter-lab --pod dota-pod \
  -v ./notebooks:/opt/airflow/notebooks:Z \
  -v ./data:/opt/airflow/data:Z \
  -v ./src:/opt/airflow/src:Z \
  -v ./logs:/opt/airflow/logs:Z \
  -v ./tests:/opt/airflow/tests:Z \
  -e PYTHONPATH=/opt/airflow \
  -w /opt/airflow/notebooks \
  dota-pipeline:latest \
  jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password=''

# Start Airflow webserver
podman run -d --name airflow-webserver --pod dota-pod \
  -v ./dags:/opt/airflow/dags:Z \
  -v ./notebooks:/opt/airflow/notebooks:Z \
  -v ./data:/opt/airflow/data:Z \
  -v ./logs:/opt/airflow/logs:Z \
  -v ./src:/opt/airflow/src:Z \
  -v ./airflow_home:/opt/airflow/airflow_home:Z \
  -v ./tests:/opt/airflow/tests:Z \
  -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow_home/airflow.db \
  -e AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0 \
  -e AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080 \
  dota-pipeline:latest airflow webserver

# Start Airflow scheduler
podman run -d --name airflow-scheduler --pod dota-pod \
  -v ./dags:/opt/airflow/dags:Z \
  -v ./notebooks:/opt/airflow/notebooks:Z \
  -v ./data:/opt/airflow/data:Z \
  -v ./logs:/opt/airflow/logs:Z \
  -v ./src:/opt/airflow/src:Z \
  -v ./airflow_home:/opt/airflow/airflow_home:Z \
  -v ./tests:/opt/airflow/tests:Z \
  -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow_home/airflow.db \
  dota-pipeline:latest airflow scheduler

sleep 5

echo ""
echo "✓ Setup complete!"
echo ""
echo "Services:"
echo "  - Jupyter Lab: http://localhost:8888 (no password)"
echo "  - Airflow UI: http://localhost:8080 (admin/admin)"
echo "  - MinIO Console: http://localhost:9001 (admin/password123)"
echo "  - MinIO API: http://localhost:9000"
echo "  - PostgreSQL: localhost:5432 (dota/password123)"
echo "  - Spark UI: http://localhost:4040 (when job running)"
echo ""
echo "SSH Tunnel (if remote):"
echo "  ssh -L 8888:0.0.0.0:8888 -L 8080:0.0.0.0:8080 -L 9001:0.0.0.0:9001 user@server"
echo ""
echo "Management:"
echo "  - Check: podman ps --pod"
echo "  - Stop: podman pod stop dota-pod"
echo "  - Remove: podman pod rm -f dota-pod"
echo "  - Tests: podman exec -it airflow-scheduler pytest /opt/airflow/tests -v"

