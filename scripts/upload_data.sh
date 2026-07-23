#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Sube los 12 archivos batch (4 entidades x 3 batches) al Volume de landing.
# Requiere Databricks CLI configurado (`databricks configure`).
#
# Uso:
#   ./scripts/upload_data.sh <catalog> <landing_schema> <volume_name> <project_name>
#
# Ejemplo:
#   ./scripts/upload_data.sh proyecto_final landing raw_data ventas_retail_ccastrovelez
# -----------------------------------------------------------------------------
set -euo pipefail

CATALOG="${1:-proyecto_final}"
LANDING_SCHEMA="${2:-landing}"
VOLUME_NAME="${3:-raw_data}"
PROJECT_NAME="${4:-ventas_retail_ccastrovelez}"

BASE_PATH="/Volumes/${CATALOG}/${LANDING_SCHEMA}/${VOLUME_NAME}/${PROJECT_NAME}"

echo "Subiendo archivos a ${BASE_PATH} ..."

for entity in clientes productos pedidos detalle_pedidos; do
  echo "-> ${entity}"
  for file in "$(dirname "$0")/../data/${entity}/"*; do
    filename="$(basename "$file")"
    databricks fs cp "$file" "dbfs:${BASE_PATH}/${entity}/${filename}" --overwrite
  done
done

echo "Listo. Verifica con: databricks fs ls dbfs:${BASE_PATH}"
