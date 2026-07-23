# -----------------------------------------------------------------------------
# CAPA BRONZE
# Ingesta cruda vía STREAM (Auto Loader / cloudFiles) desde el Volume de landing.
# Sin transformación de negocio: solo se agregan columnas de metadata de
# ingesta (_ingest_timestamp, _source_file). Todas las tablas son STREAMING
# TABLE, construidas 100% con Spark Declarative Pipelines.
#
# IMPORTANTE: para publicar en un esquema de Unity Catalog distinto al
# esquema por defecto del pipeline (que es "gold"), el esquema se indica
# DENTRO del "name" de la tabla (ej. "bronze.clientes_raw"), no con un
# parámetro "schema=" aparte -- ese parámetro sirve para otra cosa
# (definir explícitamente el StructType de columnas) y no para elegir el
# esquema de Unity Catalog.
# -----------------------------------------------------------------------------

import dlt
from pyspark.sql.functions import col, current_timestamp

landing_path = spark.conf.get("landing.volume.path")
bronze_schema = spark.conf.get("bronze.schema")


def _read_stream(entity: str, fmt: str, **options):
    """Lector incremental (STREAM) genérico basado en Auto Loader."""
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.schemaLocation", f"{landing_path}/_schemas/{entity}")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    )
    for key, value in options.items():
        reader = reader.option(key, value)
    return (
        reader.load(f"{landing_path}/{entity}")
        .withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


@dlt.table(
    name=f"{bronze_schema}.clientes_raw",
    comment="Ingesta cruda (STREAM) de los batches CSV de clientes.",
)
def clientes_raw():
    return _read_stream("clientes", "csv", header="true")


@dlt.table(
    name=f"{bronze_schema}.productos_raw",
    comment="Ingesta cruda (STREAM) de los batches CSV de productos.",
)
def productos_raw():
    return _read_stream("productos", "csv", header="true")


@dlt.table(
    name=f"{bronze_schema}.pedidos_raw",
    comment="Ingesta cruda (STREAM) de los batches JSON de pedidos.",
)
def pedidos_raw():
    # Cada batch es un arreglo JSON (no JSON-lines) -> multiLine=true
    return _read_stream("pedidos", "json", multiLine="true")


@dlt.table(
    name=f"{bronze_schema}.detalle_pedidos_raw",
    comment="Ingesta cruda (STREAM) de los batches JSON de detalle_pedidos.",
)
def detalle_pedidos_raw():
    return _read_stream("detalle_pedidos", "json", multiLine="true")
