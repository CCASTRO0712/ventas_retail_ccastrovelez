# Databricks notebook source
# MAGIC %md
# MAGIC # Setup del proyecto `ventas_retail_ccastrovelez`
# MAGIC
# MAGIC Crea, si no existen:
# MAGIC - El **catálogo** único del proyecto.
# MAGIC - Los **esquemas** por capa: `landing`, `bronze`, `silver`, `gold`.
# MAGIC - El **Volume** de landing donde se depositan los archivos crudos.
# MAGIC - Las carpetas por entidad dentro del Volume
# MAGIC   (`clientes`, `productos`, `pedidos`, `detalle_pedidos`).
# MAGIC
# MAGIC Este notebook se ejecuta como el primer task del Job (antes del
# MAGIC Declarative Pipeline) y también puede correrse manualmente.

# COMMAND ----------
dbutils.widgets.text("catalog", "proyecto_final")
dbutils.widgets.text("project_name", "ventas_retail_ccastrovelez")
dbutils.widgets.text("landing_schema", "landing")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("gold_schema", "gold")
dbutils.widgets.text("volume_name", "raw_data")

catalog = dbutils.widgets.get("catalog")
project_name = dbutils.widgets.get("project_name")
landing_schema = dbutils.widgets.get("landing_schema")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
volume_name = dbutils.widgets.get("volume_name")

# COMMAND ----------
# MAGIC %md ## 1. Catálogo y esquemas

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")

for schema in [landing_schema, bronze_schema, silver_schema, gold_schema]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

print(f"Catálogo '{catalog}' y esquemas ({landing_schema}, {bronze_schema}, "
      f"{silver_schema}, {gold_schema}) listos.")

# COMMAND ----------
# MAGIC %md ## 2. Volume de landing

# COMMAND ----------
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{landing_schema}`.`{volume_name}`"
)

volume_path = f"/Volumes/{catalog}/{landing_schema}/{volume_name}/{project_name}"

for entity in ["clientes", "productos", "pedidos", "detalle_pedidos"]:
    dbutils.fs.mkdirs(f"{volume_path}/{entity}")

print(f"Volume listo. Sube los batches de cada entidad a:\n  {volume_path}/<entidad>/")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Siguiente paso
# MAGIC
# MAGIC Sube los 12 archivos (4 entidades x 3 batches) a sus carpetas respectivas,
# MAGIC por ejemplo con el script `scripts/upload_data.sh` incluido en este
# MAGIC repositorio, o arrastrándolos manualmente desde el Catalog Explorer.
# MAGIC Luego ejecuta (o deja correr) el task `run_pipeline` del Job.
