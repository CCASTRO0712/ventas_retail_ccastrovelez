# -----------------------------------------------------------------------------
# CAPA SILVER
# Limpieza, tipado, deduplicación y expectations sobre los datos de Bronze.
# Todas las tablas son STREAMING TABLE (STREAM), leídas incrementalmente desde
# Bronze con dlt.read_stream(). Las expectations aquí validan validez
# estructural y de formato de cada campo.
# -----------------------------------------------------------------------------

import dlt
import pyspark.sql.functions as F

bronze_schema = spark.conf.get("bronze.schema")
silver_schema = spark.conf.get("silver.schema")

_DROP_INGEST_COLS = ("_rescued_data", "_source_file", "_ingest_timestamp")


# ------------------------------- clientes ------------------------------------
@dlt.table(
    name=f"{silver_schema}.clientes",
    comment="Clientes limpios, tipados y deduplicados.",
)
@dlt.expect_or_fail("pk_customer_id_not_null", "customer_id IS NOT NULL")
@dlt.expect_or_drop(
    "valid_email_format",
    "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'",
)
@dlt.expect("valid_segmento", "segmento IN ('Retail', 'Premium')")
def clientes():
    df = dlt.read_stream(f"{bronze_schema}.clientes_raw")
    return (
        df.withColumn("nombre", F.trim(F.col("nombre")))
        .withColumn("apellido", F.trim(F.col("apellido")))
        .withColumn("email", F.trim(F.lower(F.col("email"))))
        .withColumn("ciudad", F.trim(F.col("ciudad")))
        .withColumn("pais", F.trim(F.col("pais")))
        .withColumn("fecha_registro", F.to_date(F.col("fecha_registro"), "yyyy-MM-dd"))
        .withColumn("segmento", F.trim(F.col("segmento")))
        .dropDuplicates(["customer_id"])
        .drop(*_DROP_INGEST_COLS)
    )


# ------------------------------- productos -----------------------------------
@dlt.table(
    name=f"{silver_schema}.productos",
    comment="Productos limpios, tipados y deduplicados.",
)
@dlt.expect_or_fail("pk_product_id_not_null", "product_id IS NOT NULL")
@dlt.expect_or_drop("precio_valido", "precio_unitario > 0")
@dlt.expect("stock_no_negativo", "stock_actual >= 0")
def productos():
    df = dlt.read_stream(f"{bronze_schema}.productos_raw")
    return (
        df.withColumn("nombre_producto", F.trim(F.col("nombre_producto")))
        .withColumn("categoria", F.trim(F.col("categoria")))
        .withColumn("subcategoria", F.trim(F.col("subcategoria")))
        .withColumn("precio_unitario", F.col("precio_unitario").cast("decimal(10,2)"))
        .withColumn("proveedor", F.trim(F.col("proveedor")))
        .withColumn("stock_actual", F.col("stock_actual").cast("int"))
        .dropDuplicates(["product_id"])
        .drop(*_DROP_INGEST_COLS)
    )


# -------------------------------- pedidos -------------------------------------
@dlt.table(
    name=f"{silver_schema}.pedidos",
    comment="Pedidos (cabecera) limpios, tipados y deduplicados.",
)
@dlt.expect_or_fail("pk_order_id_not_null", "order_id IS NOT NULL")
@dlt.expect(
    "estado_pedido_valido",
    "estado_pedido IN ('completado', 'en_proceso', 'cancelado')",
)
@dlt.expect_or_drop("total_pedido_no_negativo", "total_pedido >= 0")
def pedidos():
    df = dlt.read_stream(f"{bronze_schema}.pedidos_raw")
    return (
        df.withColumn("fecha_pedido", F.to_date(F.col("fecha_pedido"), "yyyy-MM-dd"))
        .withColumn("canal_venta", F.trim(F.col("canal_venta")))
        .withColumn("estado_pedido", F.trim(F.col("estado_pedido")))
        .withColumn("total_pedido", F.col("total_pedido").cast("decimal(10,2)"))
        .dropDuplicates(["order_id"])
        .drop(*_DROP_INGEST_COLS)
    )


# ---------------------------- detalle_pedidos ---------------------------------
@dlt.table(
    name=f"{silver_schema}.detalle_pedidos",
    comment="Detalle de pedidos limpio, tipado y deduplicado (grano de fact_ventas).",
)
@dlt.expect_or_fail("pk_order_item_id_not_null", "order_item_id IS NOT NULL")
@dlt.expect_or_drop("cantidad_valida", "cantidad > 0")
@dlt.expect_or_fail(
    "fk_order_product_not_null", "order_id IS NOT NULL AND product_id IS NOT NULL"
)
def detalle_pedidos():
    df = dlt.read_stream(f"{bronze_schema}.detalle_pedidos_raw")
    return (
        df.withColumn("cantidad", F.col("cantidad").cast("int"))
        .withColumn("precio_unitario", F.col("precio_unitario").cast("decimal(10,2)"))
        .withColumn("descuento", F.col("descuento").cast("decimal(5,2)"))
        .dropDuplicates(["order_item_id"])
        .drop(*_DROP_INGEST_COLS)
    )
