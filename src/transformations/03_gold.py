# -----------------------------------------------------------------------------
# CAPA GOLD
# Modelo dimensional en estrella construido como MATERIALIZED VIEW:
#   dim_cliente, dim_producto, dim_fecha  <-  fact_ventas
# -----------------------------------------------------------------------------

import dlt
import pyspark.sql.functions as F

silver_schema = spark.conf.get("silver.schema")
gold_schema = spark.conf.get("gold.schema")


# ------------------------------- dim_cliente ----------------------------------
@dlt.table(
    name=f"{gold_schema}.dim_cliente",
    comment="Dimensión de clientes (1 fila por cliente).",
)
@dlt.expect_or_fail("customer_key_not_null", "customer_key IS NOT NULL")
def dim_cliente():
    df = dlt.read(f"{silver_schema}.clientes")
    return df.select(
        F.col("customer_id").alias("customer_key"),
        "customer_id",
        "nombre",
        "apellido",
        "email",
        "ciudad",
        "pais",
        "fecha_registro",
        "segmento",
    )


# ------------------------------- dim_producto ---------------------------------
@dlt.table(
    name=f"{gold_schema}.dim_producto",
    comment="Dimensión de productos (1 fila por producto).",
)
@dlt.expect_or_fail("product_key_not_null", "product_key IS NOT NULL")
def dim_producto():
    df = dlt.read(f"{silver_schema}.productos")
    return df.select(
        F.col("product_id").alias("product_key"),
        "product_id",
        "nombre_producto",
        "categoria",
        "subcategoria",
        "precio_unitario",
        "proveedor",
        "stock_actual",
    )


# -------------------------------- dim_fecha ------------------------------------
@dlt.table(
    name=f"{gold_schema}.dim_fecha",
    comment="Dimensión de calendario (1 fila por día con pedidos).",
)
@dlt.expect_or_fail("date_key_not_null", "date_key IS NOT NULL")
def dim_fecha():
    fechas = (
        dlt.read(f"{silver_schema}.pedidos")
        .select(F.col("fecha_pedido").alias("fecha"))
        .distinct()
    )
    return (
        fechas.withColumn("date_key", F.date_format("fecha", "yyyyMMdd").cast("int"))
        .withColumn("anio", F.year("fecha"))
        .withColumn("mes", F.month("fecha"))
        .withColumn("dia", F.dayofmonth("fecha"))
        .withColumn("trimestre", F.quarter("fecha"))
        .withColumn("nombre_mes", F.date_format("fecha", "MMMM"))
        .withColumn("dia_semana", F.date_format("fecha", "EEEE"))
        .withColumn("es_fin_de_semana", F.dayofweek("fecha").isin(1, 7))
    )


# -------------------------------- fact_ventas -----------------------------------
@dlt.table(
    name=f"{gold_schema}.fact_ventas",
    comment="Tabla de hechos: 1 fila por línea de detalle de pedido.",
)
@dlt.expect_or_fail("fk_customer_key_not_null", "customer_key IS NOT NULL")
@dlt.expect_or_fail("fk_product_key_not_null", "product_key IS NOT NULL")
@dlt.expect_or_fail("fk_date_key_not_null", "date_key IS NOT NULL")
@dlt.expect_or_drop("cantidad_no_negativa", "cantidad > 0")
@dlt.expect_or_drop("monto_total_no_negativo", "monto_total >= 0")
@dlt.expect("precio_unitario_no_negativo", "precio_unitario >= 0")
def fact_ventas():
    detalle = dlt.read(f"{silver_schema}.detalle_pedidos").alias("d")
    pedidos = dlt.read(f"{silver_schema}.pedidos").alias("p")
    return (
        detalle.join(pedidos, F.col("d.order_id") == F.col("p.order_id"), "inner")
        .select(
            F.col("d.order_item_id").alias("order_item_id"),
            F.col("d.order_id").alias("order_id"),
            F.col("p.customer_id").alias("customer_key"),
            F.col("d.product_id").alias("product_key"),
            F.date_format(F.col("p.fecha_pedido"), "yyyyMMdd").cast("int").alias("date_key"),
            F.col("d.cantidad").alias("cantidad"),
            F.col("d.precio_unitario").alias("precio_unitario"),
            F.col("d.descuento").alias("descuento"),
            (
                F.col("d.cantidad")
                * F.col("d.precio_unitario")
                * (F.lit(1) - F.col("d.descuento"))
            ).alias("monto_total"),
            F.col("p.canal_venta").alias("canal_venta"),
            F.col("p.estado_pedido").alias("estado_pedido"),
        )
    )
