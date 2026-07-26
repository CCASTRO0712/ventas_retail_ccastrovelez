# ventas_retail_ccastrovelez

Pipeline end-to-end de un dominio de **ventas retail**, construido 100% con
**Spark Declarative Pipelines** (Lakeflow Declarative Pipelines, ex Delta Live
Tables), que ingiere 4 entidades relacionadas, las procesa a través de las
capas **Bronze → Silver → Gold**, modela la capa Gold como **esquema
estrella** y expone los resultados en un **dashboard de Databricks**. Todo
orquestado con un **Job** y empaquetado como **Databricks Asset Bundle (DAB)**.

## 1. Arquitectura

```
Volume (landing)          Bronze                Silver                 Gold
------------------        -------------------    -------------------    ---------------------------
clientes/*.csv     --STREAM--> clientes_raw --STREAM--> clientes    --\
productos/*.csv    --STREAM--> productos_raw --STREAM--> productos  --+--> dim_cliente   (MV)
pedidos/*.json     --STREAM--> pedidos_raw   --STREAM--> pedidos     --+--> dim_producto  (MV)
detalle_pedidos/*  --STREAM--> detalle_pedidos_raw --STREAM--> detalle_pedidos --+--> dim_fecha (MV)
                                                                       \--> fact_ventas   (MV)
```

| Capa   | Tipo de tabla            |
|--------|--------------------------|
| Bronze | `STREAMING TABLE` (STREAM) |
| Silver | `STREAMING TABLE` (STREAM) |
| Gold   | `MATERIALIZED VIEW`         |


## 2. Estructura del repositorio

```
ventas_retail_ccastrovelez/
├── databricks.yml                     # Bundle raíz + variables del proyecto
├── resources/
│   ├── pipeline.yml                   # Definición del Declarative Pipeline
│   └── job.yml                        # Job: setup -> run_pipeline
├── src/
│   └── transformations/
│       ├── 01_bronze.py               # STREAM (Auto Loader) -> STREAMING TABLE
│       ├── 02_silver.py               # limpieza + dedup + expectations
│       └── 03_gold.py                 # esquema estrella (MATERIALIZED VIEW)
├── setup/
│   └── 00_setup.py                    # Notebook: crea catálogo/esquemas/Volume
├── scripts/
│   └── upload_data.sh                 # Sube los 12 batches al Volume (Databricks CLI)
├── data/                              # Los 12 archivos de origen
│   ├── clientes/{*_batch_1,2,3}.csv
│   ├── productos/{*_batch_1,2,3}.csv
│   ├── pedidos/{*_batch_1,2,3}.json
│   └── detalle_pedidos/{*_batch_1,2,3}.json
├── dashboard/
│   └── dashboard_gold.lvdash.json     # Dashboard Databricks sobre Gold
└── .gitignore
```

## 3. Entidades y diccionarios de datos

### 3.1 `clientes` (CSV) — entidad maestra

| Campo | Tipo | Descripción |
|---|---|---|
| customer_id | Integer | Identificador único del cliente (PK) |
| nombre | String | Nombre del cliente |
| apellido | String | Apellido del cliente |
| email | String | Correo electrónico de contacto |
| ciudad | String | Ciudad de residencia |
| pais | String | País de residencia |
| fecha_registro | Date | Fecha de alta del cliente (yyyy-MM-dd) |
| segmento | String | Segmento comercial: `Retail` o `Premium` |

### 3.2 `productos` (CSV) — catálogo de productos

| Campo | Tipo | Descripción |
|---|---|---|
| product_id | Integer | Identificador único del producto (PK) |
| nombre_producto | String | Nombre comercial del producto |
| categoria | String | Categoría del producto |
| subcategoria | String | Subcategoría del producto |
| precio_unitario | Decimal | Precio unitario de lista |
| proveedor | String | Proveedor del producto |
| stock_actual | Integer | Unidades disponibles en inventario |

### 3.3 `pedidos` (JSON) — cabecera de pedido

| Campo | Tipo | Descripción |
|---|---|---|
| order_id | Integer | Identificador único del pedido (PK) |
| customer_id | Integer | FK hacia `clientes.customer_id` |
| fecha_pedido | Date | Fecha en la que se realizó el pedido |
| canal_venta | String | Canal por el que se generó el pedido |
| estado_pedido | String | `completado`, `en_proceso`, `cancelado` |
| total_pedido | Decimal | Monto total del pedido |

### 3.4 `detalle_pedidos` (JSON) — grano de la tabla de hechos

| Campo | Tipo | Descripción |
|---|---|---|
| order_item_id | Integer | Identificador único de la línea (PK) |
| order_id | Integer | FK hacia `pedidos.order_id` |
| product_id | Integer | FK hacia `productos.product_id` |
| cantidad | Integer | Unidades compradas |
| precio_unitario | Decimal | Precio unitario aplicado en la venta |
| descuento | Decimal | % de descuento aplicado (0 a 1) |

## 4. Catálogo, esquemas y tablas

Un único catálogo (`proyecto_final` por defecto) con un esquema por capa.
Todo es configurable vía variables del bundle (`databricks.yml`).

| Concepto | Convención | Ejemplo (defaults) |
|---|---|---|
| Catálogo | `<nombre_proyecto>` (variable `catalog`) | `proyecto_final` |
| Esquema Landing (Volume) | `landing` | `proyecto_final.landing` |
| Esquema Bronze | `bronze` | `proyecto_final.bronze` |
| Esquema Silver | `silver` | `proyecto_final.silver` |
| Esquema Gold | `gold` | `proyecto_final.gold` |

**Bronze:** `clientes_raw`, `productos_raw`, `pedidos_raw`, `detalle_pedidos_raw`
**Silver:** `clientes`, `productos`, `pedidos`, `detalle_pedidos`
**Gold:** `dim_cliente`, `dim_producto`, `dim_fecha`, `fact_ventas`

Ruta del Volume:
```
/Volumes/<catalogo>/landing/<volumen>/ventas_retail_ccastrovelez/{entidad}/
```

## 5. Expectations (calidad de datos)

Se usan las 3 severidades soportadas por Spark Declarative Pipelines, con al
menos una regla de cada una en el proyecto:

- `expect_or_fail` → corta el pipeline (reglas de PK/FK críticas).
- `expect_or_drop` → descarta la fila inválida silenciosamente.
- `expect` → solo advierte (`warn`), no descarta ni corta.

**Silver** (validez estructural/formato) — ver `src/transformations/02_silver.py`:

| Entidad | Regla | Severidad |
|---|---|---|
| clientes | `customer_id IS NOT NULL` | fail |
| clientes | formato de `email` válido | drop |
| clientes | `segmento IN ('Retail','Premium')` | warn |
| productos | `product_id IS NOT NULL` | fail |
| productos | `precio_unitario > 0` | drop |
| productos | `stock_actual >= 0` | warn |
| pedidos | `order_id IS NOT NULL` | fail |
| pedidos | `estado_pedido IN (...)` | warn |
| pedidos | `total_pedido >= 0` | drop |
| detalle_pedidos | `order_item_id IS NOT NULL` | fail |
| detalle_pedidos | `cantidad > 0` | drop |
| detalle_pedidos | `order_id`/`product_id` no nulos | fail |

**Gold** (integridad del modelo dimensional) — ver `src/transformations/03_gold.py`:

| Tabla | Regla | Severidad |
|---|---|---|
| dim_cliente / dim_producto / dim_fecha | llave surrogate no nula | fail |
| fact_ventas | `customer_key`/`product_key`/`date_key` no nulos | fail |
| fact_ventas | `cantidad > 0` | drop |
| fact_ventas | `monto_total >= 0` | drop |
| fact_ventas | `precio_unitario >= 0` | warn |

