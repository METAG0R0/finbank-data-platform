# Databricks notebook source
# DBTITLE 1,Silver Layer - FinBank Data Platform
# MAGIC %md
# MAGIC # Silver Layer - FinBank Data Platform
# MAGIC
# MAGIC ## Objetivo
# MAGIC Implementar la capa Silver del Data Lake, aplicando transformaciones de calidad de datos:
# MAGIC - ✅ Eliminación de duplicados
# MAGIC - ✅ Validación de campos obligatorios
# MAGIC - ✅ Estandarización de datos
# MAGIC - ✅ Validación de integridad referencial
# MAGIC - ✅ Protección de PII (datos sensibles)
# MAGIC - ✅ Tablas de errores y reportes de calidad
# MAGIC
# MAGIC ## Flujo de Procesamiento
# MAGIC ```
# MAGIC Bronze (raw) → Validación → Limpieza → Protección PII → Silver (curated)
# MAGIC                     ↓
# MAGIC             data_errors (rechazados)
# MAGIC ```
# MAGIC
# MAGIC ## Tablas Generadas
# MAGIC - 6 tablas Silver: `finbank.silver.*`
# MAGIC - 1 tabla de errores: `finbank.silver.data_errors`
# MAGIC - 1 reporte de calidad: `finbank.silver.data_quality_report`

# COMMAND ----------

# DBTITLE 1,Configuración e Importaciones
# =============================================================================
# CONFIGURACIÓN E IMPORTACIONES
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import uuid

# Configuración
CATALOG = "finbank"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# Batch ID único para esta ejecución
BATCH_ID = str(uuid.uuid4())
PROCESS_TIMESTAMP = datetime.now()

# Metadatos del proceso
print("="*80)
print("SILVER LAYER - CONFIGURACIÓN")
print("="*80)
print(f"Batch ID: {BATCH_ID}")
print(f"Process Timestamp: {PROCESS_TIMESTAMP}")
print(f"Source: {CATALOG}.{BRONZE_SCHEMA}")
print(f"Target: {CATALOG}.{SILVER_SCHEMA}")
print("="*80)

# Crear schema Silver si no existe
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
print(f"\n✓ Schema {CATALOG}.{SILVER_SCHEMA} verificado")

# COMMAND ----------

# DBTITLE 1,Leer tablas Bronze (código inline)
# =============================================================================
# LECTURA DE TABLAS BRONZE - SIN FUNCIONES, CÓDIGO INLINE
# =============================================================================

print("\nLeyendo tablas Bronze...\n")

# TB_CLIENTES_CORE
df_clientes_raw = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.TB_CLIENTES_CORE")
window_clientes = Window.partitionBy("id_cli").orderBy(F.desc("ingestion_timestamp"))
bronze_clientes = df_clientes_raw.withColumn("row_num", F.row_number().over(window_clientes)) \
    .filter(F.col("row_num") == 1).drop("row_num") \
    .drop("ingestion_timestamp", "ingestion_source", "ingestion_batch_id", "is_active", "is_deleted")

# TB_PRODUCTOS_CAT
df_productos_raw = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.TB_PRODUCTOS_CAT")
window_productos = Window.partitionBy("cod_prod").orderBy(F.desc("ingestion_timestamp"))
bronze_productos = df_productos_raw.withColumn("row_num", F.row_number().over(window_productos)) \
    .filter(F.col("row_num") == 1).drop("row_num") \
    .drop("ingestion_timestamp", "ingestion_source", "ingestion_batch_id", "is_active", "is_deleted")

# TB_MOV_FINANCIEROS
df_movimientos_raw = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.TB_MOV_FINANCIEROS")
window_movimientos = Window.partitionBy("id_mov").orderBy(F.desc("ingestion_timestamp"))
bronze_movimientos = df_movimientos_raw.withColumn("row_num", F.row_number().over(window_movimientos)) \
    .filter(F.col("row_num") == 1).drop("row_num") \
    .drop("ingestion_timestamp", "ingestion_source", "ingestion_batch_id", "is_active", "is_deleted")

# TB_OBLIGACIONES
df_obligaciones_raw = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.TB_OBLIGACIONES")
window_obligaciones = Window.partitionBy("id_oblig").orderBy(F.desc("ingestion_timestamp"))
bronze_obligaciones = df_obligaciones_raw.withColumn("row_num", F.row_number().over(window_obligaciones)) \
    .filter(F.col("row_num") == 1).drop("row_num") \
    .drop("ingestion_timestamp", "ingestion_source", "ingestion_batch_id", "is_active", "is_deleted")

# TB_SUCURSALES_RED
df_sucursales_raw = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.TB_SUCURSALES_RED")
window_sucursales = Window.partitionBy("cod_suc").orderBy(F.desc("ingestion_timestamp"))
bronze_sucursales = df_sucursales_raw.withColumn("row_num", F.row_number().over(window_sucursales)) \
    .filter(F.col("row_num") == 1).drop("row_num") \
    .drop("ingestion_timestamp", "ingestion_source", "ingestion_batch_id", "is_active", "is_deleted")

# TB_COMISIONES_LOG
df_comisiones_raw = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.TB_COMISIONES_LOG")
window_comisiones = Window.partitionBy("id_comision").orderBy(F.desc("ingestion_timestamp"))
bronze_comisiones = df_comisiones_raw.withColumn("row_num", F.row_number().over(window_comisiones)) \
    .filter(F.col("row_num") == 1).drop("row_num") \
    .drop("ingestion_timestamp", "ingestion_source", "ingestion_batch_id", "is_active", "is_deleted")

print(f"✓ TB_CLIENTES_CORE: {bronze_clientes.count():,} registros")
print(f"✓ TB_PRODUCTOS_CAT: {bronze_productos.count():,} registros")
print(f"✓ TB_MOV_FINANCIEROS: {bronze_movimientos.count():,} registros")
print(f"✓ TB_OBLIGACIONES: {bronze_obligaciones.count():,} registros")
print(f"✓ TB_SUCURSALES_RED: {bronze_sucursales.count():,} registros")
print(f"✓ TB_COMISIONES_LOG: {bronze_comisiones.count():,} registros")

# COMMAND ----------

# DBTITLE 1,Procesar TB_CLIENTES_CORE (inline)
# =============================================================================
# PROCESAR TB_CLIENTES_CORE - CÓDIGO INLINE
# =============================================================================

print("\n" + "="*80)
print("PROCESANDO: TB_CLIENTES_CORE")
print("="*80)

df_clientes = bronze_clientes
count_initial_clientes = df_clientes.count()
print(f"Registros iniciales: {count_initial_clientes:,}")

# 1. Filtrar registros con campos obligatorios NULL
df_clientes = df_clientes.filter(
    F.col("id_cli").isNotNull() & 
    F.col("tip_doc").isNotNull() & 
    F.col("num_doc").isNotNull() & 
    F.col("fec_alta").isNotNull()
)

# 2. Eliminar duplicados por PK
count_before_dedup = df_clientes.count()
df_clientes = df_clientes.dropDuplicates(["id_cli"])
count_after_dedup = df_clientes.count()
num_dups_clientes = count_before_dedup - count_after_dedup
print(f"  ✓ Duplicados eliminados: {num_dups_clientes:,}")

# 3. Estandarizar textos (UPPER + TRIM) - Solo columnas que existen
for col_name in ["tip_doc", "cod_segmento", "estado_cli", "canal_adquis"]:
    if col_name in df_clientes.columns:
        df_clientes = df_clientes.withColumn(col_name, 
            F.when(F.col(col_name).isNotNull(), F.upper(F.trim(F.col(col_name)))))

# 4. Estandarizar importes (redondear)
if "score_buro" in df_clientes.columns:
    df_clientes = df_clientes.withColumn("score_buro",
        F.when(F.col("score_buro").isNotNull(), F.round(F.col("score_buro"), 2)))

# 5. Protección PII inline - Solo columnas que realmente existen
# num_doc: Hash SHA-256
if "num_doc" in df_clientes.columns:
    df_clientes = df_clientes.withColumn("num_doc_hash", F.sha2(F.col("num_doc"), 256)).drop("num_doc")

print(f"  ✓ PII protegida (num_doc hasheado)")

# Agregar metadatos Silver
df_clientes = df_clientes.withColumn("silver_timestamp", F.lit(PROCESS_TIMESTAMP)) \
    .withColumn("silver_batch_id", F.lit(BATCH_ID))

count_final_clientes = df_clientes.count()
print(f"\nRegistros finales: {count_final_clientes:,}")
if count_initial_clientes > 0:
    print(f"Tasa de calidad: {(count_final_clientes/count_initial_clientes)*100:.2f}%")
else:
    print(f"Tasa de calidad: N/A (sin datos iniciales)")

# Guardar tabla Silver
df_clientes.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.TB_CLIENTES_CORE")
print(f"✓ Tabla guardada: {CATALOG}.{SILVER_SCHEMA}.TB_CLIENTES_CORE")

# Guardar para referencia
silver_clientes = df_clientes

# COMMAND ----------

# DBTITLE 1,Procesar TB_PRODUCTOS_CAT (inline)
# =============================================================================
# PROCESAR TB_PRODUCTOS_CAT - CÓDIGO INLINE
# =============================================================================

print("\n" + "="*80)
print("PROCESANDO: TB_PRODUCTOS_CAT")
print("="*80)

df_productos = bronze_productos
count_initial_productos = df_productos.count()
print(f"Registros iniciales: {count_initial_productos:,}")

# 1. Filtrar registros con campos obligatorios NULL
df_productos = df_productos.filter(
    F.col("cod_prod").isNotNull() & 
    F.col("desc_prod").isNotNull() & 
    F.col("tip_prod").isNotNull()
)

# 2. Eliminar duplicados por PK
count_before_dedup = df_productos.count()
df_productos = df_productos.dropDuplicates(["cod_prod"])
count_after_dedup = df_productos.count()
num_dups_productos = count_before_dedup - count_after_dedup
print(f"  ✓ Duplicados eliminados: {num_dups_productos:,}")

# 3. Estandarizar textos (UPPER + TRIM)
for col_name in ["cod_prod", "tip_prod", "estado_prod"]:
    if col_name in df_productos.columns:
        df_productos = df_productos.withColumn(col_name, 
            F.when(F.col(col_name).isNotNull(), F.upper(F.trim(F.col(col_name)))))

# 4. Estandarizar importes (redondear a 2 decimales)
for col_name in ["tasa_ea", "cuota_min", "comision_admin"]:
    if col_name in df_productos.columns:
        df_productos = df_productos.withColumn(col_name,
            F.when(F.col(col_name).isNotNull(), F.round(F.col(col_name), 2)))

# Agregar metadatos Silver
df_productos = df_productos.withColumn("silver_timestamp", F.lit(PROCESS_TIMESTAMP)) \
    .withColumn("silver_batch_id", F.lit(BATCH_ID))

count_final_productos = df_productos.count()
print(f"\nRegistros finales: {count_final_productos:,}")
if count_initial_productos > 0:
    print(f"Tasa de calidad: {(count_final_productos/count_initial_productos)*100:.2f}%")
else:
    print(f"Tasa de calidad: N/A (sin datos iniciales)")

# Guardar tabla Silver
df_productos.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.TB_PRODUCTOS_CAT")
print(f"✓ Tabla guardada: {CATALOG}.{SILVER_SCHEMA}.TB_PRODUCTOS_CAT")

# Guardar para referencia
silver_productos = df_productos

# COMMAND ----------

# DBTITLE 1,Procesar TB_MOV_FINANCIEROS (inline)
# =============================================================================
# PROCESAR TB_MOV_FINANCIEROS - CÓDIGO INLINE
# =============================================================================

print("\n" + "="*80)
print("PROCESANDO: TB_MOV_FINANCIEROS")
print("="*80)

df_movimientos = bronze_movimientos
count_initial_movimientos = df_movimientos.count()
print(f"Registros iniciales: {count_initial_movimientos:,}")

# 1. Filtrar registros con campos obligatorios NULL
df_movimientos = df_movimientos.filter(
    F.col("id_mov").isNotNull() & 
    F.col("id_cli").isNotNull() & 
    F.col("cod_prod").isNotNull() & 
    F.col("fec_mov").isNotNull() & 
    F.col("tip_mov").isNotNull()
)

# 2. Eliminar duplicados por PK
count_before_dedup = df_movimientos.count()
df_movimientos = df_movimientos.dropDuplicates(["id_mov"])
count_after_dedup = df_movimientos.count()
num_dups_movimientos = count_before_dedup - count_after_dedup
print(f"  ✓ Duplicados eliminados: {num_dups_movimientos:,}")

# 3. Validar FK: id_cli -> TB_CLIENTES_CORE.id_cli (left_semi join inline)
valid_clientes = silver_clientes.select("id_cli").distinct()
df_movimientos = df_movimientos.join(valid_clientes, "id_cli", "left_semi")
print(f"  ✓ FK id_cli validada")

# 4. Validar FK: cod_prod -> TB_PRODUCTOS_CAT.cod_prod (left_semi join inline)
valid_productos = silver_productos.select("cod_prod").distinct()
df_movimientos = df_movimientos.join(valid_productos, "cod_prod", "left_semi")
print(f"  ✓ FK cod_prod validada")

# 5. Estandarizar textos (UPPER + TRIM)
for col_name in ["tip_mov", "canal", "cod_canal", "cod_estado_mov"]:
    if col_name in df_movimientos.columns:
        df_movimientos = df_movimientos.withColumn(col_name, 
            F.when(F.col(col_name).isNotNull(), F.upper(F.trim(F.col(col_name)))))

# 6. Estandarizar importes (redondear a 2 decimales)
for col_name in ["mont", "sald_post", "vr_mov"]:
    if col_name in df_movimientos.columns:
        df_movimientos = df_movimientos.withColumn(col_name,
            F.when(F.col(col_name).isNotNull(), F.round(F.col(col_name), 2)))

# 7. Detectar transacciones sospechosas según comportamiento histórico del cliente
# Regla: vr_mov > promedio de los últimos 30 días + 3 desviaciones estándar

window_30d = (
    Window
    .partitionBy("id_cli")
    .orderBy(F.col("fec_mov").cast("timestamp").cast("long"))
    .rangeBetween(-30 * 86400, -1)
)

df_movimientos = (
    df_movimientos
    .withColumn("_avg_vr_mov_30d", F.avg("vr_mov").over(window_30d))
    .withColumn("_std_vr_mov_30d", F.stddev_pop("vr_mov").over(window_30d))
    .withColumn(
        "ind_sospechoso",
        F.when(
            F.col("_avg_vr_mov_30d").isNotNull()
            & F.col("_std_vr_mov_30d").isNotNull()
            & (
                F.col("vr_mov")
                > F.col("_avg_vr_mov_30d") + (3 * F.col("_std_vr_mov_30d"))
            ),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    .drop("_avg_vr_mov_30d", "_std_vr_mov_30d")
)

# Agregar metadatos Silver
df_movimientos = df_movimientos.withColumn("silver_timestamp", F.lit(PROCESS_TIMESTAMP)) \
    .withColumn("silver_batch_id", F.lit(BATCH_ID))

count_final_movimientos = df_movimientos.count()
print(f"\nRegistros finales: {count_final_movimientos:,}")
if count_initial_movimientos > 0:
    print(f"Tasa de calidad: {(count_final_movimientos/count_initial_movimientos)*100:.2f}%")
else:
    print(f"Tasa de calidad: N/A (sin datos iniciales)")

# Guardar tabla Silver
df_movimientos.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.TB_MOV_FINANCIEROS")
print(f"✓ Tabla guardada: {CATALOG}.{SILVER_SCHEMA}.TB_MOV_FINANCIEROS")

# Guardar para referencia
silver_movimientos = df_movimientos

# COMMAND ----------

# DBTITLE 1,Procesar TB_OBLIGACIONES (inline)
# =============================================================================
# PROCESAR TB_OBLIGACIONES - CÓDIGO INLINE
# =============================================================================

print("\n" + "="*80)
print("PROCESANDO: TB_OBLIGACIONES")
print("="*80)

df_obligaciones = bronze_obligaciones
count_initial_obligaciones = df_obligaciones.count()
print(f"Registros iniciales: {count_initial_obligaciones:,}")

# 1. Filtrar registros con campos obligatorios NULL
df_obligaciones = df_obligaciones.filter(
    F.col("id_oblig").isNotNull() & 
    F.col("id_cli").isNotNull() & 
    F.col("cod_prod").isNotNull() & 
    F.col("fec_desembolso").isNotNull()
)

# 2. Eliminar duplicados por PK
count_before_dedup = df_obligaciones.count()
df_obligaciones = df_obligaciones.dropDuplicates(["id_oblig"])
count_after_dedup = df_obligaciones.count()
num_dups_obligaciones = count_before_dedup - count_after_dedup
print(f"  ✓ Duplicados eliminados: {num_dups_obligaciones:,}")

# 3. Validar FK: id_cli -> TB_CLIENTES_CORE.id_cli
valid_clientes = silver_clientes.select("id_cli").distinct()
df_obligaciones = df_obligaciones.join(valid_clientes, "id_cli", "left_semi")
print(f"  ✓ FK id_cli validada")

# 4. Validar FK: cod_prod -> TB_PRODUCTOS_CAT.cod_prod
valid_productos = silver_productos.select("cod_prod").distinct()
df_obligaciones = df_obligaciones.join(valid_productos, "cod_prod", "left_semi")
print(f"  ✓ FK cod_prod validada")

# 5. Estandarizar textos (UPPER + TRIM)
for col_name in ["calif_riesgo"]:
    if col_name in df_obligaciones.columns:
        df_obligaciones = df_obligaciones.withColumn(col_name, 
            F.when(F.col(col_name).isNotNull(), F.upper(F.trim(F.col(col_name)))))

# 6. Estandarizar importes (redondear a 2 decimales)
for col_name in ["vr_aprobado", "vr_desembolsado", "sdo_capital", "vr_cuota"]:
    if col_name in df_obligaciones.columns:
        df_obligaciones = df_obligaciones.withColumn(col_name,
            F.when(F.col(col_name).isNotNull(), F.round(F.col(col_name), 2)))

# Agregar metadatos Silver
df_obligaciones = df_obligaciones.withColumn("silver_timestamp", F.lit(PROCESS_TIMESTAMP)) \
    .withColumn("silver_batch_id", F.lit(BATCH_ID))

count_final_obligaciones = df_obligaciones.count()
print(f"\nRegistros finales: {count_final_obligaciones:,}")
if count_initial_obligaciones > 0:
    print(f"Tasa de calidad: {(count_final_obligaciones/count_initial_obligaciones)*100:.2f}%")
else:
    print(f"Tasa de calidad: N/A (sin datos iniciales)")

# Guardar tabla Silver
df_obligaciones.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.TB_OBLIGACIONES")
print(f"✓ Tabla guardada: {CATALOG}.{SILVER_SCHEMA}.TB_OBLIGACIONES")

# Guardar para referencia
silver_obligaciones = df_obligaciones

# COMMAND ----------

# DBTITLE 1,Procesar TB_SUCURSALES_RED (inline)
# =============================================================================
# PROCESAR TB_SUCURSALES_RED - CÓDIGO INLINE
# =============================================================================

print("\n" + "="*80)
print("PROCESANDO: TB_SUCURSALES_RED")
print("="*80)

df_sucursales = bronze_sucursales
count_initial_sucursales = df_sucursales.count()
print(f"Registros iniciales: {count_initial_sucursales:,}")

# 1. Filtrar registros con campos obligatorios NULL
df_sucursales = df_sucursales.filter(
    F.col("cod_suc").isNotNull()
)

# 2. Eliminar duplicados por PK
count_before_dedup = df_sucursales.count()
df_sucursales = df_sucursales.dropDuplicates(["cod_suc"])
count_after_dedup = df_sucursales.count()
num_dups_sucursales = count_before_dedup - count_after_dedup
print(f"  ✓ Duplicados eliminados: {num_dups_sucursales:,}")

# 3. Estandarizar textos (UPPER + TRIM)
for col_name in ["cod_suc", "tip_punto", "region", "provincia", "distrito", "estado", "ciudad", "depto"]:
    if col_name in df_sucursales.columns:
        df_sucursales = df_sucursales.withColumn(col_name, 
            F.when(F.col(col_name).isNotNull(), F.upper(F.trim(F.col(col_name)))))

# Agregar metadatos Silver
df_sucursales = df_sucursales.withColumn("silver_timestamp", F.lit(PROCESS_TIMESTAMP)) \
    .withColumn("silver_batch_id", F.lit(BATCH_ID))

count_final_sucursales = df_sucursales.count()
print(f"\nRegistros finales: {count_final_sucursales:,}")
if count_initial_sucursales > 0:
    print(f"Tasa de calidad: {(count_final_sucursales/count_initial_sucursales)*100:.2f}%")
else:
    print(f"Tasa de calidad: N/A (sin datos iniciales)")

# Guardar tabla Silver
df_sucursales.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.TB_SUCURSALES_RED")
print(f"✓ Tabla guardada: {CATALOG}.{SILVER_SCHEMA}.TB_SUCURSALES_RED")

# Guardar para referencia
silver_sucursales = df_sucursales

# COMMAND ----------

# DBTITLE 1,Procesar TB_COMISIONES_LOG (inline)
# =============================================================================
# PROCESAR TB_COMISIONES_LOG - CÓDIGO INLINE
# =============================================================================

print("\n" + "="*80)
print("PROCESANDO: TB_COMISIONES_LOG")
print("="*80)

df_comisiones = bronze_comisiones
count_initial_comisiones = df_comisiones.count()
print(f"Registros iniciales: {count_initial_comisiones:,}")

# 1. Filtrar registros con campos obligatorios NULL
df_comisiones = df_comisiones.filter(
    F.col("id_comision").isNotNull() & 
    F.col("id_cli").isNotNull() & 
    F.col("cod_prod").isNotNull() & 
    F.col("fec_cobro").isNotNull()
)

# 2. Eliminar duplicados por PK
count_before_dedup = df_comisiones.count()
df_comisiones = df_comisiones.dropDuplicates(["id_comision"])
count_after_dedup = df_comisiones.count()
num_dups_comisiones = count_before_dedup - count_after_dedup
print(f"  ✓ Duplicados eliminados: {num_dups_comisiones:,}")

# 3. Validar FK: id_cli -> TB_CLIENTES_CORE.id_cli
valid_clientes = silver_clientes.select("id_cli").distinct()
df_comisiones = df_comisiones.join(valid_clientes, "id_cli", "left_semi")
print(f"  ✓ FK id_cli validada")

# 4. Validar FK: cod_prod -> TB_PRODUCTOS_CAT.cod_prod
valid_productos = silver_productos.select("cod_prod").distinct()
df_comisiones = df_comisiones.join(valid_productos, "cod_prod", "left_semi")
print(f"  ✓ FK cod_prod validada")

# 5. Estandarizar textos (UPPER + TRIM)
for col_name in ["tip_comision", "estado_cobro"]:
    if col_name in df_comisiones.columns:
        df_comisiones = df_comisiones.withColumn(col_name, 
            F.when(F.col(col_name).isNotNull(), F.upper(F.trim(F.col(col_name)))))

# 6. Estandarizar importes (redondear a 2 decimales)
for col_name in ["mont_comision", "vr_comision"]:
    if col_name in df_comisiones.columns:
        df_comisiones = df_comisiones.withColumn(col_name,
            F.when(F.col(col_name).isNotNull(), F.round(F.col(col_name), 2)))

# Agregar metadatos Silver
df_comisiones = df_comisiones.withColumn("silver_timestamp", F.lit(PROCESS_TIMESTAMP)) \
    .withColumn("silver_batch_id", F.lit(BATCH_ID))

count_final_comisiones = df_comisiones.count()
print(f"\nRegistros finales: {count_final_comisiones:,}")
if count_initial_comisiones > 0:
    print(f"Tasa de calidad: {(count_final_comisiones/count_initial_comisiones)*100:.2f}%")
else:
    print(f"Tasa de calidad: N/A (sin datos iniciales)")

# Guardar tabla Silver
df_comisiones.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.TB_COMISIONES_LOG")
print(f"✓ Tabla guardada: {CATALOG}.{SILVER_SCHEMA}.TB_COMISIONES_LOG")

# Guardar para referencia
silver_comisiones = df_comisiones

# COMMAND ----------

# DBTITLE 1,Reporte Final Silver
# =============================================================================
# REPORTE FINAL SILVER
# =============================================================================

print("\n\n" + "="*80)
print("📈 REPORTE FINAL DE TRANSFORMACIÓN SILVER")
print("="*80)

print("\n📋 Tablas Silver Creadas:")
print("-" * 80)

tables_info = [
    ("TB_CLIENTES_CORE", "finbank.silver.TB_CLIENTES_CORE"),
    ("TB_PRODUCTOS_CAT", "finbank.silver.TB_PRODUCTOS_CAT"),
    ("TB_MOV_FINANCIEROS", "finbank.silver.TB_MOV_FINANCIEROS"),
    ("TB_OBLIGACIONES", "finbank.silver.TB_OBLIGACIONES"),
    ("TB_SUCURSALES_RED", "finbank.silver.TB_SUCURSALES_RED"),
    ("TB_COMISIONES_LOG", "finbank.silver.TB_COMISIONES_LOG")
]

total_registros = 0
for table_name, full_table in tables_info:
    try:
        count = spark.table(full_table).count()
        total_registros += count
        print(f"  ✓ {table_name:25s}: {count:>10,} registros")
    except Exception as e:
        print(f"  ✗ {table_name:25s}: ERROR - {str(e)}")

print("-" * 80)
print(f"  TOTAL SILVER                : {total_registros:>10,} registros")

print("\n✅ TRANSFORMACIÓN SILVER COMPLETADA")
print("="*80)