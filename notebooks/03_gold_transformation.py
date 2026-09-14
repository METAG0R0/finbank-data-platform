# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold Layer - FinBank Data Platform
# MAGIC %md
# MAGIC # Gold Layer - FinBank Data Platform
# MAGIC
# MAGIC ## Objetivo
# MAGIC Construir la capa Gold (consumption) del Data Lake con modelos dimensionales y agregaciones para consumo analítico.
# MAGIC
# MAGIC ## Alcance
# MAGIC - **Dimensiones**: dim_clientes, dim_productos, dim_geografia, dim_canal
# MAGIC - **Hechos**: fact_transacciones, fact_cartera, fact_rentabilidad_cliente
# MAGIC - **KPIs**: Cartera diaria por producto/segmento/ciudad
# MAGIC - **Agregaciones**: 3 tablas agregadas para análisis específicos
# MAGIC - **KPI Ejecutivo**: Vista consolidada para BI
# MAGIC
# MAGIC ## Arquitectura
# MAGIC ```
# MAGIC finbank.silver.* → Transformación → finbank.gold.*
# MAGIC ```
# MAGIC
# MAGIC ## Notas Técnicas
# MAGIC - Todas las transformaciones se basan en los datos reales disponibles en Silver
# MAGIC - No se asumen columnas o relaciones no verificadas
# MAGIC - Las limitaciones de datos se documentan explícitamente

# COMMAND ----------

# DBTITLE 1,Configuración e Importaciones
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import uuid

# Configuración
CATALOG = "finbank"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

BATCH_ID = str(uuid.uuid4())
PROCESS_TIMESTAMP = datetime.now()

print("="*80)
print("GOLD LAYER - CONFIGURACIÓN")
print("="*80)
print(f"Batch ID: {BATCH_ID}")
print(f"Process Timestamp: {PROCESS_TIMESTAMP}")
print(f"Source: {CATALOG}.{SILVER_SCHEMA}")
print(f"Target: {CATALOG}.{GOLD_SCHEMA}")
print("="*80)

# Crear schema Gold
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")
print(f"\n✓ Schema {CATALOG}.{GOLD_SCHEMA} verificado")

# COMMAND ----------

# DBTITLE 1,Inspección de Silver - Datos Disponibles
# MAGIC %md
# MAGIC ## Inspección de Silver - Datos Disponibles
# MAGIC
# MAGIC Antes de construir Gold, se inspeccionaron las tablas Silver reales:
# MAGIC
# MAGIC ### Tablas y Registros
# MAGIC - **TB_CLIENTES_CORE**: 10,000 registros
# MAGIC - **TB_PRODUCTOS_CAT**: 50 registros  
# MAGIC - **TB_MOV_FINANCIEROS**: 495,000 registros
# MAGIC - **TB_OBLIGACIONES**: 29,700 registros
# MAGIC - **TB_SUCURSALES_RED**: 200 registros
# MAGIC - **TB_COMISIONES_LOG**: 80,000 registros
# MAGIC
# MAGIC ### Campos Clave Identificados
# MAGIC - **Segmentos**: MASIVO, PREFERENTE, PREMIUM, PYME
# MAGIC - **Tipos de Producto**: ACTIVO, PASIVO
# MAGIC - **Tipos de Movimiento**: TRANSFERENCIA, PAGO, DEPOSITO, RETIRO
# MAGIC - **Tipos de Punto**: AGENCIA, ATM, CORRESPONSAL
# MAGIC - **Productos**: Cuenta Ahorros, Tarjeta Credito, Credito Consumo, Credito Hipotecario, CDT
# MAGIC
# MAGIC ### Limitaciones Documentadas
# MAGIC 1. **ind_sospechoso**: No existe en Silver. Requiere cálculo sobre ventana de 30 días. No se implementa en Gold según lineamiento de arquitectura.
# MAGIC 2. **Intereses**: No hay columna específica para intereses cobrados en mov_financieros. CLTV se calculará solo con comisiones.
# MAGIC 3. **Tasa de cambio COP-USD**: No disponible. Montos se mantienen en COP.

# COMMAND ----------

# DBTITLE 1,dim_clientes - Dimensión de Clientes
# =============================================================================
# DIM_CLIENTES - Dimensión de Clientes
# =============================================================================

print("\n" + "="*80)
print("CONSTRUYENDO: dim_clientes")
print("="*80)

# Leer Silver
df_clientes = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.tb_clientes_core")

# Transformaciones
dim_clientes = df_clientes.select(
    F.col("id_cli").alias("cliente_id"),
    # Nombre completo
    F.concat_ws(" ", F.col("nomb_cli"), F.col("apell_cli")).alias("nombre_completo"),
    F.col("tip_doc").alias("tipo_documento"),
    F.col("num_doc_hash").alias("documento_hash"),
    F.col("fec_nac").alias("fecha_nacimiento"),
    # Edad calculada
    F.floor(F.months_between(F.current_date(), F.col("fec_nac")) / 12).alias("edad"),
    F.col("fec_alta").alias("fecha_alta"),
    # Segmento con etiqueta
    F.col("cod_segmento").alias("segmento_codigo"),
    F.when(F.col("cod_segmento") == "MASIVO", "Masivo")
     .when(F.col("cod_segmento") == "PREFERENTE", "Preferente")
     .when(F.col("cod_segmento") == "PREMIUM", "Premium")
     .when(F.col("cod_segmento") == "PYME", "PyME")
     .otherwise("No Clasificado").alias("segmento_nombre"),
    F.col("score_buro"),
    F.col("ciudad_res").alias("ciudad_residencia"),
    F.col("depto_res").alias("departamento_residencia"),
    F.col("estado_cli").alias("estado_cliente"),
    F.col("canal_adquis").alias("canal_adquisicion"),
    F.lit(PROCESS_TIMESTAMP).alias("gold_timestamp"),
    F.lit(BATCH_ID).alias("gold_batch_id")
)

# Guardar
dim_clientes.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_clientes")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_clientes").count()
print(f"✓ dim_clientes creada: {count:,} registros")

# COMMAND ----------

# DBTITLE 1,dim_productos - Dimensión de Productos
# =============================================================================
# DIM_PRODUCTOS - Dimensión de Productos
# =============================================================================

print("\n" + "="*80)
print("CONSTRUYENDO: dim_productos")
print("="*80)

df_productos = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.tb_productos_cat")

# Clasificar familia de producto según desc_prod
dim_productos = df_productos.select(
    F.col("cod_prod").alias("producto_id"),
    F.col("desc_prod").alias("producto_nombre"),
    F.col("tip_prod").alias("tipo_producto"),
    # Clasificar familia
    F.when(F.lower(F.col("desc_prod")).contains("tarjeta"), "Crédito")
     .when(F.lower(F.col("desc_prod")).contains("credito"), "Crédito")
     .when(F.lower(F.col("desc_prod")).contains("prestamo"), "Crédito")
     .when(F.lower(F.col("desc_prod")).contains("hipotecario"), "Crédito")
     .when(F.lower(F.col("desc_prod")).contains("consumo"), "Crédito")
     .when(F.lower(F.col("desc_prod")).contains("ahorros"), "Ahorro")
     .when(F.lower(F.col("desc_prod")).contains("cdt"), "Ahorro")
     .when(F.lower(F.col("desc_prod")).contains("corriente"), "Transaccional")
     .otherwise("Otro").alias("familia_producto"),
    F.col("tasa_ea").alias("tasa_efectiva_anual"),
    # Tasa mensual equivalente: (1 + TEA)^(1/12) - 1
    F.when(F.col("tasa_ea").isNotNull(),
        (F.pow(1 + F.col("tasa_ea") / 100, 1.0 / 12) - 1) * 100
    ).alias("tasa_mensual_equivalente"),
    F.col("plazo_max_meses").alias("plazo_maximo_meses"),
    F.col("cuota_min").alias("cuota_minima"),
    F.col("comision_admin").alias("comision_administracion"),
    F.col("estado_prod").alias("estado_producto"),
    F.lit(PROCESS_TIMESTAMP).alias("gold_timestamp"),
    F.lit(BATCH_ID).alias("gold_batch_id")
)

dim_productos.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_productos")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_productos").count()
print(f"✓ dim_productos creada: {count:,} registros")

# COMMAND ----------

# DBTITLE 1,dim_geografia - Dimensión Geográfica
# =============================================================================
# DIM_GEOGRAFIA - Dimensión Geográfica
# =============================================================================

print("\n" + "="*80)
print("CONSTRUYENDO: dim_geografia")
print("="*80)

df_sucursales = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.tb_sucursales_red")

# Construir dimensión geográfica a partir de sucursales
dim_geografia = df_sucursales.groupBy(
    F.col("ciudad").alias("ciudad_codigo"),
    F.col("ciudad").alias("ciudad_nombre"),
    F.col("depto").alias("departamento_codigo"),
    F.col("depto").alias("departamento_nombre")
).agg(
    F.first("latitud").alias("latitud"),
    F.first("longitud").alias("longitud")
).withColumn("gold_timestamp", F.lit(PROCESS_TIMESTAMP)) \
 .withColumn("gold_batch_id", F.lit(BATCH_ID))

dim_geografia.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_geografia")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_geografia").count()
print(f"✓ dim_geografia creada: {count:,} registros")

# COMMAND ----------

# DBTITLE 1,dim_canal - Dimensión de Canales
# =============================================================================
# DIM_CANAL - Dimensión de Canales de Atención
# =============================================================================

print("\n" + "="*80)
print("CONSTRUYENDO: dim_canal")
print("="*80)

df_sucursales = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.tb_sucursales_red")

# Dimensión de canal basada en tipo de punto
dim_canal = df_sucursales.select(
    F.col("tip_punto").alias("canal_codigo"),
    # Etiquetas de negocio
    F.when(F.col("tip_punto") == "AGENCIA", "Agencia / Sucursal")
     .when(F.col("tip_punto") == "ATM", "Cajero Automático")
     .when(F.col("tip_punto") == "CORRESPONSAL", "Corresponsal Bancario")
     .otherwise("Otro").alias("canal_nombre"),
    F.lit(PROCESS_TIMESTAMP).alias("gold_timestamp"),
    F.lit(BATCH_ID).alias("gold_batch_id")
).distinct()

dim_canal.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_canal")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_canal").count()
print(f"✓ dim_canal creada: {count:,} registros")

# COMMAND ----------

# DBTITLE 1,fact_transacciones - Hechos Transaccionales
# =============================================================================
# FACT_TRANSACCIONES - Hechos Transaccionales
# =============================================================================

print("\n" + "="*80)
print("CONSTRUYENDO: fact_transacciones")
print("="*80)

df_movimientos = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.tb_mov_financieros")
dim_clientes_df = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_clientes")

# Validar FK contra dim_clientes
fact_transacciones = df_movimientos.join(
    dim_clientes_df.select("cliente_id"),
    df_movimientos["id_cli"] == dim_clientes_df["cliente_id"],
    "left_semi"
)

# Construir fact con métricas y flags
fact_transacciones = fact_transacciones.select(
    F.col("id_mov").alias("transaccion_id"),
    F.col("id_cli").alias("cliente_id"),
    F.col("cod_prod").alias("producto_id"),
    F.col("num_cuenta").alias("numero_cuenta"),
    F.col("fec_mov").alias("fecha_movimiento"),
    F.col("hra_mov").alias("hora_movimiento"),
    # Flag horario hábil (Lunes-Viernes 8:00-18:00)
    F.when(
        (F.dayofweek(F.col("fec_mov")).between(2, 6)) &  # Lunes=2, Viernes=6
        (F.hour(F.col("hra_mov")).between(8, 17))
    , True).otherwise(False).alias("flag_horario_habil"),
    F.col("vr_mov").alias("valor_movimiento_cop"),
    # Nota: No hay tasa de cambio disponible, se mantiene en COP
    F.col("tip_mov").alias("tipo_movimiento"),
    F.col("cod_canal").alias("canal_codigo"),
    F.col("cod_ciudad").alias("ciudad_codigo"),
    F.col("cod_estado_mov").alias("estado_movimiento"),
    F.col("id_dispositivo").alias("dispositivo_id"),
    # Nota: ind_sospechoso no se calcula en Gold según arquitectura
    # Debe calcularse en Silver con ventana de 30 días
    F.lit(PROCESS_TIMESTAMP).alias("gold_timestamp"),
    F.lit(BATCH_ID).alias("gold_batch_id")
)

fact_transacciones.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.fact_transacciones")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_transacciones").count()
print(f"✓ fact_transacciones creada: {count:,} registros")

# COMMAND ----------

# DBTITLE 1,fact_cartera - Hechos de Cartera
# =============================================================================
# FACT_CARTERA - Hechos de Cartera
# =============================================================================

print("\n" + "="*80)
print("CONSTRUYENDO: fact_cartera")
print("="*80)

df_obligaciones = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.tb_obligaciones")

# Construir fact_cartera con bucket_mora
fact_cartera = df_obligaciones.select(
    F.col("id_oblig").alias("obligacion_id"),
    F.col("id_cli").alias("cliente_id"),
    F.col("cod_prod").alias("producto_id"),
    F.col("vr_aprobado").alias("valor_aprobado"),
    F.col("vr_desembolsado").alias("valor_desembolsado"),
    F.col("sdo_capital").alias("saldo_capital"),
    F.col("vr_cuota").alias("valor_cuota"),
    F.col("fec_desembolso").alias("fecha_desembolso"),
    F.col("fec_venc").alias("fecha_vencimiento"),
    F.col("dias_mora_act").alias("dias_mora_actual"),
    # Bucket de mora según días de mora
    F.when(F.col("dias_mora_act") == 0, "Al día")
     .when(F.col("dias_mora_act").between(1, 30), "Mora 1-30 días")
     .when(F.col("dias_mora_act").between(31, 60), "Mora 31-60 días")
     .when(F.col("dias_mora_act").between(61, 90), "Mora 61-90 días")
     .when(F.col("dias_mora_act") > 90, "Deteriorado (>90 días)")
     .otherwise("No Clasificado").alias("bucket_mora"),
    F.col("num_cuotas_pend").alias("cuotas_pendientes"),
    F.col("calif_riesgo").alias("calificacion_riesgo"),
    # Nota: Provisión estimada requeriría tabla regulatoria no disponible
    F.lit(PROCESS_TIMESTAMP).alias("gold_timestamp"),
    F.lit(BATCH_ID).alias("gold_batch_id")
)

fact_cartera.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.fact_cartera")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_cartera").count()
print(f"✓ fact_cartera creada: {count:,} registros")

# COMMAND ----------

# DBTITLE 1,kpi_cartera_diario - KPIs Diarios de Cartera
# =============================================================================
# KPI_CARTERA_DIARIO - KPIs Diarios por Producto/Segmento/Ciudad
# =============================================================================

print("\n" + "="*80)
print("CONSTRUYENDO: kpi_cartera_diario")
print("="*80)

fact_cartera_df = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_cartera")
dim_clientes_df = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_clientes")

# Join con dim_clientes para obtener segmento y ciudad
df_kpi = fact_cartera_df.join(
    dim_clientes_df.select("cliente_id", "segmento_codigo", "ciudad_residencia"),
    "cliente_id",
    "left"
)

# Agregar por fecha (usamos fecha de proceso como snapshot), producto, segmento, ciudad
kpi_cartera_diario = df_kpi.groupBy(
    F.lit(F.current_date()).alias("fecha_snapshot"),
    F.col("producto_id"),
    F.col("segmento_codigo").alias("segmento"),
    F.col("ciudad_residencia").alias("ciudad")
).agg(
    # Total obligaciones activas
    F.count("obligacion_id").alias("total_obligaciones_activas"),
    # Monto total de cartera
    F.sum("saldo_capital").alias("monto_total_cartera"),
    # Monto en mora (solo donde dias_mora_actual > 0)
    F.sum(F.when(F.col("dias_mora_actual") > 0, F.col("saldo_capital")).otherwise(0)).alias("monto_en_mora"),
    # Clientes con mora
    F.countDistinct(F.when(F.col("dias_mora_actual") > 0, F.col("cliente_id"))).alias("clientes_con_mora")
).withColumn(
    # Tasa de mora (evitar división por cero)
    "tasa_mora",
    F.when(F.col("monto_total_cartera") > 0,
        (F.col("monto_en_mora") / F.col("monto_total_cartera")) * 100
    ).otherwise(0)
)

kpi_cartera_diario = kpi_cartera_diario.withColumn("gold_timestamp", F.lit(PROCESS_TIMESTAMP)) \
    .withColumn("gold_batch_id", F.lit(BATCH_ID))

kpi_cartera_diario.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.kpi_cartera_diario")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.kpi_cartera_diario").count()
print(f"✓ kpi_cartera_diario creado: {count:,} registros")

# COMMAND ----------

# DBTITLE 1,fact_rentabilidad_cliente - Rentabilidad por Cliente
# =============================================================================
# FACT_RENTABILIDAD_CLIENTE - Rentabilidad Mensual por Cliente
# =============================================================================

print("\n" + "="*80)
print("CONSTRUYENDO: fact_rentabilidad_cliente")
print("="*80)
print("NOTA: CLTV calculado solo con comisiones. Intereses no identificables en datos actuales.")

df_comisiones = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.tb_comisiones_log")

# Agregar por cliente y mes
fact_rentabilidad = df_comisiones.filter(
    F.col("estado_cobro") == "COBRADO"  # Solo comisiones efectivamente cobradas
).withColumn(
    "anio_mes",
    F.date_format(F.col("fec_cobro"), "yyyy-MM")
).groupBy(
    F.col("id_cli").alias("cliente_id"),
    F.col("anio_mes"),
    F.col("cod_prod").alias("producto_id")
).agg(
    # Comisiones del período
    F.sum("vr_comision").alias("comisiones_periodo"),
    F.count("id_comision").alias("num_comisiones")
)

# Calcular CLTV como suma móvil de últimos 12 meses
window_12m = Window.partitionBy("cliente_id").orderBy("anio_mes").rowsBetween(-11, 0)

fact_rentabilidad = fact_rentabilidad.withColumn(
    "cltv_12m_comisiones",
    F.sum("comisiones_periodo").over(window_12m)
).withColumn(
    "gold_timestamp", F.lit(PROCESS_TIMESTAMP)
).withColumn(
    "gold_batch_id", F.lit(BATCH_ID)
)

fact_rentabilidad.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.fact_rentabilidad_cliente")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_rentabilidad_cliente").count()
print(f"✓ fact_rentabilidad_cliente creada: {count:,} registros")
print("  Limitación: CLTV solo incluye comisiones (intereses no disponibles en datos)")

# COMMAND ----------

# DBTITLE 1,Agregación 1: Transacciones Diarias
# =============================================================================
# AGG_TRANSACCIONES_DIARIAS - Volumen Transaccional Diario
# =============================================================================

print("\n" + "="*80)
print("AGREGACIÓN 1: agg_transacciones_diarias")
print("="*80)

fact_trans = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_transacciones")

agg_transacciones_diarias = fact_trans.groupBy(
    F.col("fecha_movimiento"),
    F.col("tipo_movimiento"),
    F.col("canal_codigo")
).agg(
    F.count("transaccion_id").alias("num_transacciones"),
    F.sum("valor_movimiento_cop").alias("valor_total_cop"),
    F.avg("valor_movimiento_cop").alias("valor_promedio_cop"),
    F.countDistinct("cliente_id").alias("clientes_unicos"),
    F.sum(F.when(F.col("flag_horario_habil") == True, 1).otherwise(0)).alias("transacciones_horario_habil")
).withColumn(
    "porcentaje_horario_habil",
    F.when(F.col("num_transacciones") > 0,
        (F.col("transacciones_horario_habil") / F.col("num_transacciones")) * 100
    ).otherwise(0)
).withColumn("gold_timestamp", F.lit(PROCESS_TIMESTAMP)) \
 .withColumn("gold_batch_id", F.lit(BATCH_ID))

agg_transacciones_diarias.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.agg_transacciones_diarias")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.agg_transacciones_diarias").count()
print(f"✓ agg_transacciones_diarias creada: {count:,} registros")

# COMMAND ----------

# DBTITLE 1,Agregación 2: Análisis Producto-Segmento
# =============================================================================
# AGG_PRODUCTO_SEGMENTO - Análisis por Producto y Segmento
# =============================================================================

print("\n" + "="*80)
print("AGREGACIÓN 2: agg_producto_segmento")
print("="*80)

fact_cart = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_cartera")
dim_cli = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_clientes")
dim_prod = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_productos")

agg_producto_segmento = fact_cart.join(
    dim_cli.select("cliente_id", "segmento_codigo"),
    "cliente_id",
    "left"
).join(
    dim_prod.select("producto_id", "familia_producto"),
    "producto_id",
    "left"
).groupBy(
    F.col("producto_id"),
    F.col("familia_producto"),
    F.col("segmento_codigo")
).agg(
    F.count("obligacion_id").alias("num_obligaciones"),
    F.countDistinct("cliente_id").alias("num_clientes"),
    F.sum("saldo_capital").alias("saldo_total"),
    F.avg("saldo_capital").alias("saldo_promedio"),
    F.sum(F.when(F.col("dias_mora_actual") > 0, F.col("saldo_capital")).otherwise(0)).alias("saldo_en_mora"),
    F.countDistinct(F.when(F.col("dias_mora_actual") > 0, F.col("cliente_id"))).alias("clientes_en_mora")
).withColumn(
    "tasa_mora",
    F.when(F.col("saldo_total") > 0, (F.col("saldo_en_mora") / F.col("saldo_total")) * 100).otherwise(0)
).withColumn(
    "penetracion_mora",
    F.when(F.col("num_clientes") > 0, (F.col("clientes_en_mora") / F.col("num_clientes")) * 100).otherwise(0)
).withColumn("gold_timestamp", F.lit(PROCESS_TIMESTAMP)) \
 .withColumn("gold_batch_id", F.lit(BATCH_ID))

agg_producto_segmento.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.agg_producto_segmento")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.agg_producto_segmento").count()
print(f"✓ agg_producto_segmento creada: {count:,} registros")

# COMMAND ----------

# DBTITLE 1,Agregación 3: Comportamiento por Cliente
# =============================================================================
# AGG_COMPORTAMIENTO_CLIENTE - Resumen de Comportamiento por Cliente
# =============================================================================

print("\n" + "="*80)
print("AGREGACIÓN 3: agg_comportamiento_cliente")
print("="*80)

fact_trans = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_transacciones")
fact_cart = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_cartera")
fact_rent = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_rentabilidad_cliente")

# Transacciones por cliente (últimos 30 días)
fecha_corte = F.date_sub(F.current_date(), 30)
trans_cliente = fact_trans.filter(F.col("fecha_movimiento") >= fecha_corte) \
    .groupBy("cliente_id").agg(
        F.count("transaccion_id").alias("num_transacciones_30d"),
        F.sum("valor_movimiento_cop").alias("valor_total_transacciones_30d")
    )

# Cartera por cliente
cartera_cliente = fact_cart.groupBy("cliente_id").agg(
    F.count("obligacion_id").alias("num_obligaciones_activas"),
    F.sum("saldo_capital").alias("saldo_total_cartera"),
    F.max("dias_mora_actual").alias("max_dias_mora"),
    F.first("bucket_mora").alias("bucket_mora_actual")
)

# CLTV más reciente por cliente
cltv_cliente = fact_rent.withColumn(
    "row_num",
    F.row_number().over(Window.partitionBy("cliente_id").orderBy(F.desc("anio_mes")))
).filter(F.col("row_num") == 1) \
 .select("cliente_id", "cltv_12m_comisiones")

# Consolidar
agg_comportamiento_cliente = trans_cliente.join(cartera_cliente, "cliente_id", "full") \
    .join(cltv_cliente, "cliente_id", "left")

agg_comportamiento_cliente = agg_comportamiento_cliente.withColumn("gold_timestamp", F.lit(PROCESS_TIMESTAMP)) \
    .withColumn("gold_batch_id", F.lit(BATCH_ID))

agg_comportamiento_cliente.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.agg_comportamiento_cliente")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.agg_comportamiento_cliente").count()
print(f"✓ agg_comportamiento_cliente creada: {count:,} registros")

# COMMAND ----------

# DBTITLE 1,KPI Ejecutivo - Vista Consolidada para BI
# =============================================================================
# KPI_EJECUTIVO - Vista Consolidada para Consumo BI
# =============================================================================

print("\n" + "="*80)
print("CONSTRUYENDO: kpi_ejecutivo")
print("="*80)

fact_cart = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_cartera")
fact_trans = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_transacciones")

# KPIs de cartera
kpi_cartera = fact_cart.agg(
    F.lit("Cartera").alias("categoria"),
    F.count("obligacion_id").alias("total_obligaciones"),
    F.countDistinct("cliente_id").alias("total_clientes"),
    F.sum("saldo_capital").alias("saldo_total_cartera"),
    F.sum(F.when(F.col("dias_mora_actual") > 0, F.col("saldo_capital")).otherwise(0)).alias("saldo_en_mora"),
    F.countDistinct(F.when(F.col("dias_mora_actual") > 0, F.col("cliente_id"))).alias("clientes_en_mora")
).withColumn(
    "tasa_mora_pct",
    F.when(F.col("saldo_total_cartera") > 0,
        (F.col("saldo_en_mora") / F.col("saldo_total_cartera")) * 100
    ).otherwise(0)
)

# KPIs transaccionales (últimos 30 días)
fecha_corte = F.date_sub(F.current_date(), 30)
kpi_transaccional = fact_trans.filter(F.col("fecha_movimiento") >= fecha_corte).agg(
    F.lit("Transaccional").alias("categoria"),
    F.count("transaccion_id").alias("total_transacciones_30d"),
    F.countDistinct("cliente_id").alias("clientes_activos_30d"),
    F.sum("valor_movimiento_cop").alias("volumen_total_cop_30d"),
    F.avg("valor_movimiento_cop").alias("valor_promedio_transaccion_30d")
)

# Consolidar KPIs
kpi_ejecutivo = kpi_cartera.select(
    F.col("categoria"),
    F.col("total_obligaciones").alias("metrica_1"),
    F.col("saldo_total_cartera").alias("metrica_2"),
    F.col("saldo_en_mora").alias("metrica_3"),
    F.col("tasa_mora_pct").alias("metrica_4"),
    F.col("clientes_en_mora").alias("metrica_5"),
    F.lit(None).cast("long").alias("metrica_6")
).union(
    kpi_transaccional.select(
        F.col("categoria"),
        F.col("total_transacciones_30d").alias("metrica_1"),
        F.col("volumen_total_cop_30d").alias("metrica_2"),
        F.col("valor_promedio_transaccion_30d").alias("metrica_3"),
        F.lit(None).cast("double").alias("metrica_4"),
        F.col("clientes_activos_30d").alias("metrica_5"),
        F.lit(None).cast("long").alias("metrica_6")
    )
)

kpi_ejecutivo = kpi_ejecutivo.withColumn("fecha_actualizacion", F.current_date()) \
    .withColumn("gold_timestamp", F.lit(PROCESS_TIMESTAMP)) \
    .withColumn("gold_batch_id", F.lit(BATCH_ID))

kpi_ejecutivo.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.kpi_ejecutivo")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.kpi_ejecutivo").count()
print(f"✓ kpi_ejecutivo creado: {count:,} registros")

# COMMAND ----------

# DBTITLE 1,Validaciones - Calidad de Tablas Gold
# =============================================================================
# VALIDACIONES - Calidad de Tablas Gold
# =============================================================================

print("\n\n" + "="*80)
print("🔍 VALIDACIONES DE CALIDAD")
print("="*80)

# Listar todas las tablas Gold
gold_tables = [
    "dim_clientes", "dim_productos", "dim_geografia", "dim_canal",
    "fact_transacciones", "fact_cartera", "fact_rentabilidad_cliente",
    "kpi_cartera_diario", "agg_transacciones_diarias", "agg_producto_segmento",
    "agg_comportamiento_cliente", "kpi_ejecutivo"
]

print("\n1. Existencia y Conteos")
print("-" * 80)
for table in gold_tables:
    try:
        count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.{table}").count()
        print(f"  ✓ {table:35s}: {count:>10,} registros")
    except Exception as e:
        print(f"  ✗ {table:35s}: ERROR - {str(e)[:50]}")

print("\n2. Validación de Claves Principales")
print("-" * 80)

# dim_clientes
total = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_clientes").count()
distinct = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_clientes").select("cliente_id").distinct().count()
print(f"  dim_clientes - PK cliente_id: {total:,} total, {distinct:,} distintos {'✓' if total == distinct else '✗ DUPLICADOS'}")

# dim_productos
total = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_productos").count()
distinct = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_productos").select("producto_id").distinct().count()
print(f"  dim_productos - PK producto_id: {total:,} total, {distinct:,} distintos {'✓' if total == distinct else '✗ DUPLICADOS'}")

# fact_transacciones
total = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_transacciones").count()
distinct = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_transacciones").select("transaccion_id").distinct().count()
print(f"  fact_transacciones - PK transaccion_id: {total:,} total, {distinct:,} distintos {'✓' if total == distinct else '✗ DUPLICADOS'}")

# fact_cartera
total = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_cartera").count()
distinct = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_cartera").select("obligacion_id").distinct().count()
print(f"  fact_cartera - PK obligacion_id: {total:,} total, {distinct:,} distintos {'✓' if total == distinct else '✗ DUPLICADOS'}")

print("\n3. Validación de Bucket Mora")
print("-" * 80)
bucket_dist = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_cartera").groupBy("bucket_mora").count().orderBy("bucket_mora")
print("  Distribución de bucket_mora:")
for row in bucket_dist.collect():
    print(f"    {row['bucket_mora']:30s}: {row['count']:>8,} obligaciones")

print("\n4. Validación de Métricas de Cartera")
print("-" * 80)
kpi = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.kpi_cartera_diario").agg(
    F.sum("total_obligaciones_activas").alias("total_oblig"),
    F.sum("monto_total_cartera").alias("total_cartera"),
    F.sum("monto_en_mora").alias("total_mora"),
    F.avg("tasa_mora").alias("tasa_mora_promedio")
).collect()[0]

print(f"  Total Obligaciones: {kpi['total_oblig']:,}")
print(f"  Monto Total Cartera: ${kpi['total_cartera']:,.2f}")
print(f"  Monto en Mora: ${kpi['total_mora']:,.2f}")
print(f"  Tasa Mora Promedio: {kpi['tasa_mora_promedio']:.2f}%")

print("\n" + "="*80)
print("✅ VALIDACIONES COMPLETADAS")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Lineage - Documentación de Campos Calculados
# MAGIC %md
# MAGIC ## Lineage - Campos Calculados Importantes
# MAGIC
# MAGIC Documentación de transformaciones clave aplicadas en la capa Gold.
# MAGIC
# MAGIC ### 1. edad (dim_clientes)
# MAGIC - **Origen**: `finbank.silver.tb_clientes_core.fec_nac`
# MAGIC - **Fórmula**: `FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), fec_nac) / 12)`
# MAGIC - **Descripción**: Edad del cliente en años completos calculada desde fecha de nacimiento
# MAGIC - **Tabla Destino**: `finbank.gold.dim_clientes`
# MAGIC
# MAGIC ### 2. tasa_mensual_equivalente (dim_productos)
# MAGIC - **Origen**: `finbank.silver.tb_productos_cat.tasa_ea`
# MAGIC - **Fórmula**: `((1 + tasa_ea/100)^(1/12) - 1) * 100`
# MAGIC - **Descripción**: Tasa efectiva mensual equivalente derivada de la tasa efectiva anual
# MAGIC - **Tabla Destino**: `finbank.gold.dim_productos`
# MAGIC
# MAGIC ### 3. bucket_mora (fact_cartera)
# MAGIC - **Origen**: `finbank.silver.tb_obligaciones.dias_mora_act`
# MAGIC - **Fórmula**:
# MAGIC   - `dias_mora_act = 0` → "Al día"
# MAGIC   - `dias_mora_act BETWEEN 1 AND 30` → "Mora 1-30 días"
# MAGIC   - `dias_mora_act BETWEEN 31 AND 60` → "Mora 31-60 días"
# MAGIC   - `dias_mora_act BETWEEN 61 AND 90` → "Mora 61-90 días"
# MAGIC   - `dias_mora_act > 90` → "Deteriorado (>90 días)"
# MAGIC - **Descripción**: Clasificación de riesgo según días de mora acumulados
# MAGIC - **Tabla Destino**: `finbank.gold.fact_cartera`
# MAGIC
# MAGIC ### 4. flag_horario_habil (fact_transacciones)
# MAGIC - **Origen**: `finbank.silver.tb_mov_financieros.fec_mov`, `hra_mov`
# MAGIC - **Fórmula**: `DAYOFWEEK(fec_mov) BETWEEN 2 AND 6 AND HOUR(hra_mov) BETWEEN 8 AND 17`
# MAGIC - **Descripción**: Indica si la transacción ocurrió en horario hábil (Lunes-Viernes 8:00-18:00)
# MAGIC - **Tabla Destino**: `finbank.gold.fact_transacciones`
# MAGIC
# MAGIC ### 5. tasa_mora (kpi_cartera_diario, agregaciones)
# MAGIC - **Origen**: `fact_cartera.saldo_capital`, `dias_mora_actual`
# MAGIC - **Fórmula**: `(SUM(saldo_capital WHERE dias_mora_actual > 0) / SUM(saldo_capital)) * 100`
# MAGIC - **Descripción**: Porcentaje del saldo total que se encuentra en mora
# MAGIC - **Tabla Destino**: `finbank.gold.kpi_cartera_diario`, `agg_producto_segmento`
# MAGIC
# MAGIC ### 6. cltv_12m_comisiones (fact_rentabilidad_cliente)
# MAGIC - **Origen**: `finbank.silver.tb_comisiones_log.vr_comision`, `fec_cobro`
# MAGIC - **Fórmula**: `SUM(vr_comision) OVER (PARTITION BY cliente_id ORDER BY anio_mes ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)`
# MAGIC - **Descripción**: Customer Lifetime Value basado en comisiones cobradas en últimos 12 meses
# MAGIC - **Limitación**: No incluye intereses (no disponibles en datos fuente)
# MAGIC - **Tabla Destino**: `finbank.gold.fact_rentabilidad_cliente`
# MAGIC
# MAGIC ### Limitaciones Documentadas
# MAGIC
# MAGIC 1. **ind_sospechoso**: No implementado en Gold. Requiere cálculo en Silver con ventana de 30 días sobre historial transaccional.
# MAGIC
# MAGIC 2. **Intereses**: No existe fuente de datos para intereses cobrados. CLTV calculado solo con comisiones.
# MAGIC
# MAGIC 3. **Tasa de cambio COP-USD**: No disponible. Montos permanecen en COP.
# MAGIC
# MAGIC 4. **Provisión regulatoria**: No implementada. Requiere tabla regulatoria de provisiones no disponible en datos fuente.

# COMMAND ----------

# DBTITLE 1,Resumen Final - Ejecución Gold
# =============================================================================
# RESUMEN FINAL - Ejecución Completa de Gold Layer
# =============================================================================

print("\n\n" + "="*80)
print("🎯 RESUMEN FINAL - GOLD LAYER")
print("="*80)

print("\n📋 Tablas Gold Creadas:")
print("-" * 80)

gold_tables_summary = [
    ("Dimensiones", [
        "dim_clientes", "dim_productos", "dim_geografia", "dim_canal"
    ]),
    ("Hechos", [
        "fact_transacciones", "fact_cartera", "fact_rentabilidad_cliente"
    ]),
    ("KPIs", [
        "kpi_cartera_diario", "kpi_ejecutivo"
    ]),
    ("Agregaciones", [
        "agg_transacciones_diarias", "agg_producto_segmento", "agg_comportamiento_cliente"
    ])
]

total_registros_gold = 0
for categoria, tables in gold_tables_summary:
    print(f"\n{categoria}:")
    for table in tables:
        try:
            count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.{table}").count()
            total_registros_gold += count
            print(f"  ✓ {table:35s}: {count:>10,} registros")
        except Exception as e:
            print(f"  ✗ {table:35s}: ERROR")

print("\n" + "-" * 80)
print(f"TOTAL REGISTROS EN GOLD: {total_registros_gold:,}")

print("\n📈 KPIs Principales:")
print("-" * 80)

# Cartera
cartera_summary = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_cartera").agg(
    F.count("obligacion_id").alias("total_obligaciones"),
    F.countDistinct("cliente_id").alias("clientes_con_cartera"),
    F.sum("saldo_capital").alias("saldo_total"),
    F.sum(F.when(F.col("dias_mora_actual") > 0, 1).otherwise(0)).alias("obligaciones_mora")
).collect()[0]

print(f"  Obligaciones Activas: {cartera_summary['total_obligaciones']:,}")
print(f"  Clientes con Cartera: {cartera_summary['clientes_con_cartera']:,}")
print(f"  Saldo Total Cartera: ${cartera_summary['saldo_total']:,.2f} COP")
print(f"  Obligaciones en Mora: {cartera_summary['obligaciones_mora']:,}")

# Transacciones
trans_summary = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_transacciones").agg(
    F.count("transaccion_id").alias("total_transacciones"),
    F.countDistinct("cliente_id").alias("clientes_activos"),
    F.sum("valor_movimiento_cop").alias("volumen_total")
).collect()[0]

print(f"\n  Transacciones Totales: {trans_summary['total_transacciones']:,}")
print(f"  Clientes Activos: {trans_summary['clientes_activos']:,}")
print(f"  Volumen Transaccional: ${trans_summary['volumen_total']:,.2f} COP")

print("\n🔑 Campos Calculados Implementados:")
print("-" * 80)
print("  1. edad (años desde fecha nacimiento)")
print("  2. tasa_mensual_equivalente (de tasa EA)")
print("  3. bucket_mora (clasificación por días mora)")
print("  4. flag_horario_habil (Lun-Vie 8:00-18:00)")
print("  5. tasa_mora (% saldo en mora)")
print("  6. cltv_12m_comisiones (valor 12 meses)")

print("\n⚠️  Limitaciones Documentadas:")
print("-" * 80)
print("  1. ind_sospechoso: No calculado (debe estar en Silver)")
print("  2. Intereses: No disponibles (CLTV solo con comisiones)")
print("  3. Tasa cambio: No disponible (montos en COP)")
print("  4. Provisión: No calculada (requiere tabla regulatoria)")

print("\n" + "="*80)
print("✅ GOLD LAYER COMPLETADO EXITOSAMENTE")
print("="*80)
print(f"Batch ID: {BATCH_ID}")
print(f"Timestamp: {PROCESS_TIMESTAMP}")
print("="*80)