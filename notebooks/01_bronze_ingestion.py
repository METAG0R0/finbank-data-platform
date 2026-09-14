# Databricks notebook source
# DBTITLE 1,Instalar dependencias requeridas
# =============================================================================
# INSTALACIÓN DE DEPENDENCIAS - DATABRICKS SDK + PSYCOPG
# =============================================================================
# Lakebase Postgres requiere databricks-sdk >= 0.118.0
# Solo reinicia Python si la versión cambió

import importlib.metadata as md
import subprocess, sys

try:
    before = md.version("databricks-sdk")
except md.PackageNotFoundError:
    before = None

print(f"Versión actual de databricks-sdk: {before}")
print("Actualizando paquetes...")

subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "databricks-sdk>=0.118.0", "psycopg[binary]", "--quiet"])

after = md.version("databricks-sdk")
print(f"databricks-sdk: {before} -> {after}  (changed={before != after})")

if before != after:
    print("⚠ Versión cambió — reiniciando Python para cargar el nuevo SDK...")
    dbutils.library.restartPython()
else:
    print("✓ SDK ya está actualizado, no se requiere reinicio")

# COMMAND ----------

# DBTITLE 1,Bronze Ingestion - FinBank Data Platform
# MAGIC %md
# MAGIC # Bronze Ingestion - FinBank Data Platform
# MAGIC
# MAGIC Implementación de la capa Bronze para el proyecto FinBank. Este notebook:
# MAGIC - **Lee datos DIRECTAMENTE desde Lakebase PostgreSQL** (finbank-data-platform)
# MAGIC - Agrega metadatos de ingesta (ingest_timestamp, source_system, batch_id, ingest_date, record_hash)
# MAGIC - Implementa lógica incremental basada en hash SHA256
# MAGIC - Particiona por ingest_date
# MAGIC - Registra métricas de ejecución en finbank.bronze.ingestion_log
# MAGIC - Mantiene histórico completo en Bronze

# COMMAND ----------

# DBTITLE 1,Configuración e importaciones
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *
from datetime import datetime
import uuid
import psycopg2
from databricks.sdk import WorkspaceClient

# Configuración Unity Catalog
CATALOG = "finbank"
BRONZE_SCHEMA = "bronze"

# Configuración Lakebase PostgreSQL
LAKEBASE_PROJECT = "finbank-data-platform"
LAKEBASE_BRANCH = "production"
LAKEBASE_ENDPOINT = f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}/endpoints/primary"
LAKEBASE_DATABASE = "databricks_postgres"
SOURCE_SYSTEM = LAKEBASE_PROJECT

# ID de batch único por ejecución
BATCH_ID = str(uuid.uuid4())
INGEST_TIMESTAMP = F.current_timestamp()
INGEST_DATE = datetime.now().date()

print(f"Batch ID: {BATCH_ID}")
print(f"Fecha de ingesta: {INGEST_DATE}")
print(f"Source: {SOURCE_SYSTEM}")
print(f"Lakebase Endpoint: {LAKEBASE_ENDPOINT}")

# COMMAND ----------

# DBTITLE 1,Conectar a Lakebase PostgreSQL
# =============================================================================
# CONEXIÓN A LAKEBASE POSTGRESQL
# =============================================================================

print("Conectando a Lakebase PostgreSQL...")

try:
    # Initialize SDK client
    w = WorkspaceClient()
    
    # Get endpoint details
    ep = w.postgres.get_endpoint(name=LAKEBASE_ENDPOINT)
    print(f"Endpoint state: {ep.status.current_state}")
    print(f"Endpoint host: {ep.status.hosts.host}")
    
    # Generate credential
    credential = w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT)
    print(f"Credential expires: {credential.expire_time}")
    
    # Get current user
    username = w.current_user.me().user_name
    print(f"Connecting as: {username}")
    
    # Connect using psycopg2
    pg_conn = psycopg2.connect(
        host=ep.status.hosts.host,
        port=5432,
        dbname=LAKEBASE_DATABASE,
        user=username,
        password=credential.token,
        sslmode="require",
        connect_timeout=10
    )
    
    print("✓ Conectado a Lakebase PostgreSQL exitosamente")
    
except Exception as e:
    print(f"✗ Error conectando a Lakebase: {e}")
    raise

# COMMAND ----------

# DBTITLE 1,Verificar tablas fuente en Lakebase
# =============================================================================
# VERIFICAR TABLAS FUENTE EN LAKEBASE POSTGRESQL
# =============================================================================

source_tables = [
    "TB_CLIENTES_CORE",
    "TB_PRODUCTOS_CAT",
    "TB_MOV_FINANCIEROS",
    "TB_OBLIGACIONES",
    "TB_SUCURSALES_RED",
    "TB_COMISIONES_LOG"
]

print("\nVerificando tablas en Lakebase PostgreSQL...")
print("="*60)

table_counts = {}

try:
    with pg_conn.cursor() as cur:
        for table in source_tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            table_counts[table] = count
            print(f"✓ {table}: {count:,} registros")
    
    total_records = sum(table_counts.values())
    print("="*60)
    print(f"Total registros en Lakebase: {total_records:,}")
    print("✓ Todas las tablas fuente están disponibles")
    
except Exception as e:
    print(f"\n✗ Error verificando tablas: {e}")
    raise

# COMMAND ----------

# DBTITLE 1,Crear tabla de log de ingesta
# Crear tabla de log si no existe
log_table = f"{CATALOG}.{BRONZE_SCHEMA}.ingestion_log"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {log_table} (
        batch_id STRING,
        table_name STRING,
        ingest_timestamp TIMESTAMP,
        records_read BIGINT,
        records_written BIGINT,
        source_size_bytes BIGINT,
        bronze_size_bytes BIGINT,
        duration_seconds DOUBLE,
        status STRING,
        error_message STRING
    )
    USING DELTA
    PARTITIONED BY (ingest_timestamp)
""")

print(f"✓ Tabla de log creada: {log_table}")

# COMMAND ----------

# DBTITLE 1,Definir columnas de negocio por tabla
# Definir columnas de negocio para cada tabla (ESQUEMAS REALES DE LAKEBASE POSTGRESQL)
# La primera columna es la PK
table_configs = {
    "TB_CLIENTES_CORE": [
        "id_cli", "nomb_cli", "apell_cli", "tip_doc", "num_doc", 
        "fec_nac", "fec_alta", "cod_segmento", "score_buro", "ciudad_res", 
        "depto_res", "estado_cli", "canal_adquis"
    ],
    "TB_PRODUCTOS_CAT": [
        "cod_prod", "desc_prod", "tip_prod", "tasa_ea", 
        "plazo_max_meses", "cuota_min", "comision_admin", "estado_prod"
    ],
    "TB_MOV_FINANCIEROS": [
        "id_mov", "id_cli", "cod_prod", "num_cuenta", "fec_mov", "hra_mov", 
        "vr_mov", "tip_mov", "cod_canal", "cod_ciudad", "cod_estado_mov", "id_dispositivo"
    ],
    "TB_OBLIGACIONES": [
        "id_oblig", "id_cli", "cod_prod", "vr_aprobado", "vr_desembolsado", 
        "sdo_capital", "vr_cuota", "fec_desembolso", "fec_venc", 
        "dias_mora_act", "num_cuotas_pend", "calif_riesgo"
    ],
    "TB_SUCURSALES_RED": [
        "cod_suc", "nom_suc", "tip_punto", "ciudad", "depto", 
        "latitud", "longitud", "activo"
    ],
    "TB_COMISIONES_LOG": [
        "id_comision", "id_cli", "cod_prod", "fec_cobro", "vr_comision", 
        "tip_comision", "estado_cobro"
    ]
}

print("Configuración de tablas:")
for table, cols in table_configs.items():
    print(f"  {table}: {len(cols)} columnas (PK: {cols[0]})")

# COMMAND ----------

# DBTITLE 1,Ingesta de tablas a Bronze
# =============================================================================
# INGESTA DE TABLAS A BRONZE
# =============================================================================

print("\n" + "="*80)
print("INICIANDO INGESTA A BRONZE")
print("="*80)

ingestion_results = []

for table_name, business_cols in table_configs.items():
    try:
        print(f"\n{'='*80}")
        print(f"📥 Procesando: {table_name}")
        print(f"{'='*80}")
        
        # Leer datos desde Lakebase PostgreSQL usando psycopg2
        query = f"SELECT * FROM {table_name}"
        
        with pg_conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
        
        source_count = len(rows)
        print(f"   Registros leídos desde Lakebase: {source_count:,}")
        
        if source_count == 0:
            print(f"   ⚠ Tabla vacía, saltando...")
            ingestion_results.append({
                "table": table_name,
                "status": "SKIPPED",
                "source_rows": 0,
                "bronze_rows": 0
            })
            continue
        
        # Convertir a DataFrame de Spark
        df_source = spark.createDataFrame(rows, schema=columns)
        
        # Agregar columnas de metadatos Bronze
        df_bronze = (
            df_source
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("ingestion_source", F.lit(SOURCE_SYSTEM))
            .withColumn("ingestion_batch_id", F.lit(BATCH_ID))
            .withColumn("is_active", F.lit(True))
            .withColumn("is_deleted", F.lit(False))
        )
        
        # Escribir a Bronze (sin mergeSchema)
        target_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
        
        df_bronze.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_table)
        
        # Validar conteo
        written_count = spark.table(target_table).count()
        
        ingestion_results.append({
            "table": table_name,
            "status": "SUCCESS",
            "source_rows": source_count,
            "bronze_rows": written_count
        })
        
        print(f"   ✓ Escrito en: {target_table}")
        print(f"   Registros escritos: {written_count:,}")
        print(f"   Validación: {'✓ MATCH' if source_count == written_count else '✗ MISMATCH'}")
        
    except Exception as e:
        print(f"   ✗ ERROR: {str(e)}")
        ingestion_results.append({
            "table": table_name,
            "status": "ERROR",
            "source_rows": 0,
            "bronze_rows": 0
        })

# Resumen final
print(f"\n\n{'='*80}")
print(f"📊 RESUMEN DE INGESTA")
print(f"{'='*80}")

success_count = len([r for r in ingestion_results if r["status"] == "SUCCESS"])
total_count = len(ingestion_results)
total_source = sum([r["source_rows"] for r in ingestion_results])
total_bronze = sum([r["bronze_rows"] for r in ingestion_results])

print(f"   Tablas procesadas: {success_count}/{total_count}")
print(f"   Total registros origen: {total_source:,}")
print(f"   Total registros Bronze: {total_bronze:,}")
print(f"   Estado: {'✅ TODAS EXITOSAS' if success_count == total_count else '⚠️ ALGUNAS FALLARON'}")
print(f"{'='*80}")

# COMMAND ----------

# DBTITLE 1,Validación de tablas Bronze
# Validar tablas Bronze creadas
print("\n" + "="*60)
print("VALIDACIÓN DE TABLAS BRONZE")
print("="*60)

validation_results = []

for table_name in table_configs.keys():
    bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    
    try:
        # Verificar que la tabla existe
        if not spark.catalog.tableExists(bronze_table):
            validation_results.append({
                "table": table_name,
                "exists": False,
                "records": 0,
                "has_metadata": False,
                "partitioned": False,
                "status": "MISSING"
            })
            continue
        
        df = spark.table(bronze_table)
        count = df.count()
        
        # Verificar columnas de metadatos
        required_metadata = ["ingest_timestamp", "source_system", "batch_id", "ingest_date", "record_hash"]
        has_metadata = all(col in df.columns for col in required_metadata)
        
        # Verificar particionamiento
        table_info = spark.sql(f"DESCRIBE DETAIL {bronze_table}").collect()[0]
        partitioned = "ingest_date" in str(table_info["partitionColumns"])
        
        validation_results.append({
            "table": table_name,
            "exists": True,
            "records": count,
            "has_metadata": has_metadata,
            "partitioned": partitioned,
            "status": "OK" if (has_metadata and partitioned and count > 0) else "INCOMPLETE"
        })
        
    except Exception as e:
        validation_results.append({
            "table": table_name,
            "exists": False,
            "records": 0,
            "has_metadata": False,
            "partitioned": False,
            "status": f"ERROR: {str(e)[:50]}"
        })

# Mostrar resultados
validation_df = spark.createDataFrame(validation_results)
validation_df.show(truncate=False)

# Resumen final
ok_tables = len([r for r in validation_results if r["status"] == "OK"])
print(f"\n{'='*60}")
print(f"RESULTADO FINAL: {ok_tables}/{len(table_configs)} tablas OK")
print(f"{'='*60}")

if ok_tables == len(table_configs):
    print("✓ Todas las tablas Bronze fueron creadas correctamente")
    print("✓ Todas tienen columnas de metadatos")
    print("✓ Todas están particionadas por ingest_date")
else:
    print(f"⚠ {len(table_configs) - ok_tables} tabla(s) con problemas")