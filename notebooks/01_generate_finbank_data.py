# Databricks notebook source
from datetime import date

CONFIG = {
    "seed": 20260913,
    "fecha_inicio": "2025-09-01",
    "fecha_fin": "2026-08-31",
    "null_rate": 0.05,

    "clientes": 10_000,
    "productos": 50,
    "mov_financieros": 500_000,
    "obligaciones": 30_000,
    "sucursales": 200,
    "comisiones": 80_000,
}

CONFIG

# COMMAND ----------

from pyspark.sql import functions as F

productos = (
    spark.range(1, CONFIG["productos"] + 1)
    .withColumnRenamed("id", "num_producto")
    .withColumn("cod_prod", F.concat(F.lit("PROD_"), F.lpad(F.col("num_producto"), 3, "0")))
    .withColumn(
        "desc_prod",
        F.when((F.col("num_producto") % 4) == 0, "Cuenta Corriente")
        .when((F.col("num_producto") % 4) == 1, "Cuenta Ahorros")
        .when((F.col("num_producto") % 4) == 2, "Credito Consumo")
        .otherwise("Tarjeta Credito")
    )
    .withColumn(
        "tip_prod",
        F.when(F.col("num_producto") % 2 == 0, "PASIVO")
        .otherwise("ACTIVO")
    )
    .withColumn("tasa_ea", F.round(F.lit(5) + F.rand(CONFIG["seed"]) * 25, 4))
    .withColumn("plazo_max_meses", (F.lit(12) + (F.rand(CONFIG["seed"] + 1) * 48)).cast("int"))
    .withColumn("cuota_min", F.round(F.lit(50) + F.rand(CONFIG["seed"] + 2) * 950, 2))
    .withColumn("comision_admin", F.round(F.rand(CONFIG["seed"] + 3) * 100, 2))
    .withColumn(
        "estado_prod",
        F.when(F.col("num_producto") % 10 == 0, "INACTIVO")
        .otherwise("ACTIVO")
    )
    .select(
        "cod_prod",
        "desc_prod",
        "tip_prod",
        "tasa_ea",
        "plazo_max_meses",
        "cuota_min",
        "comision_admin",
        "estado_prod"
    )
)

display(productos)

# COMMAND ----------

clientes = (
    spark.range(1, CONFIG["clientes"] + 1)
    .withColumnRenamed("id", "id_cli")
    .withColumn("nomb_cli", F.concat(F.lit("Cliente_"), F.col("id_cli")))
    .withColumn("apell_cli", F.concat(F.lit("Apellido_"), F.col("id_cli")))
    .withColumn(
        "tip_doc",
        F.when(F.col("id_cli") % 10 == 0, "CE").otherwise("DNI")
    )
    .withColumn("num_doc", F.concat(F.lit("DOC"), F.lpad(F.col("id_cli"), 8, "0")))
    .withColumn(
        "fec_nac",
        F.date_add(
            F.to_date(F.lit(CONFIG["fecha_inicio"])),
            -(
                F.lit(18 * 365)
                + (F.rand(CONFIG["seed"] + 10) * (65 * 365)).cast("int")
            )
        )
    )
    .withColumn(
        "fec_alta",
        F.expr(
            f"date_add('{CONFIG['fecha_inicio']}', "
            f"cast(rand({CONFIG['seed'] + 11}) * 365 as int))"
        )
    )
    .withColumn(
        "cod_segmento",
        F.when(F.col("id_cli") % 4 == 0, "PREMIUM")
        .when(F.col("id_cli") % 4 == 1, "MASIVO")
        .when(F.col("id_cli") % 4 == 2, "PYME")
        .otherwise("PREFERENTE")
    )
    .withColumn("score_buro", F.round(F.lit(400) + F.rand(CONFIG["seed"] + 12) * 450, 2))
    .withColumn(
        "ciudad_res",
        F.when(F.col("id_cli") % 5 == 0, "Lima")
        .when(F.col("id_cli") % 5 == 1, "Arequipa")
        .when(F.col("id_cli") % 5 == 2, "Cusco")
        .when(F.col("id_cli") % 5 == 3, "Trujillo")
        .otherwise("Piura")
    )
    .withColumn(
        "depto_res",
        F.when(F.col("ciudad_res") == "Lima", "Lima")
        .when(F.col("ciudad_res") == "Arequipa", "Arequipa")
        .when(F.col("ciudad_res") == "Cusco", "Cusco")
        .when(F.col("ciudad_res") == "Trujillo", "La Libertad")
        .otherwise("Piura")
    )
    .withColumn(
        "estado_cli",
        F.when(F.col("id_cli") % 20 == 0, "INACTIVO").otherwise("ACTIVO")
    )
    .withColumn(
        "canal_adquis",
        F.when(F.col("id_cli") % 3 == 0, "APP")
        .when(F.col("id_cli") % 3 == 1, "SUCURSAL")
        .otherwise("WEB")
    )
    .select(
        "id_cli",
        "nomb_cli",
        "apell_cli",
        "tip_doc",
        "num_doc",
        "fec_nac",
        "fec_alta",
        "cod_segmento",
        "score_buro",
        "ciudad_res",
        "depto_res",
        "estado_cli",
        "canal_adquis"
    )
)

display(clientes)

# COMMAND ----------

sucursales = (
    spark.range(1, CONFIG["sucursales"] + 1)
    .withColumnRenamed("id", "num_sucursal")
    .withColumn(
        "cod_suc",
        F.concat(F.lit("SUC_"), F.lpad(F.col("num_sucursal"), 3, "0"))
    )
    .withColumn(
        "nom_suc",
        F.concat(F.lit("Sucursal "), F.col("num_sucursal"))
    )
    .withColumn(
        "tip_punto",
        F.when(F.col("num_sucursal") % 3 == 0, "AGENCIA")
        .when(F.col("num_sucursal") % 3 == 1, "ATM")
        .otherwise("CORRESPONSAL")
    )
    .withColumn(
        "ciudad",
        F.when(F.col("num_sucursal") % 5 == 0, "Lima")
        .when(F.col("num_sucursal") % 5 == 1, "Arequipa")
        .when(F.col("num_sucursal") % 5 == 2, "Cusco")
        .when(F.col("num_sucursal") % 5 == 3, "Trujillo")
        .otherwise("Piura")
    )
    .withColumn(
        "depto",
        F.when(F.col("ciudad") == "Lima", "Lima")
        .when(F.col("ciudad") == "Arequipa", "Arequipa")
        .when(F.col("ciudad") == "Cusco", "Cusco")
        .when(F.col("ciudad") == "Trujillo", "La Libertad")
        .otherwise("Piura")
    )
    .withColumn(
        "latitud",
        F.round(F.lit(-12.05) + (F.rand(CONFIG["seed"] + 20) - 0.5) * 8, 7)
    )
    .withColumn(
        "longitud",
        F.round(F.lit(-77.04) + (F.rand(CONFIG["seed"] + 21) - 0.5) * 8, 7)
    )
    .withColumn(
        "activo",
        F.when(F.col("num_sucursal") % 20 == 0, False).otherwise(True)
    )
    .select(
        "cod_suc",
        "nom_suc",
        "tip_punto",
        "ciudad",
        "depto",
        "latitud",
        "longitud",
        "activo"
    )
)

display(sucursales)

# COMMAND ----------

obligaciones = (
    spark.range(1, CONFIG["obligaciones"] + 1)
    .withColumnRenamed("id", "id_oblig")

    # Referencia válida a clientes; 1% inválida de forma intencional
    .withColumn(
        "id_cli",
        F.when(
            F.col("id_oblig") % 100 == 0,
            F.lit(CONFIG["clientes"] + 999)
        ).otherwise(
            (F.rand(CONFIG["seed"] + 30) * CONFIG["clientes"]).cast("long") + 1
        )
    )

    # Referencia válida a productos; 1% inválida de forma intencional
    .withColumn(
        "cod_prod",
        F.when(
            F.col("id_oblig") % 100 == 0,
            F.lit("PROD_999")
        ).otherwise(
            F.concat(
                F.lit("PROD_"),
                F.lpad(
                    (F.rand(CONFIG["seed"] + 31) * CONFIG["productos"])
                    .cast("int") + 1,
                    3,
                    "0"
                )
            )
        )
    )

    .withColumn(
        "vr_aprobado",
        F.round(
            F.lit(1000) + F.rand(CONFIG["seed"] + 32) * 49000,
            2
        )
    )

    .withColumn(
        "vr_desembolsado",
        F.round(
            F.col("vr_aprobado") *
            (0.8 + F.rand(CONFIG["seed"] + 33) * 0.2),
            2
        )
    )

    .withColumn(
        "sdo_capital",
        F.round(
            F.col("vr_desembolsado") *
            (0.1 + F.rand(CONFIG["seed"] + 34) * 0.8),
            2
        )
    )

    .withColumn(
        "vr_cuota",
        F.round(
            F.lit(50) + F.rand(CONFIG["seed"] + 35) * 1950,
            2
        )
    )

    .withColumn(
        "fec_desembolso",
        F.date_add(
            F.to_date(F.lit(CONFIG["fecha_inicio"])),
            (F.rand(CONFIG["seed"] + 36) * 365).cast("int")
        )
    )

    .withColumn(
        "fec_venc",
        F.date_add(
            F.to_date(F.lit(CONFIG["fecha_inicio"])),
            365 + (F.rand(CONFIG["seed"] + 37) * 365).cast("int")
        )
    )

    .withColumn(
        "dias_mora_act",
        F.when(
            F.col("id_oblig") % 10 == 0,
            (F.rand(CONFIG["seed"] + 38) * 120).cast("int")
        ).otherwise(0)
    )

    .withColumn(
        "num_cuotas_pend",
        (F.rand(CONFIG["seed"] + 39) * 48).cast("int")
    )

    .withColumn(
        "calif_riesgo",
        F.when(F.col("dias_mora_act") > 90, "D")
        .when(F.col("dias_mora_act") > 60, "C")
        .when(F.col("dias_mora_act") > 30, "B")
        .otherwise("A")
    )

    .select(
        "id_oblig",
        "id_cli",
        "cod_prod",
        "vr_aprobado",
        "vr_desembolsado",
        "sdo_capital",
        "vr_cuota",
        "fec_desembolso",
        "fec_venc",
        "dias_mora_act",
        "num_cuotas_pend",
        "calif_riesgo"
    )
)

display(obligaciones)

# COMMAND ----------

comisiones = (
    spark.range(1, CONFIG["comisiones"] + 1)
    .withColumnRenamed("id", "id_comision")

    .withColumn(
        "id_cli",
        (F.rand(CONFIG["seed"] + 40) * CONFIG["clientes"]).cast("long") + 1
    )

    .withColumn(
        "cod_prod",
        F.concat(
            F.lit("PROD_"),
            F.lpad(
                (F.rand(CONFIG["seed"] + 41) * CONFIG["productos"]).cast("int") + 1,
                3,
                "0"
            )
        )
    )

    .withColumn(
        "fec_cobro",
        F.date_add(
            F.to_date(F.lit(CONFIG["fecha_inicio"])),
            (F.rand(CONFIG["seed"] + 42) * 365).cast("int")
        )
    )

    .withColumn(
        "vr_comision",
        F.round(
            F.lit(1) + F.rand(CONFIG["seed"] + 43) * 199,
            2
        )
    )

    .withColumn(
        "tip_comision",
        F.when(F.col("id_comision") % 3 == 0, "ADMINISTRACION")
        .when(F.col("id_comision") % 3 == 1, "TRANSACCIONAL")
        .otherwise("SERVICIO")
    )

    .withColumn(
        "estado_cobro",
        F.when(F.col("id_comision") % 20 == 0, "PENDIENTE")
        .otherwise("COBRADA")
    )

    .select(
        "id_comision",
        "id_cli",
        "cod_prod",
        "fec_cobro",
        "vr_comision",
        "tip_comision",
        "estado_cobro"
    )
)

display(comisiones)

# COMMAND ----------

mov_financieros = (
    spark.range(1, CONFIG["mov_financieros"] + 1)
    .withColumnRenamed("id", "id_mov")

    .withColumn(
        "id_cli",
        F.when(
            F.col("id_mov") % 100 == 0,
            F.lit(CONFIG["clientes"] + 999)
        ).otherwise(
            (F.rand(CONFIG["seed"] + 50) * CONFIG["clientes"])
            .cast("long") + 1
        )
    )

    .withColumn(
        "cod_prod",
        F.when(
            F.col("id_mov") % 100 == 0,
            F.lit("PROD_999")
        ).otherwise(
            F.concat(
                F.lit("PROD_"),
                F.lpad(
                    (F.rand(CONFIG["seed"] + 51) * CONFIG["productos"])
                    .cast("int") + 1,
                    3,
                    "0"
                )
            )
        )
    )

    .withColumn(
        "num_cuenta",
        F.concat(
            F.lit("CTA_"),
            F.lpad(F.col("id_cli"), 8, "0")
        )
    )

    .withColumn(
        "fec_mov",
        F.date_add(
            F.to_date(F.lit(CONFIG["fecha_inicio"])),
            (F.rand(CONFIG["seed"] + 52) * 365).cast("int")
        )
    )

    .withColumn(
        "hra_mov",
        F.make_time(
            (F.rand(CONFIG["seed"] + 53) * 24).cast("int"),
            (F.rand(CONFIG["seed"] + 54) * 60).cast("int"),
            (F.rand(CONFIG["seed"] + 55) * 60).cast("int")
        )
    )

    .withColumn(
        "vr_mov",
        F.round(
            F.lit(10) + F.rand(CONFIG["seed"] + 56) * 4990,
            2
        )
    )

    .withColumn(
        "tip_mov",
        F.when(F.col("id_mov") % 4 == 0, "RETIRO")
        .when(F.col("id_mov") % 4 == 1, "DEPOSITO")
        .when(F.col("id_mov") % 4 == 2, "PAGO")
        .otherwise("TRANSFERENCIA")
    )

    .withColumn(
        "cod_canal",
        F.when(F.col("id_mov") % 3 == 0, "APP")
        .when(F.col("id_mov") % 3 == 1, "WEB")
        .otherwise("SUCURSAL")
    )

    .withColumn(
        "cod_ciudad",
        F.when(F.col("id_mov") % 5 == 0, "Lima")
        .when(F.col("id_mov") % 5 == 1, "Arequipa")
        .when(F.col("id_mov") % 5 == 2, "Cusco")
        .when(F.col("id_mov") % 5 == 3, "Trujillo")
        .otherwise("Piura")
    )

    .withColumn(
        "cod_estado_mov",
        F.when(F.col("id_mov") % 50 == 0, "RECHAZADO")
        .otherwise("APROBADO")
    )

    .withColumn(
        "id_dispositivo",
        F.concat(
            F.lit("DEV_"),
            F.lpad(
                (F.rand(CONFIG["seed"] + 57) * 50000)
                .cast("int"),
                5,
                "0"
            )
        )
    )

    .select(
        "id_mov",
        "id_cli",
        "cod_prod",
        "num_cuenta",
        "fec_mov",
        "hra_mov",
        "vr_mov",
        "tip_mov",
        "cod_canal",
        "cod_ciudad",
        "cod_estado_mov",
        "id_dispositivo"
    )
)

display(mov_financieros)

# COMMAND ----------

dfs = {
    "TB_CLIENTES_CORE": clientes,
    "TB_PRODUCTOS_CAT": productos,
    "TB_MOV_FINANCIEROS": mov_financieros,
    "TB_OBLIGACIONES": obligaciones,
    "TB_SUCURSALES_RED": sucursales,
    "TB_COMISIONES_LOG": comisiones
}

for nombre, df in dfs.items():
    print(f"{nombre}: {df.count():,} registros")

# COMMAND ----------

pk_cols = {
    "TB_CLIENTES_CORE": "id_cli",
    "TB_PRODUCTOS_CAT": "cod_prod",
    "TB_MOV_FINANCIEROS": "id_mov",
    "TB_OBLIGACIONES": "id_oblig",
    "TB_SUCURSALES_RED": "cod_suc",
    "TB_COMISIONES_LOG": "id_comision"
}

for nombre, pk in pk_cols.items():
    df = dfs[nombre]

    duplicados = (
        df.groupBy(pk)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"{nombre} → duplicados PK: {duplicados}")

# COMMAND ----------

clientes_ids = clientes.select("id_cli").distinct()
productos_ids = productos.select("cod_prod").distinct()

print("Referencias inválidas en obligaciones:")

(
    obligaciones
    .join(clientes_ids, "id_cli", "left_anti")
    .count()
)

# COMMAND ----------

print("Referencias inválidas de producto en obligaciones:")

(
    obligaciones
    .join(productos_ids, "cod_prod", "left_anti")
    .count()
)

# COMMAND ----------

# MAGIC %pip install --upgrade "databricks-sdk>=0.89.0" "psycopg[binary]"

# COMMAND ----------

# DBTITLE 1,Cell 16
import psycopg2
from databricks.sdk import WorkspaceClient

# Initialize SDK client
w = WorkspaceClient()

# Get endpoint details to verify it's active and get the correct host
endpoint_name = "projects/finbank-data-platform/branches/production/endpoints/primary"
ep = w.postgres.get_endpoint(name=endpoint_name)

print(f"Endpoint state: {ep.status.current_state}")
print(f"Endpoint host: {ep.status.hosts.host}")

# Generate fresh credential for this endpoint
credential = w.postgres.generate_database_credential(endpoint=endpoint_name)
print(f"Credential expires: {credential.expire_time}")

# Get current user
username = w.current_user.me().user_name
print(f"Connecting as: {username}")

# Connect using psycopg2 with verified endpoint details
conn = psycopg2.connect(
    host=ep.status.hosts.host,
    port=5432,
    dbname="databricks_postgres",
    user=username,
    password=credential.token,
    sslmode="require",
    connect_timeout=10
)

print("✅ Conectado a Lakebase")

# COMMAND ----------

# DBTITLE 1,Carga masiva a Lakebase
# =============================================================================
# CARGA MASIVA DE DATOS A LAKEBASE POSTGRES
# =============================================================================
# Carga todos los DataFrames a sus tablas correspondientes en Lakebase
# usando TRUNCATE + INSERT masivo con psycopg2.extras.execute_values
# =============================================================================

import psycopg2
from psycopg2.extras import execute_values
from databricks.sdk import WorkspaceClient
import pandas as pd

# Diccionario: tabla_destino -> dataframe_spark
tablas_df = {
    "TB_PRODUCTOS_CAT": productos,
    "TB_CLIENTES_CORE": clientes,
    "TB_SUCURSALES_RED": sucursales,
    "TB_OBLIGACIONES": obligaciones,
    "TB_COMISIONES_LOG": comisiones,
    "TB_MOV_FINANCIEROS": mov_financieros
}

print("\n" + "="*70)
print("INICIANDO CARGA MASIVA A LAKEBASE POSTGRES")
print("="*70)

# Verificar y restablecer conexión si está cerrada
if conn.closed:
    print("\n⚠️  Reconectando...")
    w = WorkspaceClient()
    endpoint_name = "projects/finbank-data-platform/branches/production/endpoints/primary"
    ep = w.postgres.get_endpoint(name=endpoint_name)
    credential = w.postgres.generate_database_credential(endpoint=endpoint_name)
    username = w.current_user.me().user_name
    
    conn = psycopg2.connect(
        host=ep.status.hosts.host,
        port=5432,
        dbname="databricks_postgres",
        user=username,
        password=credential.token,
        sslmode="require",
        connect_timeout=10
    )
    print("✅ Reconexión exitosa\n")
else:
    print("✅ Conexión activa\n")

# Iterar sobre cada tabla y cargar
resumen = []

for tabla, df_spark in tablas_df.items():
    try:
        print(f"\n{'='*70}")
        print(f"CARGANDO: {tabla}")
        print(f"{'='*70}")
        
        # 1. Contar registros actuales
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {tabla}")
            count_antes = cur.fetchone()[0]
            print(f"Registros actuales: {count_antes:,}")
        
        # 2. TRUNCAR la tabla
        if count_antes > 0:
            with conn.cursor() as cur:
                print(f"Vaciando tabla...")
                cur.execute(f"TRUNCATE TABLE {tabla} CASCADE")
                conn.commit()
                print(f"✅ Tabla vaciada")
        
        # 3. Convertir Spark DataFrame a pandas
        print(f"Convirtiendo a pandas...")
        df_pandas = df_spark.toPandas()
        num_registros = len(df_pandas)
        print(f"Registros a cargar: {num_registros:,}")
        
        # 4. Insertar datos
        print(f"Insertando datos...")
        with conn.cursor() as cur:
            columnas = list(df_pandas.columns)
            col_names = ', '.join(columnas)
            valores = [tuple(x) for x in df_pandas.to_numpy()]
            
            query = f"INSERT INTO {tabla} ({col_names}) VALUES %s"
            execute_values(cur, query, valores)
            conn.commit()
            print(f"✅ Inserción completada")
        
        # 5. Verificar resultado
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {tabla}")
            count_despues = cur.fetchone()[0]
            print(f"Registros finales: {count_despues:,}")
        
        resumen.append({
            "tabla": tabla,
            "intentados": num_registros,
            "insertados": count_despues,
            "status": "✅ OK"
        })
        
    except Exception as e:
        print(f"\n❌ ERROR en {tabla}: {e}")
        conn.rollback()
        resumen.append({
            "tabla": tabla,
            "intentados": 0,
            "insertados": 0,
            "status": f"❌ ERROR: {str(e)[:50]}"
        })
        # No hacer raise para continuar con las demás tablas

# Resumen final
print("\n" + "="*70)
print("RESUMEN DE CARGA")
print("="*70)
for item in resumen:
    print(f"{item['tabla']:25} | Intentados: {item['intentados']:6,} | Insertados: {item['insertados']:6,} | {item['status']}")

print("\n" + "="*70)
print("✅ PROCESO DE CARGA COMPLETADO")
print("="*70)