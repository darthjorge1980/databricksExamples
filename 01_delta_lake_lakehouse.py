# =============================================================================
# DATABRICKS - DELTA LAKE & LAKEHOUSE ARCHITECTURE
# =============================================================================
# Característica diferenciadora: Delta Lake es el formato de tabla abierto que
# combina lo mejor de data warehouses y data lakes. Ningún otro producto ofrece
# ACID transactions + time travel + schema evolution sobre archivos en un lake.
# Competidores como Snowflake requieren cargar datos en su formato propietario.
# =============================================================================

# ---------------------------------------------------------------------------
# Ejemplo 1: Crear y escribir una tabla Delta con ACID transactions
# ---------------------------------------------------------------------------
# A diferencia de Hive/Parquet tradicional, Delta Lake garantiza atomicidad.
# Si un job falla a mitad de escritura, la tabla NO queda corrupta.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

spark = SparkSession.builder.getOrCreate()

# Crear datos de ejemplo
data = [
    ("TX001", "2026-03-10", "Compra Online", 1250.00, "USD"),
    ("TX002", "2026-03-10", "Transferencia", 5000.00, "USD"),
    ("TX003", "2026-03-10", "Pago Nómina", 32000.00, "MXN"),
    ("TX004", "2026-03-10", "Compra POS", 890.50, "USD"),
    ("TX005", "2026-03-10", "Retiro ATM", 200.00, "USD"),
]

schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("fecha", StringType(), False),
    StructField("tipo", StringType(), True),
    StructField("monto", DoubleType(), True),
    StructField("moneda", StringType(), True),
])

df = spark.createDataFrame(data, schema)

# Escritura como tabla Delta (ACID garantizado)
df.write.format("delta").mode("overwrite").saveAsTable("finanzas.transacciones")

print("✅ Tabla Delta creada con transacciones ACID")

# ---------------------------------------------------------------------------
# Ejemplo 2: Time Travel - Consultar versiones anteriores de los datos
# ---------------------------------------------------------------------------
# EXCLUSIVO de Delta Lake: puedes viajar en el tiempo y ver cómo estaban
# los datos en cualquier momento pasado. Snowflake tiene algo similar pero
# limitado a 90 días y sin control de versiones granular.

# Ver historial completo de cambios
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "finanzas.transacciones")
history = delta_table.history()
history.select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

# Consultar datos como estaban en la versión 0
df_version_0 = spark.read.format("delta").option("versionAsOf", 0).table("finanzas.transacciones")
df_version_0.show()

# Consultar datos por timestamp
df_timestamp = (spark.read.format("delta")
    .option("timestampAsOf", "2026-03-10 08:00:00")
    .table("finanzas.transacciones"))

# Restaurar a una versión anterior (ROLLBACK instantáneo)
delta_table.restoreToVersion(0)
print("✅ Tabla restaurada a versión 0 - Rollback instantáneo!")

# ---------------------------------------------------------------------------
# Ejemplo 3: Schema Evolution - Evolución automática del esquema
# ---------------------------------------------------------------------------
# Delta Lake permite agregar columnas automáticamente sin romper pipelines.
# En Redshift/BigQuery necesitas ALTER TABLE + migración manual.

nuevos_datos = [
    ("TX006", "2026-03-11", "Compra Online", 750.00, "EUR", "España"),
    ("TX007", "2026-03-11", "Transferencia", 3200.00, "USD", "México"),
]

schema_extendido = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("fecha", StringType(), False),
    StructField("tipo", StringType(), True),
    StructField("monto", DoubleType(), True),
    StructField("moneda", StringType(), True),
    StructField("pais_origen", StringType(), True),  # Nueva columna
])

df_nuevos = spark.createDataFrame(nuevos_datos, schema_extendido)

# mergeSchema=True permite agregar la nueva columna automáticamente
df_nuevos.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("finanzas.transacciones")

print("✅ Schema evolucionado automáticamente - nueva columna 'pais_origen' agregada")

# ---------------------------------------------------------------------------
# Ejemplo 4: MERGE (Upsert) - Operación imposible en data lakes tradicionales
# ---------------------------------------------------------------------------
# Delta Lake soporta MERGE nativo. En un data lake con Parquet puro,
# necesitarías leer todo, filtrar, unir y reescribir. ¡Ineficiente!

actualizaciones = [
    ("TX001", "2026-03-10", "Compra Online", 1300.00, "USD", "USA"),      # UPDATE
    ("TX008", "2026-03-11", "Depósito", 10000.00, "USD", "USA"),          # INSERT
]

df_updates = spark.createDataFrame(actualizaciones, schema_extendido)

delta_table = DeltaTable.forName(spark, "finanzas.transacciones")

delta_table.alias("target").merge(
    df_updates.alias("source"),
    "target.transaction_id = source.transaction_id"
).whenMatchedUpdate(
    set={
        "monto": col("source.monto"),
        "pais_origen": col("source.pais_origen")
    }
).whenNotMatchedInsertAll().execute()

print("✅ MERGE completado - Upsert atómico sobre Delta Lake")

# ---------------------------------------------------------------------------
# Ejemplo 5: OPTIMIZE y Z-ORDER - Aceleración automática de queries
# ---------------------------------------------------------------------------
# Databricks compacta archivos pequeños y ordena datos para acelerar queries.
# Esto es AUTOMÁTICO en Databricks y no existe en Spark open-source.

# Compactar archivos pequeños (small file problem)
spark.sql("OPTIMIZE finanzas.transacciones")

# Z-ORDER: co-localiza datos relacionados para queries más rápidas
spark.sql("OPTIMIZE finanzas.transacciones ZORDER BY (fecha, tipo)")

print("✅ Tabla optimizada con Z-ORDER para queries ultrarrápidos")

# ---------------------------------------------------------------------------
# Ejemplo 6: Change Data Feed (CDC) - Captura automática de cambios
# ---------------------------------------------------------------------------
# Exclusivo Delta Lake: rastrea INSERT, UPDATE, DELETE automáticamente.
# Ideal para alimentar downstream systems en tiempo real.

# Habilitar CDF en la tabla
spark.sql("""
    ALTER TABLE finanzas.transacciones 
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Leer solo los cambios desde la versión 2
cambios = (spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 2)
    .table("finanzas.transacciones"))

cambios.select("transaction_id", "monto", "_change_type", "_commit_version").show()
# _change_type puede ser: insert, update_preimage, update_postimage, delete

print("✅ Change Data Feed capturando cambios incrementales automáticamente")

# ---------------------------------------------------------------------------
# Ejemplo 7: Liquid Clustering (Databricks 2024+) - Reemplaza particionamiento
# ---------------------------------------------------------------------------
# Nuevo en Databricks: clustering líquido que se adapta automáticamente
# a los patrones de query. Elimina la necesidad de elegir columnas de partición.

spark.sql("""
    CREATE TABLE IF NOT EXISTS finanzas.transacciones_v2 (
        transaction_id STRING,
        fecha DATE,
        tipo STRING,
        monto DOUBLE,
        moneda STRING
    )
    USING DELTA
    CLUSTER BY (fecha, tipo)
""")

print("✅ Liquid Clustering configurado - se adapta automáticamente a tus queries")

# ---------------------------------------------------------------------------
# Ejemplo 8: Vacuum - Limpieza de archivos obsoletos
# ---------------------------------------------------------------------------
# Delta mantiene historial pero los archivos viejos ocupan espacio.
# VACUUM elimina archivos que ya no son referenciados.

# Eliminar archivos más antiguos de 168 horas (7 días)
spark.sql("VACUUM finanzas.transacciones RETAIN 168 HOURS")

print("✅ Archivos obsoletos eliminados - storage optimizado")
