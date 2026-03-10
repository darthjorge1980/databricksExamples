# =============================================================================
# DATABRICKS - STRUCTURED STREAMING & PROCESAMIENTO EN TIEMPO REAL
# =============================================================================
# Característica diferenciadora: Databricks unifica batch y streaming en un
# solo framework. Spark Structured Streaming es más robusto que Snowpipe
# (Snowflake) y más fácil que AWS Kinesis + Lambda.
# Con Delta Live Tables, los pipelines se definen declarativamente.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, count, sum as spark_sum,
    current_timestamp, expr, to_timestamp, watermark
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, IntegerType, LongType
)

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Ejemplo 1: Streaming desde Kafka con Structured Streaming
# ---------------------------------------------------------------------------
# Lee datos en tiempo real de Kafka y escribe en Delta Lake.
# Todo con exactamente-una-vez (exactly-once) garantizado.

schema_evento = StructType([
    StructField("evento_id", StringType(), False),
    StructField("usuario_id", LongType(), False),
    StructField("tipo_evento", StringType(), True),
    StructField("pagina", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("dispositivo", StringType(), True),
    StructField("valor", DoubleType(), True),
])

# Leer stream desde Kafka
df_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-broker:9092")
    .option("subscribe", "eventos_web")
    .option("startingOffsets", "latest")
    .option("kafka.security.protocol", "SASL_SSL")
    .load()
    .select(
        from_json(col("value").cast("string"), schema_evento).alias("data")
    )
    .select("data.*")
)

# Escribir en Delta Lake con exactly-once semantics
query = (df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/eventos_web")
    .trigger(processingTime="10 seconds")
    .toTable("produccion.analytics.eventos_web_raw")
)

print("✅ Streaming Kafka -> Delta Lake con exactly-once semantics")

# ---------------------------------------------------------------------------
# Ejemplo 2: Agregaciones en ventanas de tiempo (Window Aggregations)
# ---------------------------------------------------------------------------
# Calcular métricas en tiempo real con ventanas deslizantes.
# Snowflake no puede hacer esto en tiempo real, solo en batch.

# Métricas por ventana de 5 minutos, deslizando cada 1 minuto
df_metricas = (df_stream
    .withWatermark("timestamp", "10 minutes")
    .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("tipo_evento")
    )
    .agg(
        count("*").alias("total_eventos"),
        avg("valor").alias("valor_promedio"),
        spark_sum("valor").alias("valor_total")
    )
)

# Escribir métricas en tiempo real
query_metricas = (df_metricas.writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", "/checkpoints/metricas_5min")
    .trigger(processingTime="30 seconds")
    .toTable("produccion.analytics.metricas_tiempo_real")
)

print("✅ Agregaciones en ventanas de tiempo corriendo en streaming")

# ---------------------------------------------------------------------------
# Ejemplo 3: Stream-Stream Join - Unir dos streams en tiempo real
# ---------------------------------------------------------------------------
# Unir eventos de clicks con eventos de compra en tiempo real.
# Esto NO es posible en Snowflake ni Redshift.

schema_compra = StructType([
    StructField("compra_id", StringType(), False),
    StructField("usuario_id", LongType(), False),
    StructField("producto", StringType(), True),
    StructField("monto", DoubleType(), True),
    StructField("timestamp_compra", TimestampType(), True),
])

# Segundo stream de compras
df_compras = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-broker:9092")
    .option("subscribe", "compras")
    .load()
    .select(from_json(col("value").cast("string"), schema_compra).alias("data"))
    .select("data.*")
    .withWatermark("timestamp_compra", "1 hour")
)

df_clicks = (df_stream
    .filter(col("tipo_evento") == "click_producto")
    .withWatermark("timestamp", "1 hour")
)

# JOIN entre dos streams con ventana temporal
df_conversion = df_clicks.join(
    df_compras,
    expr("""
        clicks.usuario_id = compras.usuario_id AND
        compras.timestamp_compra >= clicks.timestamp AND
        compras.timestamp_compra <= clicks.timestamp + interval 30 minutes
    """),
    "leftOuter"
)

print("✅ Stream-Stream Join configurado - análisis de conversión en tiempo real")

# ---------------------------------------------------------------------------
# Ejemplo 4: foreachBatch - Lógica personalizada por micro-batch
# ---------------------------------------------------------------------------
# Ejecutar cualquier lógica (incluyendo MERGE) en cada micro-batch.

def procesar_microbatch(batch_df, batch_id):
    """Procesa cada micro-batch con lógica de upsert."""
    from delta.tables import DeltaTable
    
    if batch_df.isEmpty():
        return
    
    delta_table = DeltaTable.forName(spark, "produccion.analytics.usuarios_activos")
    
    # MERGE: actualizar usuarios existentes, insertar nuevos
    delta_table.alias("target").merge(
        batch_df.alias("source"),
        "target.usuario_id = source.usuario_id"
    ).whenMatchedUpdate(
        set={
            "ultimo_evento": col("source.timestamp"),
            "total_eventos": col("target.total_eventos") + 1,
            "ultimo_dispositivo": col("source.dispositivo")
        }
    ).whenNotMatchedInsert(
        values={
            "usuario_id": col("source.usuario_id"),
            "primer_evento": col("source.timestamp"),
            "ultimo_evento": col("source.timestamp"),
            "total_eventos": 1,
            "ultimo_dispositivo": col("source.dispositivo")
        }
    ).execute()
    
    print(f"   Micro-batch {batch_id}: {batch_df.count()} registros procesados")

# Usar foreachBatch para lógica de MERGE en streaming
query_upsert = (df_stream.writeStream
    .foreachBatch(procesar_microbatch)
    .option("checkpointLocation", "/checkpoints/usuarios_activos")
    .trigger(processingTime="1 minute")
    .start()
)

print("✅ Streaming con MERGE (upsert) usando foreachBatch")

# ---------------------------------------------------------------------------
# Ejemplo 5: Auto Loader - Ingestión incremental de archivos
# ---------------------------------------------------------------------------
# EXCLUSIVO de Databricks: lee automáticamente archivos nuevos que llegan
# a cloud storage. Más eficiente que COPY INTO y que Snowpipe.
# Soporta millones de archivos con schema inference automática.

# Auto Loader con cloudFiles
df_autoloader = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/schemas/logs_app")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    # Rescata columnas que no encajan en el schema
    .option("cloudFiles.schemaHints", "timestamp TIMESTAMP, user_id LONG")
    .load("s3://mi-bucket/logs/aplicacion/")
)

# Escribir en Delta Lake con schema evolution automática
query_autoloader = (df_autoloader.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/logs_app")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)  # Procesa todo lo pendiente y se detiene
    .toTable("produccion.analytics.logs_aplicacion")
)

print("✅ Auto Loader configurado - ingestión automática de archivos nuevos")

# ---------------------------------------------------------------------------
# Ejemplo 6: Streaming con múltiples sinks (fan-out)
# ---------------------------------------------------------------------------
# Un solo stream alimenta múltiples destinos simultáneamente.

# Sink 1: Delta Lake para analytics
query_delta = (df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/eventos_delta")
    .toTable("produccion.analytics.todos_eventos")
)

# Sink 2: Kafka para notificaciones en tiempo real
eventos_criticos = df_stream.filter(
    (col("tipo_evento") == "error") | (col("valor") > 10000)
)

query_kafka = (eventos_criticos.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-broker:9092")
    .option("topic", "alertas_criticas")
    .option("checkpointLocation", "/checkpoints/alertas_kafka")
    .start()
)

# Sink 3: Tabla agregada para dashboards
query_dashboard = (df_metricas.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", "/checkpoints/dashboard")
    .toTable("produccion.analytics.dashboard_metricas")
)

print("✅ Fan-out: un stream alimentando Delta Lake, Kafka y dashboards")

# ---------------------------------------------------------------------------
# Ejemplo 7: Delta Live Tables (DLT) - Pipelines declarativos
# ---------------------------------------------------------------------------
# EXCLUSIVO de Databricks: defines QUÉ quieres, no CÓMO hacerlo.
# DLT maneja automáticamente: dependencias, calidad de datos, retry, scaling.
# Es como dbt pero para batch Y streaming juntos.

import dlt  # Delta Live Tables module

# Capa Bronze: datos RAW tal cual llegan
@dlt.table(
    name="eventos_raw",
    comment="Eventos web sin procesar desde Kafka",
    table_properties={"quality": "bronze"}
)
def eventos_raw():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("s3://mi-bucket/eventos/")
    )

# Capa Silver: datos limpios y validados con EXPECTATIONS
@dlt.table(
    name="eventos_limpios",
    comment="Eventos validados y enriquecidos"
)
@dlt.expect_or_drop("evento_id_valido", "evento_id IS NOT NULL")
@dlt.expect_or_fail("timestamp_valido", "timestamp > '2020-01-01'")
@dlt.expect("valor_positivo", "valor >= 0")  # Solo marca, no descarta
def eventos_limpios():
    return (dlt.read_stream("eventos_raw")
        .filter(col("tipo_evento").isNotNull())
        .withColumn("fecha", col("timestamp").cast("date"))
        .withColumn("hora", expr("hour(timestamp)"))
    )

# Capa Gold: agregaciones de negocio
@dlt.table(
    name="metricas_diarias",
    comment="KPIs diarios por tipo de evento"
)
def metricas_diarias():
    return (dlt.read("eventos_limpios")
        .groupBy("fecha", "tipo_evento")
        .agg(
            count("*").alias("total"),
            spark_sum("valor").alias("valor_total"),
            avg("valor").alias("valor_promedio")
        )
    )

# Materialized View (se actualiza incrementalmente)
@dlt.table(
    name="top_usuarios",
    comment="Top 100 usuarios más activos - actualización incremental"
)
def top_usuarios():
    return (dlt.read("eventos_limpios")
        .groupBy("usuario_id")
        .agg(
            count("*").alias("total_eventos"),
            spark_sum("valor").alias("valor_total")
        )
        .orderBy(col("total_eventos").desc())
        .limit(100)
    )

print("✅ Delta Live Tables: pipeline Bronze -> Silver -> Gold declarativo")

# ---------------------------------------------------------------------------
# Ejemplo 8: Monitorear y gestionar streams
# ---------------------------------------------------------------------------

# Ver todos los streams activos
for stream in spark.streams.active:
    print(f"Stream: {stream.name}")
    print(f"  Status: {stream.status}")
    print(f"  ID: {stream.id}")
    print(f"  Progress: {stream.lastProgress}")

# Detener un stream específico
# query.stop()

# Métricas del stream disponibles en la UI de Databricks:
# - Input rate (registros/segundo)
# - Processing rate
# - Batch duration
# - Estado del watermark

print("✅ Monitoreo de streams configurado")
