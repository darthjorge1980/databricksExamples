# =============================================================================
# DATABRICKS - DELTA SHARING, CLEAN ROOMS Y COLABORACIÓN
# =============================================================================
# Característica diferenciadora: Delta Sharing es el PRIMER protocolo abierto
# para compartir datos entre organizaciones sin copiarlos.
# Snowflake tiene Secure Sharing pero solo entre clientes Snowflake.
# Delta Sharing funciona con cualquier cliente (Pandas, Spark, Power BI, etc.)
# =============================================================================

from databricks.sdk import WorkspaceClient
import delta_sharing

# ---------------------------------------------------------------------------
# Ejemplo 1: Compartir datos con Delta Sharing (proveedor)
# ---------------------------------------------------------------------------
# Compartir tablas con clientes/socios SIN copiar datos.
# El receptor NO necesita Databricks.

w = WorkspaceClient()

# Crear un Share (grupo de tablas compartidas)
spark.sql("""
    CREATE SHARE IF NOT EXISTS share_ventas_partners
    COMMENT 'Datos de ventas compartidos con partners de negocio'
""")

# Agregar tablas al share
spark.sql("""
    ALTER SHARE share_ventas_partners 
    ADD TABLE produccion.ventas.ordenes
    COMMENT 'Órdenes de venta anonimizadas'
    -- Puedes filtrar filas y columnas
    AS SELECT 
        orden_id,
        producto,
        cantidad,
        fecha_orden,
        region
        -- Sin datos sensibles: cliente_id, precio, etc.
    FROM produccion.ventas.ordenes
    WHERE region IN ('LATAM', 'USA')
""")

# Agregar particiones específicas
spark.sql("""
    ALTER SHARE share_ventas_partners 
    ADD TABLE produccion.analytics.metricas_diarias
    PARTITION (fecha >= '2026-01-01')
""")

# Crear receptor
spark.sql("""
    CREATE RECIPIENT IF NOT EXISTS partner_analytics_inc
    COMMENT 'Partner: Analytics Inc.'
""")

# Otorgar acceso
spark.sql("""
    GRANT SELECT ON SHARE share_ventas_partners 
    TO RECIPIENT partner_analytics_inc
""")

print("✅ Delta Share creado - datos compartidos sin copiar")

# ---------------------------------------------------------------------------
# Ejemplo 2: Consumir datos de Delta Sharing (receptor)
# ---------------------------------------------------------------------------
# El receptor puede usar Python, Spark, Pandas, Power BI, Tableau...
# NO necesita Databricks ni Spark instalado.

# Opción A: Consumir con pandas (sin Spark)
import delta_sharing

profile_file = "config/partner_profile.share"
# El profile file contiene: endpoint, token, share

# Listar tablas disponibles
shares = delta_sharing.SharingClient(profile_file)
tables = shares.list_all_tables()
for table in tables:
    print(f"  {table.share}.{table.schema}.{table.name}")

# Leer como pandas DataFrame
df_pandas = delta_sharing.load_as_pandas(
    f"{profile_file}#share_ventas_partners.ventas.ordenes",
)
print(f"Registros recibidos: {len(df_pandas)}")

# Opción B: Consumir con Spark
df_spark = (spark.read
    .format("deltaSharing")
    .load(f"{profile_file}#share_ventas_partners.ventas.ordenes")
)

# Opción C: CDF (Change Data Feed) - Solo recibir cambios
df_cambios = (spark.readStream
    .format("deltaSharing")
    .option("startingVersion", 10)
    .option("readChangeFeed", "true")
    .load(f"{profile_file}#share_ventas_partners.ventas.ordenes")
)

print("✅ Datos consumidos via Delta Sharing - sin copiar, sin moverse")

# ---------------------------------------------------------------------------
# Ejemplo 3: Clean Rooms - Análisis conjunto sin exponer datos
# ---------------------------------------------------------------------------
# EXCLUSIVO Databricks: Dos empresas analizan datos juntos SIN ver los
# datos del otro. Solo ven resultados agregados.
# Snowflake tiene Clean Rooms pero con menos flexibilidad.

# Empresa A: Retailer (tiene datos de compras)
# Empresa B: Publisher (tiene datos de impresiones de ads)

# Crear Clean Room
spark.sql("""
    CREATE CLEAN ROOM IF NOT EXISTS clean_room_retail_ads
    COMMENT 'Análisis conjunto Retailer + Publisher'
""")

# Empresa A agrega sus datos al clean room
spark.sql("""
    ALTER CLEAN ROOM clean_room_retail_ads
    ADD TABLE retailer.ventas.compras_anonimizadas
    WITH COLUMNS (
        hashed_email,    -- Email hasheado para matching
        fecha_compra,
        monto,
        categoria_producto
    )
""")

# Definir notebook de análisis permitido
# Solo se pueden ejecutar queries pre-aprobadas
spark.sql("""
    ALTER CLEAN ROOM clean_room_retail_ads
    ADD NOTEBOOK '/clean_rooms/analisis_conversion.py'
    COMMENT 'Mide conversión de ads a compras'
""")

# El notebook aprobado ejecuta:
clean_room_result = spark.sql("""
    -- Este query se ejecuta DENTRO del clean room
    -- Ninguna empresa ve los datos crudos de la otra
    SELECT 
        p.campaign_name,
        p.ad_format,
        COUNT(DISTINCT r.hashed_email) AS usuarios_convertidos,
        SUM(r.monto) AS revenue_atribuible,
        SUM(r.monto) / NULLIF(SUM(p.ad_spend), 0) AS roas
    FROM retailer_data r  -- Datos del retailer
    JOIN publisher_data p  -- Datos del publisher
        ON r.hashed_email = p.hashed_email
        AND r.fecha_compra BETWEEN p.impression_date AND p.impression_date + INTERVAL 7 DAYS
    GROUP BY p.campaign_name, p.ad_format
    HAVING COUNT(DISTINCT r.hashed_email) >= 10  -- K-anonymity
""")

# Solo se ven resultados agregados, nunca datos individuales
print("✅ Clean Room: análisis conjunto preservando privacidad")

# ---------------------------------------------------------------------------
# Ejemplo 4: Marketplace de Datos de Databricks
# ---------------------------------------------------------------------------
# Comprar/vender datos a través del Marketplace integrado.
# Similar a Snowflake Marketplace pero con datos abiertos (Delta Sharing).

# Descubrir datasets en el Marketplace
# (normalmente desde la UI de Databricks)

# Instalar un dataset del marketplace
spark.sql("""
    CREATE CATALOG IF NOT EXISTS marketplace_weather
    USING SHARE `databricks_marketplace`.`weather_data`
""")

# Usar datos del marketplace en tus análisis
spark.sql("""
    SELECT 
        v.fecha_venta,
        v.region,
        SUM(v.monto_total) AS ventas,
        w.temperatura_max,
        w.precipitacion_mm,
        CORR(v.monto_total, w.temperatura_max) 
            OVER (PARTITION BY v.region ORDER BY v.fecha_venta ROWS 30 PRECEDING) 
            AS correlacion_temp_ventas
    FROM produccion.ventas.ventas v
    JOIN marketplace_weather.public.datos_diarios w 
        ON v.fecha_venta = w.fecha AND v.region = w.region
    WHERE v.fecha_venta >= '2026-01-01'
""").show()

print("✅ Datos del Marketplace integrados con datos internos")

# ---------------------------------------------------------------------------
# Ejemplo 5: Colaboración en notebooks
# ---------------------------------------------------------------------------
# Múltiples usuarios editando el mismo notebook en tiempo real.
# Similar a Google Docs pero para código.

# Notebooks multi-lenguaje en el mismo notebook:

# %python
df = spark.sql("SELECT * FROM produccion.ventas.ordenes LIMIT 10")
df.display()

# %sql
# SELECT COUNT(*), region FROM produccion.ventas.ordenes GROUP BY region

# %r
# library(SparkR)
# df <- sql("SELECT * FROM produccion.ventas.ordenes")
# head(df)

# %scala
# val df = spark.table("produccion.ventas.ordenes")
# df.groupBy("region").count().show()

# %md
# ## Análisis de Ventas Q1 2026
# Este notebook analiza las tendencias de venta por región...

# Comentarios y revisión de código directamente en el notebook
# Historial de versiones automático

print("✅ Notebooks colaborativos multi-lenguaje")

# ---------------------------------------------------------------------------
# Ejemplo 6: Secrets y gestión de credenciales
# ---------------------------------------------------------------------------
# Databricks Secret Scopes protegen credenciales.
# Integración con Azure Key Vault, AWS Secrets Manager, HashiCorp Vault.

# Crear scope de secrets
# databricks secrets create-scope --scope mi-scope

# Guardar secrets
# databricks secrets put --scope mi-scope --key db-password

# Usar secrets en código (NUNCA se muestran en logs)
db_password = dbutils.secrets.get(scope="mi-scope", key="db-password")
api_key = dbutils.secrets.get(scope="mi-scope", key="api-key")

# En queries SQL
spark.sql("""
    CREATE CONNECTION IF NOT EXISTS mysql_crm
    TYPE MYSQL
    OPTIONS (
        host 'crm-db.empresa.com',
        port '3306',
        user secret('crm-scope', 'db-user'),
        password secret('crm-scope', 'db-password')
    )
""")

# Si alguien intenta imprimir el secret:
print(db_password)  # Muestra: [REDACTED] (nunca el valor real)

print("✅ Secrets configurados - credenciales protegidas")

# ---------------------------------------------------------------------------
# Ejemplo 7: Clusters personalizados con Init Scripts
# ---------------------------------------------------------------------------
# Personalizar el ambiente de ejecución con scripts de inicialización.

# Init script para instalar dependencias
init_script = """
#!/bin/bash
pip install great-expectations==0.18.0
pip install faker==22.0.0
pip install boto3
apt-get install -y libgdal-dev  # Para datos geoespaciales
"""

# Configurar cluster con init script
cluster_config = {
    "cluster_name": "analytics-team",
    "spark_version": "15.4.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 4,
    "autoscale": {
        "min_workers": 2,
        "max_workers": 10
    },
    "spark_conf": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true"
    },
    "custom_tags": {
        "equipo": "analytics",
        "costo_centro": "CC-001"
    },
    "init_scripts": [
        {"workspace": {"destination": "/Shared/init-scripts/setup.sh"}}
    ],
    "autotermination_minutes": 30,
    "enable_elastic_disk": True
}

print("✅ Cluster personalizado con init scripts y auto-scaling")

# ---------------------------------------------------------------------------
# Ejemplo 8: dbutils - Utilidades integradas de Databricks
# ---------------------------------------------------------------------------
# dbutils es un conjunto de herramientas exclusivas de Databricks
# para gestionar archivos, secrets, widgets y más.

# File System utilities
dbutils.fs.ls("/mnt/data/")
dbutils.fs.cp("/mnt/data/source.csv", "/mnt/data/backup/source.csv")
dbutils.fs.rm("/mnt/data/temp/", recurse=True)
dbutils.fs.mkdirs("/mnt/data/output/2026/03/")

# Montar storage externo
dbutils.fs.mount(
    source="wasbs://container@storageaccount.blob.core.windows.net/",
    mount_point="/mnt/azure_data",
    extra_configs={
        "fs.azure.account.key.storageaccount.blob.core.windows.net": 
            dbutils.secrets.get("azure-scope", "storage-key")
    }
)

# Notebook utilities
dbutils.notebook.run("./otro_notebook", timeout_seconds=600, arguments={"param1": "valor1"})
dbutils.notebook.exit("Proceso completado exitosamente")

# Widgets para parametrizar notebooks
dbutils.widgets.text("fecha_inicio", "2026-01-01")
dbutils.widgets.multiselect("regiones", "LATAM", ["LATAM", "NA", "EMEA", "APAC"])

print("✅ dbutils: filesystem, notebooks, secrets y widgets integrados")
