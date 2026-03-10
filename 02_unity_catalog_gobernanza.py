# =============================================================================
# DATABRICKS - UNITY CATALOG: GOBERNANZA UNIFICADA DE DATOS
# =============================================================================
# Característica diferenciadora: Unity Catalog es el ÚNICO sistema de gobernanza
# que unifica datos, ML models, dashboards y archivos bajo un solo namespace
# con control de acceso granular (row-level, column-level).
# Snowflake tiene gobernanza pero no cubre ML models ni notebooks.
# AWS Glue Catalog no tiene lineage automático ni auditoría integrada.
# =============================================================================

# ---------------------------------------------------------------------------
# Ejemplo 1: Namespace jerárquico de 3 niveles
# ---------------------------------------------------------------------------
# Unity Catalog organiza: CATALOG > SCHEMA > TABLE/VIEW/FUNCTION
# Similar a un sistema de archivos para TODOS tus assets de datos.

# Crear catálogos para diferentes entornos/dominios
spark.sql("CREATE CATALOG IF NOT EXISTS produccion")
spark.sql("CREATE CATALOG IF NOT EXISTS desarrollo")
spark.sql("CREATE CATALOG IF NOT EXISTS sandbox")

# Crear schemas dentro del catálogo
spark.sql("CREATE SCHEMA IF NOT EXISTS produccion.ventas")
spark.sql("CREATE SCHEMA IF NOT EXISTS produccion.rrhh")
spark.sql("CREATE SCHEMA IF NOT EXISTS produccion.finanzas")

# Crear tabla en el namespace completo
spark.sql("""
    CREATE TABLE IF NOT EXISTS produccion.ventas.ordenes (
        orden_id BIGINT GENERATED ALWAYS AS IDENTITY,
        cliente_id BIGINT,
        producto STRING,
        cantidad INT,
        precio_unitario DECIMAL(10,2),
        total DECIMAL(12,2) GENERATED ALWAYS AS (cantidad * precio_unitario),
        fecha_orden TIMESTAMP DEFAULT current_timestamp(),
        region STRING
    )
    USING DELTA
    COMMENT 'Tabla maestra de órdenes de venta'
""")

print("✅ Namespace jerárquico creado: produccion.ventas.ordenes")

# ---------------------------------------------------------------------------
# Ejemplo 2: Control de acceso granular (Row & Column Level Security)
# ---------------------------------------------------------------------------
# Unity Catalog permite controlar acceso a nivel de fila y columna.
# Más granular que IAM roles en AWS o ACLs en Azure.

# Otorgar permisos a nivel de catálogo
spark.sql("GRANT USE CATALOG ON CATALOG produccion TO `data_engineers`")
spark.sql("GRANT USE SCHEMA ON SCHEMA produccion.ventas TO `data_engineers`")
spark.sql("GRANT SELECT ON TABLE produccion.ventas.ordenes TO `data_analysts`")

# Column-level security: crear vista que oculta columnas sensibles
spark.sql("""
    CREATE OR REPLACE VIEW produccion.ventas.ordenes_publicas AS
    SELECT 
        orden_id,
        producto,
        cantidad,
        region,
        fecha_orden
        -- precio_unitario y total están OCULTOS para analistas junior
    FROM produccion.ventas.ordenes
""")
spark.sql("GRANT SELECT ON VIEW produccion.ventas.ordenes_publicas TO `junior_analysts`")

# Row-level security: filtrar datos según el usuario
spark.sql("""
    CREATE OR REPLACE VIEW produccion.ventas.ordenes_por_region AS
    SELECT *
    FROM produccion.ventas.ordenes
    WHERE region = current_user_region()
    -- Cada usuario solo ve datos de su región
""")

# Dynamic Views para row-level security
spark.sql("""
    CREATE OR REPLACE VIEW produccion.ventas.ordenes_seguras AS
    SELECT 
        orden_id,
        cliente_id,
        producto,
        cantidad,
        CASE 
            WHEN is_member('finance_team') THEN precio_unitario
            ELSE NULL 
        END AS precio_unitario,
        CASE 
            WHEN is_member('finance_team') THEN total
            ELSE NULL 
        END AS total,
        fecha_orden,
        region
    FROM produccion.ventas.ordenes
""")

print("✅ Seguridad a nivel de fila y columna configurada")

# ---------------------------------------------------------------------------
# Ejemplo 3: Data Lineage automático
# ---------------------------------------------------------------------------
# Unity Catalog rastrea AUTOMÁTICAMENTE de dónde vienen los datos y a dónde van.
# No necesitas herramientas externas como Apache Atlas o dbt lineage.

# Al ejecutar esto, Unity Catalog registra el lineage automáticamente
spark.sql("""
    CREATE OR REPLACE TABLE produccion.ventas.resumen_mensual AS
    SELECT 
        date_trunc('month', fecha_orden) AS mes,
        region,
        COUNT(*) AS total_ordenes,
        SUM(total) AS ingreso_total,
        AVG(total) AS ticket_promedio
    FROM produccion.ventas.ordenes
    GROUP BY 1, 2
""")

# El lineage muestra:
# ordenes -> resumen_mensual (automático, sin configuración)
# Puedes ver esto en la UI de Databricks: Catalog Explorer > Lineage tab

print("✅ Lineage capturado automáticamente al crear resumen_mensual")

# ---------------------------------------------------------------------------
# Ejemplo 4: Volumes - Gestionar archivos no estructurados
# ---------------------------------------------------------------------------
# Exclusivo de Unity Catalog: gestionar archivos (CSV, JSON, imágenes, PDFs)
# con la misma gobernanza que tablas. Snowflake no tiene esto.

# Crear un Volume para archivos raw
spark.sql("""
    CREATE VOLUME IF NOT EXISTS produccion.ventas.archivos_raw
    COMMENT 'Archivos fuente de ventas: CSVs, JSONs, Excel'
""")

# Crear un Volume externo apuntando a cloud storage
spark.sql("""
    CREATE EXTERNAL VOLUME IF NOT EXISTS produccion.ventas.archivos_externos
    LOCATION 's3://mi-bucket/ventas/raw/'
    COMMENT 'Archivos en S3 con gobernanza Unity Catalog'
""")

# Acceder a archivos dentro del Volume
df_csv = spark.read.csv(
    "/Volumes/produccion/ventas/archivos_raw/ventas_2026.csv",
    header=True,
    inferSchema=True
)

# Los mismos permisos de Unity Catalog aplican a los archivos
spark.sql("GRANT READ VOLUME ON VOLUME produccion.ventas.archivos_raw TO `etl_service`")

print("✅ Volumes configurados - archivos gobernados como tablas")

# ---------------------------------------------------------------------------
# Ejemplo 5: Tags y clasificación de datos
# ---------------------------------------------------------------------------
# Etiquetar datos sensibles para cumplimiento (GDPR, HIPAA, etc.)

# Crear tags a nivel de tabla
spark.sql("""
    ALTER TABLE produccion.ventas.ordenes 
    SET TAGS ('clasificacion' = 'confidencial', 'dominio' = 'ventas')
""")

# Tags a nivel de columna (ideal para PII)
spark.sql("""
    ALTER TABLE produccion.ventas.ordenes 
    ALTER COLUMN cliente_id SET TAGS ('pii' = 'true', 'gdpr' = 'requiere_consentimiento')
""")

# Buscar todas las tablas con datos PII
spark.sql("""
    SELECT table_name, column_name, tag_name, tag_value
    FROM system.information_schema.column_tags
    WHERE tag_name = 'pii' AND tag_value = 'true'
""").show()

print("✅ Tags y clasificación configurados para cumplimiento regulatorio")

# ---------------------------------------------------------------------------
# Ejemplo 6: Functions como ciudadanos de primera clase
# ---------------------------------------------------------------------------
# Registrar UDFs en Unity Catalog para compartirlas entre equipos.

spark.sql("""
    CREATE OR REPLACE FUNCTION produccion.ventas.calcular_iva(
        monto DECIMAL(12,2), 
        pais STRING
    )
    RETURNS DECIMAL(12,2)
    LANGUAGE SQL
    DETERMINISTIC
    COMMENT 'Calcula IVA según el país'
    RETURN CASE 
        WHEN pais = 'Mexico' THEN monto * 0.16
        WHEN pais = 'España' THEN monto * 0.21
        WHEN pais = 'Colombia' THEN monto * 0.19
        WHEN pais = 'Chile' THEN monto * 0.19
        WHEN pais = 'Argentina' THEN monto * 0.21
        ELSE monto * 0.15
    END
""")

# Usar la función en cualquier query
spark.sql("""
    SELECT 
        producto, 
        total,
        produccion.ventas.calcular_iva(total, region) AS iva,
        total + produccion.ventas.calcular_iva(total, region) AS total_con_iva
    FROM produccion.ventas.ordenes
""").show()

print("✅ UDF registrada en Unity Catalog - compartida entre todos los equipos")

# ---------------------------------------------------------------------------
# Ejemplo 7: Auditoría automática de acceso a datos
# ---------------------------------------------------------------------------
# Unity Catalog registra CADA acceso a datos automáticamente.
# Consultar quién accedió qué datos y cuándo.

spark.sql("""
    SELECT 
        event_time,
        user_identity.email AS usuario,
        action_name,
        request_params.full_name_arg AS recurso,
        response.status_code
    FROM system.access.audit
    WHERE action_name IN ('getTable', 'commandSubmit', 'createTable')
    AND event_date >= '2026-03-01'
    ORDER BY event_time DESC
    LIMIT 100
""").show(truncate=False)

print("✅ Auditoría completa disponible - cumplimiento SOX, GDPR, HIPAA")

# ---------------------------------------------------------------------------
# Ejemplo 8: Federated Queries - Consultar bases de datos externas
# ---------------------------------------------------------------------------
# Conectar MySQL, PostgreSQL, SQL Server directamente desde Databricks
# sin mover los datos. Snowflake requiere cargar los datos primero.

# Crear conexión a PostgreSQL externo
spark.sql("""
    CREATE CONNECTION IF NOT EXISTS postgres_erp
    TYPE POSTGRESQL
    OPTIONS (
        host 'erp-db.empresa.com',
        port '5432',
        user secret('erp-scope', 'db-user'),
        password secret('erp-scope', 'db-password')
    )
""")

# Crear catálogo federado
spark.sql("""
    CREATE FOREIGN CATALOG IF NOT EXISTS erp_produccion
    USING CONNECTION postgres_erp
    OPTIONS (database 'erp_prod')
""")

# Hacer JOIN entre datos locales y externos SIN mover datos
spark.sql("""
    SELECT 
        o.orden_id,
        o.producto,
        o.total,
        c.nombre_empresa,
        c.email
    FROM produccion.ventas.ordenes o
    JOIN erp_produccion.public.clientes c 
        ON o.cliente_id = c.id
""").show()

print("✅ Federated Query ejecutada - datos de PostgreSQL consultados in-situ")
