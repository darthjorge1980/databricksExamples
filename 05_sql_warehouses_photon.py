# =============================================================================
# DATABRICKS - SQL WAREHOUSES, PHOTON ENGINE Y ANALYTICS
# =============================================================================
# Característica diferenciadora: Photon es el motor C++ nativo de Databricks
# que acelera queries SQL hasta 12x vs Spark estándar. Los SQL Warehouses
# ofrecen experiencia tipo data warehouse con costos de data lake.
# A diferencia de Snowflake, pagas solo por lo que usas (serverless).
# =============================================================================

# ---------------------------------------------------------------------------
# Ejemplo 1: SQL Warehouse Serverless - Analytics instantáneo
# ---------------------------------------------------------------------------
# SQL Warehouses arrancan en segundos y escalan automáticamente.
# No necesitas gestionar clusters como en EMR o Dataproc.

-- Crear SQL Warehouse desde SQL (también se puede desde UI/API)
-- Los SQL Warehouses son endpoints de compute optimizados para SQL

-- Query analítico complejo que Photon acelera automáticamente
SELECT 
    d.region,
    d.pais,
    p.categoria,
    p.subcategoria,
    DATE_TRUNC('month', v.fecha_venta) AS mes,
    COUNT(DISTINCT v.cliente_id) AS clientes_unicos,
    COUNT(*) AS num_transacciones,
    SUM(v.cantidad) AS unidades_vendidas,
    SUM(v.monto_total) AS ingreso_bruto,
    SUM(v.descuento) AS total_descuentos,
    SUM(v.monto_total - v.descuento) AS ingreso_neto,
    AVG(v.monto_total) AS ticket_promedio,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v.monto_total) AS mediana_venta,
    SUM(v.monto_total) / COUNT(DISTINCT v.cliente_id) AS arpu
FROM produccion.ventas.ventas v
JOIN produccion.ventas.productos p ON v.producto_id = p.producto_id
JOIN produccion.ventas.dimensiones_geo d ON v.geo_id = d.geo_id
WHERE v.fecha_venta >= '2025-01-01'
GROUP BY GROUPING SETS (
    (d.region, d.pais, p.categoria, p.subcategoria, DATE_TRUNC('month', v.fecha_venta)),
    (d.region, d.pais, p.categoria, DATE_TRUNC('month', v.fecha_venta)),
    (d.region, p.categoria, DATE_TRUNC('month', v.fecha_venta)),
    (p.categoria, DATE_TRUNC('month', v.fecha_venta)),
    (DATE_TRUNC('month', v.fecha_venta))
)
ORDER BY mes DESC, ingreso_neto DESC;

-- ✅ Photon ejecuta esto hasta 12x más rápido que Spark SQL estándar

# ---------------------------------------------------------------------------
# Ejemplo 2: Materialized Views - Resultados pre-computados
# ---------------------------------------------------------------------------
# Databricks Materialized Views se actualizan INCREMENTALMENTE.
# Snowflake tiene MVs pero no con actualización incremental inteligente.

spark.sql("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS produccion.analytics.dashboard_ventas
    COMMENT 'KPIs de ventas actualizados incrementalmente'
    SCHEDULE CRON '0 */15 * * * ?'
    AS
    SELECT 
        fecha_venta AS fecha,
        region,
        COUNT(*) AS num_ventas,
        SUM(monto_total) AS ingreso_total,
        AVG(monto_total) AS ticket_promedio,
        COUNT(DISTINCT cliente_id) AS clientes_unicos,
        SUM(CASE WHEN es_primera_compra THEN 1 ELSE 0 END) AS nuevos_clientes,
        SUM(monto_total) / NULLIF(COUNT(DISTINCT cliente_id), 0) AS revenue_per_user
    FROM produccion.ventas.ventas
    GROUP BY fecha_venta, region
""")

# Refrescar manualmente si es necesario
spark.sql("REFRESH MATERIALIZED VIEW produccion.analytics.dashboard_ventas")

print("✅ Materialized View con actualización incremental cada 15 minutos")

# ---------------------------------------------------------------------------
# Ejemplo 3: Predictive Optimization - Optimización automática
# ---------------------------------------------------------------------------
# EXCLUSIVO Databricks: el sistema detecta patrones de queries y
# automáticamente ejecuta OPTIMIZE, VACUUM, y Z-ORDER.
# No necesitas DBAs ni scripts de mantenimiento.

spark.sql("""
    ALTER TABLE produccion.ventas.ventas
    SET TBLPROPERTIES (
        'delta.enableDeletionVectors' = true,
        'delta.tuneFileSizesForRewrites' = true
    )
""")

# Habilitar Predictive Optimization a nivel de schema
spark.sql("""
    ALTER SCHEMA produccion.ventas 
    ENABLE PREDICTIVE OPTIMIZATION
""")

# Databricks automáticamente:
# 1. Compacta archivos pequeños (OPTIMIZE)
# 2. Limpia archivos obsoletos (VACUUM)  
# 3. Aplica Z-ORDER en columnas frecuentes en filtros
# 4. Actualiza estadísticas para el optimizador de queries

print("✅ Predictive Optimization habilitado - zero-maintenance")

# ---------------------------------------------------------------------------
# Ejemplo 4: Databricks SQL + Python en el mismo notebook
# ---------------------------------------------------------------------------
# A diferencia de Snowflake donde SQL y Python están separados,
# en Databricks puedes mezclarlos fluidamente.

# Python: preparar datos
import pandas as pd
import plotly.express as px

# Ejecutar SQL y obtener resultado como DataFrame
df_tendencias = spark.sql("""
    SELECT 
        DATE_TRUNC('week', fecha_venta) AS semana,
        categoria,
        SUM(monto_total) AS ingresos,
        LAG(SUM(monto_total)) OVER (
            PARTITION BY categoria ORDER BY DATE_TRUNC('week', fecha_venta)
        ) AS ingresos_semana_anterior,
        (SUM(monto_total) - LAG(SUM(monto_total)) OVER (
            PARTITION BY categoria ORDER BY DATE_TRUNC('week', fecha_venta)
        )) / NULLIF(LAG(SUM(monto_total)) OVER (
            PARTITION BY categoria ORDER BY DATE_TRUNC('week', fecha_venta)
        ), 0) * 100 AS crecimiento_pct
    FROM produccion.ventas.ventas v
    JOIN produccion.ventas.productos p ON v.producto_id = p.producto_id
    WHERE fecha_venta >= '2026-01-01'
    GROUP BY 1, 2
    ORDER BY semana, categoria
""").toPandas()

# Visualización directa en el notebook con Plotly
fig = px.line(
    df_tendencias,
    x="semana",
    y="ingresos",
    color="categoria",
    title="Tendencia de Ingresos por Categoría - 2026",
    labels={"ingresos": "Ingresos ($)", "semana": "Semana"}
)
fig.show()

# Gráfico de crecimiento
fig2 = px.bar(
    df_tendencias.dropna(),
    x="semana",
    y="crecimiento_pct",
    color="categoria",
    barmode="group",
    title="Crecimiento Semanal (%)"
)
fig2.show()

print("✅ SQL + Python + Visualización en el mismo notebook")

# ---------------------------------------------------------------------------
# Ejemplo 5: Query Federation - Consultar fuentes externas en SQL
# ---------------------------------------------------------------------------
# Consultar MySQL, PostgreSQL, BigQuery, Redshift directamente desde SQL.
# Los datos NO se mueven - se consultan in-situ.

spark.sql("""
    -- Combinar datos del lakehouse con PostgreSQL en tiempo real
    WITH ventas_lake AS (
        SELECT cliente_id, SUM(monto_total) AS total_ventas
        FROM produccion.ventas.ventas
        WHERE fecha_venta >= '2026-01-01'
        GROUP BY cliente_id
    ),
    clientes_erp AS (
        SELECT id, nombre, segmento, credito_disponible
        FROM erp_postgres.public.clientes
        WHERE activo = true
    )
    SELECT 
        c.nombre,
        c.segmento,
        v.total_ventas,
        c.credito_disponible,
        v.total_ventas / NULLIF(c.credito_disponible, 0) AS ratio_uso_credito
    FROM ventas_lake v
    JOIN clientes_erp c ON v.cliente_id = c.id
    WHERE v.total_ventas > 10000
    ORDER BY ratio_uso_credito DESC
""")

print("✅ Query Federation: datos del lake + PostgreSQL sin mover datos")

# ---------------------------------------------------------------------------
# Ejemplo 6: Dashboards nativos en Databricks SQL
# ---------------------------------------------------------------------------
# Crear dashboards directamente en Databricks sin Tableau/PowerBI.
# Se ejecutan sobre SQL Warehouses optimizados con Photon.

# Este SQL alimenta un widget de dashboard
spark.sql("""
    -- Widget: Embudo de conversión
    SELECT 
        etapa,
        usuarios,
        LAG(usuarios) OVER (ORDER BY orden_etapa) AS etapa_anterior,
        ROUND(usuarios * 100.0 / FIRST_VALUE(usuarios) OVER (ORDER BY orden_etapa), 2) 
            AS tasa_conversion_total,
        ROUND(usuarios * 100.0 / NULLIF(LAG(usuarios) OVER (ORDER BY orden_etapa), 0), 2) 
            AS tasa_conversion_etapa
    FROM (
        SELECT 'Visitantes' AS etapa, 1 AS orden_etapa, COUNT(DISTINCT session_id) AS usuarios
        FROM produccion.analytics.eventos_web_raw WHERE fecha = current_date()
        UNION ALL
        SELECT 'Vieron Producto', 2, COUNT(DISTINCT session_id) 
        FROM produccion.analytics.eventos_web_raw 
        WHERE tipo_evento = 'product_view' AND fecha = current_date()
        UNION ALL
        SELECT 'Agregaron al Carrito', 3, COUNT(DISTINCT session_id)
        FROM produccion.analytics.eventos_web_raw 
        WHERE tipo_evento = 'add_to_cart' AND fecha = current_date()
        UNION ALL
        SELECT 'Iniciaron Checkout', 4, COUNT(DISTINCT session_id)
        FROM produccion.analytics.eventos_web_raw 
        WHERE tipo_evento = 'begin_checkout' AND fecha = current_date()
        UNION ALL
        SELECT 'Compraron', 5, COUNT(DISTINCT session_id)
        FROM produccion.analytics.eventos_web_raw 
        WHERE tipo_evento = 'purchase' AND fecha = current_date()
    )
    ORDER BY orden_etapa
""")

print("✅ Dashboard SQL nativo con embudo de conversión")

# ---------------------------------------------------------------------------
# Ejemplo 7: Query Profile y Performance Insights
# ---------------------------------------------------------------------------
# Databricks muestra un perfil detallado de cada query para optimización.
# Más detallado que EXPLAIN en cualquier otro sistema.

# Ver plan de ejecución detallado
spark.sql("""
    EXPLAIN FORMATTED
    SELECT /*+ BROADCAST(p) */
        d.region,
        p.categoria,
        SUM(v.monto_total) AS ingresos
    FROM produccion.ventas.ventas v
    JOIN produccion.ventas.productos p ON v.producto_id = p.producto_id
    JOIN produccion.ventas.dimensiones_geo d ON v.geo_id = d.geo_id  
    WHERE v.fecha_venta BETWEEN '2026-01-01' AND '2026-03-10'
    GROUP BY d.region, p.categoria
""").show(truncate=False)

# Estadísticas de tabla para el optimizador
spark.sql("ANALYZE TABLE produccion.ventas.ventas COMPUTE STATISTICS FOR ALL COLUMNS")

print("✅ Query optimizado con hints y estadísticas actualizadas")

# ---------------------------------------------------------------------------
# Ejemplo 8: Serverless compute - Escalado automático
# ---------------------------------------------------------------------------
# SQL Warehouses serverless escalan de 0 a N automáticamente.
# Pagas SOLO por los queries que ejecutas, no por tiempo de cluster.

# Configurar warehouse vía API
warehouse_config = {
    "name": "analytics-warehouse",
    "cluster_size": "2X-Small",  # Tamaño inicial
    "min_num_clusters": 1,
    "max_num_clusters": 10,    # Escala hasta 10 clusters
    "auto_stop_mins": 15,      # Se apaga tras 15 min de inactividad
    "warehouse_type": "PRO",    # PRO = Photon habilitado
    "enable_serverless_compute": True,
    "spot_instance_policy": "COST_OPTIMIZED",
    "tags": {
        "custom_tags": [
            {"key": "equipo", "value": "analytics"},
            {"key": "ambiente", "value": "produccion"}
        ]
    }
}

# En Serverless:
# - Arranque en ~10 segundos (vs minutos en EMR/Dataproc)
# - Escalado automático por concurrencia de queries
# - Scale-to-zero cuando no hay queries
# - Sin gestión de infraestructura

print("✅ SQL Warehouse Serverless configurado con auto-scaling")
