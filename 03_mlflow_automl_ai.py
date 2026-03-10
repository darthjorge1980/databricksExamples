# =============================================================================
# DATABRICKS - MLFLOW, AutoML Y FEATURE STORE
# =============================================================================
# Característica diferenciadora: Databricks integra MLflow (creado por ellos),
# AutoML, Feature Store y Model Serving en una plataforma unificada.
# En AWS necesitarías SageMaker + Feature Store + MLflow separados.
# En GCP necesitarías Vertex AI + BigQuery ML + herramientas separadas.
# Snowflake tiene Snowpark ML pero es mucho más limitado.
# =============================================================================

import mlflow
import mlflow.sklearn
import mlflow.spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count, sum as spark_sum
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Ejemplo 1: MLflow Tracking - Registro automático de experimentos
# ---------------------------------------------------------------------------
# MLflow fue CREADO por Databricks y está integrado nativamente.
# Cada run se registra automáticamente con métricas, parámetros y artefactos.

# Activar autologging (exclusivo de la integración nativa)
mlflow.autolog()

# Configurar experimento
mlflow.set_experiment("/Experiments/prediccion_churn")

# Datos de ejemplo para predicción de churn
data_churn = [
    (1, 24, 12, 5, 2500.0, 3, 0),
    (2, 35, 36, 15, 8500.0, 8, 0),
    (3, 19, 3, 1, 500.0, 1, 1),
    (4, 45, 48, 25, 15000.0, 12, 0),
    (5, 28, 6, 2, 1200.0, 2, 1),
]

df_churn = spark.createDataFrame(
    data_churn,
    ["cliente_id", "edad", "meses_cliente", "productos", "saldo", "interacciones", "churn"]
)

# Pipeline de ML con tracking automático
assembler = VectorAssembler(
    inputCols=["edad", "meses_cliente", "productos", "saldo", "interacciones"],
    outputCol="features"
)

scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

rf = RandomForestClassifier(
    labelCol="churn",
    featuresCol="scaled_features",
    numTrees=100,
    maxDepth=5
)

pipeline = Pipeline(stages=[assembler, scaler, rf])

# MLflow registra AUTOMÁTICAMENTE:
# - Parámetros del modelo (numTrees, maxDepth, etc.)
# - Métricas (accuracy, F1, AUC)
# - El modelo serializado
# - El pipeline completo
with mlflow.start_run(run_name="random_forest_churn_v1") as run:
    mlflow.log_param("algoritmo", "RandomForest")
    mlflow.log_param("dataset_size", df_churn.count())
    
    model = pipeline.fit(df_churn)
    predictions = model.transform(df_churn)
    
    evaluator = BinaryClassificationEvaluator(labelCol="churn")
    auc = evaluator.evaluate(predictions)
    
    mlflow.log_metric("auc_roc", auc)
    mlflow.log_metric("num_features", 5)
    
    # Log del modelo con firma automática
    mlflow.spark.log_model(model, "churn_model")
    
    print(f"✅ Experiment tracked - Run ID: {run.info.run_id}")
    print(f"   AUC-ROC: {auc:.4f}")

# ---------------------------------------------------------------------------
# Ejemplo 2: MLflow Model Registry - Gestión del ciclo de vida de modelos
# ---------------------------------------------------------------------------
# Registro centralizado de modelos con versionado, staging y producción.
# Integrado con Unity Catalog para gobernanza de modelos.

# Registrar modelo en Unity Catalog (nuevo en Databricks)
model_uri = f"runs:/{run.info.run_id}/churn_model"

# Registrar en Unity Catalog Model Registry
mlflow.register_model(
    model_uri=model_uri,
    name="produccion.ml_models.churn_predictor"
)

# Transicionar modelo a producción con aprobación
from mlflow import MlflowClient
client = MlflowClient()

client.set_registered_model_alias(
    name="produccion.ml_models.churn_predictor",
    alias="champion",
    version=1
)

# Cargar modelo de producción
champion_model = mlflow.spark.load_model(
    "models:/produccion.ml_models.churn_predictor@champion"
)

print("✅ Modelo registrado y promovido a 'champion' en Model Registry")

# ---------------------------------------------------------------------------
# Ejemplo 3: AutoML - Machine Learning automatizado
# ---------------------------------------------------------------------------
# Databricks AutoML es ÚNICO: genera notebooks explicables con el código
# completo, no una caja negra. Puedes ver y editar todo lo que hizo.
# AWS SageMaker Autopilot genera modelos pero no notebooks editables.

from databricks import automl

# AutoML para clasificación
summary = automl.classify(
    dataset=df_churn,
    target_col="churn",
    primary_metric="roc_auc",
    timeout_minutes=30,
    max_trials=50
)

# AutoML devuelve:
print(f"Mejor modelo: {summary.best_trial.model_description}")
print(f"Mejor AUC: {summary.best_trial.metrics['test_roc_auc']:.4f}")
print(f"Notebook del mejor trial: {summary.best_trial.notebook_path}")
# ¡Puedes abrir ese notebook y ver TODO el código que AutoML generó!

# AutoML para regresión
summary_reg = automl.regress(
    dataset=df_churn,
    target_col="saldo",
    primary_metric="rmse",
    timeout_minutes=20
)

# AutoML para forecasting de series temporales
# (Snowflake NO tiene esto integrado)
summary_forecast = automl.forecast(
    dataset=df_ventas_diarias,
    target_col="ventas",
    time_col="fecha",
    frequency="D",
    horizon=30,
    primary_metric="smape"
)

print("✅ AutoML completado con notebooks explicables generados")

# ---------------------------------------------------------------------------
# Ejemplo 4: Feature Store - Centralizar features para ML
# ---------------------------------------------------------------------------
# Feature Store integrado con Unity Catalog.
# Reutiliza features entre equipos, evita duplicación de trabajo.

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()

# Crear tabla de features
spark.sql("""
    CREATE TABLE IF NOT EXISTS produccion.features.cliente_features (
        cliente_id BIGINT,
        avg_transaccion_30d DOUBLE,
        num_transacciones_30d INT,
        dias_desde_ultima_compra INT,
        categoria_gasto STRING,
        score_engagement DOUBLE,
        timestamp_actualizacion TIMESTAMP
    )
    USING DELTA
""")

# Registrar como Feature Table
fe.create_table(
    name="produccion.features.cliente_features",
    primary_keys=["cliente_id"],
    timestamp_keys=["timestamp_actualizacion"],
    description="Features de cliente calculadas diariamente"
)

# Escribir features
features_df = spark.sql("""
    SELECT 
        cliente_id,
        AVG(monto) AS avg_transaccion_30d,
        COUNT(*) AS num_transacciones_30d,
        DATEDIFF(current_date(), MAX(fecha)) AS dias_desde_ultima_compra,
        CASE 
            WHEN AVG(monto) > 5000 THEN 'premium'
            WHEN AVG(monto) > 1000 THEN 'medio'
            ELSE 'basico'
        END AS categoria_gasto,
        (COUNT(*) * 0.3 + AVG(monto) * 0.0001) AS score_engagement,
        current_timestamp() AS timestamp_actualizacion
    FROM produccion.ventas.ordenes
    WHERE fecha >= date_sub(current_date(), 30)
    GROUP BY cliente_id
""")

fe.write_table(
    name="produccion.features.cliente_features",
    df=features_df,
    mode="merge"
)

# Entrenar modelo USANDO Feature Store
# Las features se buscan automáticamente por cliente_id
training_set = fe.create_training_set(
    df=df_churn.select("cliente_id", "churn"),
    feature_lookups=[
        FeatureLookup(
            table_name="produccion.features.cliente_features",
            lookup_key="cliente_id"
        )
    ],
    label="churn"
)

training_df = training_set.load_df()
print("✅ Feature Store configurado - features reutilizables entre equipos")

# ---------------------------------------------------------------------------
# Ejemplo 5: Model Serving - Despliegue de modelos como API REST
# ---------------------------------------------------------------------------
# Databricks permite servir modelos como endpoints REST con un clic.
# Incluye auto-scaling, A/B testing y monitoreo integrado.
# SageMaker Endpoints requiere mucha más configuración.

import requests

# Crear endpoint de serving (normalmente desde UI o API)
# El modelo se despliega como una API REST escalable

# Configuración del endpoint
endpoint_config = {
    "name": "churn-predictor-endpoint",
    "config": {
        "served_models": [
            {
                "model_name": "produccion.ml_models.churn_predictor",
                "model_version": "1",
                "workload_size": "Small",
                "scale_to_zero_enabled": True  # Ahorro de costos
            }
        ],
        "auto_capture_config": {
            "catalog_name": "produccion",
            "schema_name": "ml_models",
            "table_name_prefix": "churn_predictor"
            # Captura automática de requests/responses para monitoreo
        }
    }
}

# Llamar al endpoint para predicción
# (Una vez desplegado, es una API REST estándar)
DATABRICKS_HOST = "https://mi-workspace.cloud.databricks.com"
ENDPOINT_URL = f"{DATABRICKS_HOST}/serving-endpoints/churn-predictor-endpoint/invocations"

payload = {
    "dataframe_records": [
        {
            "edad": 28,
            "meses_cliente": 6,
            "productos": 2,
            "saldo": 1200.0,
            "interacciones": 2
        }
    ]
}

# El endpoint escala automáticamente según la demanda
print("✅ Model Serving endpoint configurado con auto-scaling")

# ---------------------------------------------------------------------------
# Ejemplo 6: Monitoreo de modelos (Model Monitoring)
# ---------------------------------------------------------------------------
# Detectar data drift y degradación del modelo automáticamente.
# Databricks Lakehouse Monitoring es integrado, no necesitas herramientas externas.

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Crear monitor para la tabla de inferencias
spark.sql("""
    CREATE MONITOR produccion.ml_models.churn_predictor_payload
    SCHEDULE CRON '0 0 * * *'
    USING PROFILE_TYPE INFERENCE
    WITH (
        MODEL_TYPE = 'CLASSIFICATION',
        PREDICTION_COL = 'prediction',
        LABEL_COL = 'churn',
        TIMESTAMP_COL = 'timestamp',
        PROBLEM_TYPE = 'CLASSIFICATION',
        GRANULARITIES = ('1 day', '1 week')
    )
""")

# El monitor detectará automáticamente:
# - Data drift (cambios en la distribución de features)
# - Concept drift (degradación de métricas)
# - Anomalías en las predicciones
# - Cambios estadísticos en inputs

print("✅ Monitoreo de modelo configurado - alertas automáticas de drift")

# ---------------------------------------------------------------------------
# Ejemplo 7: Mosaic AI (antes Databricks AI) - LLMs y RAG
# ---------------------------------------------------------------------------
# Databricks ofrece hosting de LLMs abiertos (Llama, DBRX, Mixtral)
# y herramientas de RAG integradas con tus datos. Único en el mercado.

# Usar Foundation Model APIs (modelos hospedados por Databricks)
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Llamar a un LLM hospedado en Databricks
response = w.serving_endpoints.query(
    name="databricks-dbrx-instruct",
    messages=[
        {"role": "system", "content": "Eres un analista financiero experto."},
        {"role": "user", "content": "Analiza las tendencias de churn del Q1 2026"}
    ],
    max_tokens=500,
    temperature=0.7
)

# Vector Search para RAG (Retrieval Augmented Generation)
# Integrado con Delta Lake - tus documentos se vectorizan automáticamente

# Crear índice de vector search
spark.sql("""
    CREATE INDEX IF NOT EXISTS produccion.ventas.docs_venta_index
    ON TABLE produccion.ventas.documentos_venta
    (contenido_embedding VECTOR(1536))
    USING VECTOR_SEARCH
    WITH (
        endpoint_name = 'vector-search-endpoint',
        embedding_model = 'databricks-gte-large'
    )
""")

# Buscar documentos similares
results = w.vector_search_indexes.query(
    index_name="produccion.ventas.docs_venta_index",
    query_text="política de devolución de productos electrónicos",
    columns=["titulo", "contenido", "fecha"],
    num_results=5
)

print("✅ Mosaic AI configurado - LLMs y RAG con tus datos empresariales")

# ---------------------------------------------------------------------------
# Ejemplo 8: AI Functions en SQL - ML sin código Python
# ---------------------------------------------------------------------------
# Usar modelos de ML directamente en queries SQL.
# Ningún otro data warehouse ofrece esto de forma tan nativa.

spark.sql("""
    SELECT 
        cliente_id,
        nombre,
        email,
        ai_classify(
            CONCAT('Cliente con ', meses_antiguedad, ' meses, ', 
                   num_quejas, ' quejas, score NPS: ', nps_score),
            ARRAY('alto_riesgo_churn', 'medio_riesgo', 'bajo_riesgo', 'leal')
        ) AS riesgo_churn,
        ai_summarize(notas_servicio_cliente) AS resumen_interacciones,
        ai_extract(
            notas_servicio_cliente,
            ARRAY('sentimiento', 'tema_principal', 'urgencia')
        ) AS insights
    FROM produccion.ventas.clientes
    WHERE activo = true
""")

# Generar SQL con lenguaje natural
spark.sql("""
    SELECT ai_query(
        'databricks-dbrx-instruct',
        'Genera un query SQL que encuentre los top 10 clientes por valor 
         de vida del cliente (CLV) del último año, incluyendo frecuencia 
         de compra y ticket promedio'
    ) AS query_generado
""")

print("✅ AI Functions en SQL - Machine Learning accesible para analistas")
