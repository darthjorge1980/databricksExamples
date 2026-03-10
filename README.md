# 🧱 Databricks Examples — Guía Completa con Ejemplos Prácticos

> Colección de **60+ ejemplos** de las características diferenciadoras de Databricks, organizados por área funcional. Ideal para Data Engineers, Data Scientists y Architects que quieren evaluar o aprender la plataforma.

---

## 📁 Estructura del Proyecto

| # | Archivo | Tema | Ejemplos |
|---|---------|------|----------|
| 01 | [`01_delta_lake_lakehouse.py`](01_delta_lake_lakehouse.py) | **Delta Lake & Lakehouse** | ACID transactions, time travel, MERGE (upsert), schema evolution, Z-ORDER, liquid clustering, Change Data Feed, VACUUM |
| 02 | [`02_unity_catalog_gobernanza.py`](02_unity_catalog_gobernanza.py) | **Unity Catalog** | Namespace jerárquico, row/column-level security, lineage automático, volumes, tags/clasificación, UDFs, auditoría, federated queries |
| 03 | [`03_mlflow_automl_ai.py`](03_mlflow_automl_ai.py) | **MLflow, AutoML & Mosaic AI** | MLflow tracking, Model Registry, AutoML con notebooks explicables, Feature Store, Model Serving, LLMs + RAG, AI Functions en SQL |
| 04 | [`04_streaming_tiempo_real.py`](04_streaming_tiempo_real.py) | **Structured Streaming** | Kafka ingestion, window aggregations, stream-stream joins, foreachBatch, Auto Loader, Delta Live Tables (medallion architecture) |
| 05 | [`05_sql_warehouses_photon.py`](05_sql_warehouses_photon.py) | **SQL Warehouses & Photon** | Photon Engine, Materialized Views, Predictive Optimization, SQL+Python notebooks, query federation, serverless compute |
| 06 | [`06_workflows_orquestacion.py`](06_workflows_orquestacion.py) | **Workflows & CI/CD** | Multi-task workflows, condicionales IF/ELSE, For-Each tasks, Databricks Asset Bundles (IaC), Git repos, parametrización |
| 07 | [`07_delta_sharing_colaboracion.py`](07_delta_sharing_colaboracion.py) | **Delta Sharing & Colaboración** | Delta Sharing (protocolo abierto), Clean Rooms, Marketplace, secrets management, dbutils, notebooks colaborativos |
| 08 | [`08_comparativa_competencia.py`](08_comparativa_competencia.py) | **Comparativa vs Competencia** | Tabla detallada: Databricks vs Snowflake vs AWS vs GCP vs Azure en 10 categorías |

---

## 🔑 Características Diferenciadoras de Databricks

### 🏗️ Arquitectura Lakehouse
Combina lo mejor de **data lakes** (bajo costo, formato abierto) con **data warehouses** (ACID, performance, gobernanza) en una sola plataforma.

### 📦 Delta Lake (Formato Abierto)
- **ACID Transactions** sobre data lakes
- **Time Travel** — consulta datos en cualquier versión pasada
- **MERGE/Upsert** — imposible en Parquet puro
- **Schema Evolution** — columnas nuevas sin romper pipelines
- **Liquid Clustering** — reemplaza particionamiento estático

### 🔐 Unity Catalog
- Gobernanza unificada: tablas + ML models + archivos + funciones
- **Lineage automático** sin herramientas externas
- Row-level y column-level security
- **Federated Queries** a PostgreSQL, MySQL, SQL Server

### 🤖 Machine Learning End-to-End
- **MLflow** (creado por Databricks) integrado nativamente
- **AutoML** que genera notebooks explicables (no caja negra)
- **Feature Store** con Unity Catalog
- **Mosaic AI** — RAG + Vector Search + LLMs con tus datos
- **AI Functions en SQL** — ML sin código Python

### ⚡ Streaming Unificado
- Batch y streaming en **un solo framework**
- **Delta Live Tables** — pipelines declarativos (Bronze → Silver → Gold)
- **Auto Loader** — ingestión automática de archivos nuevos
- Stream-stream joins con exactly-once semantics

### 🚀 Performance
- **Photon Engine** (C++ nativo) — hasta 12x más rápido que Spark estándar
- **Predictive Optimization** — OPTIMIZE y VACUUM automáticos
- **Serverless SQL Warehouses** — arranque en ~10 segundos

### 🔄 Delta Sharing
- **Protocolo abierto** — el receptor NO necesita Databricks
- **Clean Rooms** — análisis conjunto preservando privacidad
- Compatible con pandas, Spark, Power BI, Tableau

---

## 🆚 Databricks vs Competencia (Resumen)

| Capacidad | Databricks | Snowflake | AWS | GCP |
|-----------|:----------:|:---------:|:---:|:---:|
| Lakehouse nativo | ✅ | ❌ (solo warehouse) | ❌ (4 servicios) | ❌ (separados) |
| Formato abierto | ✅ Delta Lake | ❌ Propietario | ⚠️ Hudi/Iceberg | ❌ Propietario |
| Gobernanza unificada (datos+ML) | ✅ Unity Catalog | ⚠️ Solo tablas | ❌ Glue básico | ❌ Múltiples servicios |
| ML integrado | ✅ MLflow + AutoML + Serving | ⚠️ Snowpark ML básico | ⚠️ SageMaker (separado) | ⚠️ Vertex AI (separado) |
| Streaming real-time | ✅ Structured Streaming + DLT | ❌ Solo Snowpipe | ⚠️ Kinesis+Lambda | ⚠️ Dataflow (separado) |
| Sharing abierto | ✅ Delta Sharing | ❌ Solo entre Snowflake | ❌ Data Exchange | ❌ Analytics Hub |
| Motor acelerado | ✅ Photon (12x) | ✅ Propietario | ❌ Spark estándar | ✅ Dremel |
| CI/CD nativo | ✅ Asset Bundles | ❌ Externo | ❌ CloudFormation | ❌ Terraform |
| Multi-cloud | ✅ AWS/Azure/GCP | ✅ AWS/Azure/GCP | ❌ Solo AWS | ❌ Solo GCP |

---

## 🛠️ Requisitos

- **Databricks Workspace** en AWS, Azure o GCP
- **Databricks Runtime 14.0+** (recomendado 15.4+)
- Python 3.10+
- Librerías incluidas en Databricks Runtime:
  - `pyspark`
  - `delta-spark`
  - `mlflow`
  - `databricks-sdk`

## 🚀 Cómo Usar

1. **Clonar el repositorio:**
   ```bash
   git clone https://github.com/darthjorge1980/databricksExamples.git
   ```

2. **Importar en Databricks:**
   - Ve a tu Workspace → **Repos** → **Add Repo**
   - Pega la URL del repositorio
   - Los notebooks estarán disponibles inmediatamente

3. **Ejecutar:**
   - Abre cualquier archivo `.py` como notebook
   - Adjunta un cluster con Databricks Runtime 14.0+
   - Ejecuta celda por celda — cada ejemplo es independiente

> **Nota:** Algunos ejemplos requieren permisos de administrador (Unity Catalog, Delta Sharing) o recursos externos (Kafka, PostgreSQL). Los comentarios en cada archivo indican los prerrequisitos.

---

## 📚 Recursos Adicionales

- [Documentación oficial de Databricks](https://docs.databricks.com/)
- [Delta Lake — Documentación](https://docs.delta.io/)
- [MLflow — Documentación](https://mlflow.org/docs/latest/)
- [Databricks Academy (cursos gratuitos)](https://www.databricks.com/learn)
- [Delta Sharing — Protocolo abierto](https://delta.io/sharing/)

---

## 📄 Licencia

Este proyecto está bajo la licencia MIT. Ver [LICENSE](LICENSE) para más detalles.

---

## 🤝 Contribuciones

¡Las contribuciones son bienvenidas! Si tienes ejemplos adicionales o mejoras:

1. Haz fork del repositorio
2. Crea una rama (`git checkout -b feature/nuevo-ejemplo`)
3. Commit tus cambios (`git commit -m 'Agrega ejemplo de X'`)
4. Push a la rama (`git push origin feature/nuevo-ejemplo`)
5. Abre un Pull Request
