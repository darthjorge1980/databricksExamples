# =============================================================================
# DATABRICKS - WORKFLOWS, ORQUESTACIÓN Y DATABRICKS ASSET BUNDLES
# =============================================================================
# Característica diferenciadora: Databricks Workflows orquesta notebooks,
# scripts Python, SQL, JAR, y pipelines DLT en un solo sistema.
# A diferencia de Airflow, no necesitas infraestructura separada.
# A diferencia de Step Functions (AWS), tiene UI visual y retry inteligente.
# Databricks Asset Bundles (DABs) permiten CI/CD nativo.
# =============================================================================

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task, NotebookTask, SparkPythonTask, SqlTask, 
    PipelineTask, JobCluster, TaskDependency,
    CronSchedule, Condition, RunIf
)

w = WorkspaceClient()

# ---------------------------------------------------------------------------
# Ejemplo 1: Workflow multi-tarea con dependencias
# ---------------------------------------------------------------------------
# Define un pipeline ETL completo con tareas dependientes.
# Databricks gestiona el orden de ejecución automáticamente.

workflow = w.jobs.create(
    name="pipeline_ventas_diario",
    tasks=[
        # Tarea 1: Ingestión (Auto Loader)
        Task(
            task_key="ingesta_datos",
            description="Ingestar archivos nuevos de S3",
            notebook_task=NotebookTask(
                notebook_path="/Repos/produccion/etl/01_ingesta",
                base_parameters={
                    "fecha": "{{job.start_time.iso_date}}",
                    "ambiente": "produccion"
                }
            ),
            job_cluster_key="cluster_etl",
            max_retries=3,
            min_retry_interval_millis=60000
        ),
        
        # Tarea 2: Limpieza y validación (depende de ingesta)
        Task(
            task_key="limpieza_datos",
            description="Limpiar y validar datos ingeridos",
            depends_on=[TaskDependency(task_key="ingesta_datos")],
            notebook_task=NotebookTask(
                notebook_path="/Repos/produccion/etl/02_limpieza",
                base_parameters={
                    "tabla_origen": "raw.ventas_staging",
                    "tabla_destino": "silver.ventas_limpias"
                }
            ),
            job_cluster_key="cluster_etl"
        ),
        
        # Tarea 3: Calidad de datos con DLT
        Task(
            task_key="calidad_datos",
            description="Pipeline DLT de calidad",
            depends_on=[TaskDependency(task_key="limpieza_datos")],
            pipeline_task=PipelineTask(
                pipeline_id="abc123-dlt-pipeline-id",
                full_refresh=False  # Incremental
            )
        ),
        
        # Tareas 4a y 4b: Paralelas (ambas dependen de calidad)
        Task(
            task_key="agregaciones_ventas",
            description="Calcular métricas de ventas",
            depends_on=[TaskDependency(task_key="calidad_datos")],
            notebook_task=NotebookTask(
                notebook_path="/Repos/produccion/etl/04a_agregaciones"
            ),
            job_cluster_key="cluster_analytics"
        ),
        Task(
            task_key="actualizar_features",
            description="Actualizar Feature Store",
            depends_on=[TaskDependency(task_key="calidad_datos")],
            notebook_task=NotebookTask(
                notebook_path="/Repos/produccion/ml/04b_features"
            ),
            job_cluster_key="cluster_ml"
        ),
        
        # Tarea 5: SQL Analytics (depende de agregaciones)
        Task(
            task_key="refresh_dashboards",
            description="Refrescar materialized views",
            depends_on=[TaskDependency(task_key="agregaciones_ventas")],
            sql_task=SqlTask(
                query=SqlTask.SqlTaskQuery(query_id="query-123"),
                warehouse_id="warehouse-456"
            )
        ),
        
        # Tarea 6: Reentrenar modelo (depende de features)
        Task(
            task_key="reentrenar_modelo",
            description="Reentrenar modelo de churn si hay drift",
            depends_on=[TaskDependency(task_key="actualizar_features")],
            notebook_task=NotebookTask(
                notebook_path="/Repos/produccion/ml/06_reentrenar",
                base_parameters={
                    "threshold_drift": "0.05",
                    "registrar_modelo": "true"
                }
            ),
            job_cluster_key="cluster_ml"
        ),
        
        # Tarea 7: Notificación (depende de ambas ramas)
        Task(
            task_key="notificar_completado",
            description="Enviar notificación de éxito",
            depends_on=[
                TaskDependency(task_key="refresh_dashboards"),
                TaskDependency(task_key="reentrenar_modelo")
            ],
            notebook_task=NotebookTask(
                notebook_path="/Repos/produccion/utils/notificar"
            ),
            job_cluster_key="cluster_etl"
        ),
    ],
    
    # Clusters del workflow (compartidos entre tareas)
    job_clusters=[
        JobCluster(
            job_cluster_key="cluster_etl",
            new_cluster={
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 4,
                "autoscale": {"min_workers": 2, "max_workers": 8}
            }
        ),
        JobCluster(
            job_cluster_key="cluster_analytics",
            new_cluster={
                "spark_version": "15.4.x-photon-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2
            }
        ),
        JobCluster(
            job_cluster_key="cluster_ml",
            new_cluster={
                "spark_version": "15.4.x-gpu-ml-scala2.12",
                "node_type_id": "g5.xlarge",
                "num_workers": 2
            }
        ),
    ],
    
    # Programación
    schedule=CronSchedule(
        quartz_cron_expression="0 0 6 * * ?",  # 6 AM diario
        timezone_id="America/Mexico_City"
    ),
    
    # Notificaciones
    email_notifications={
        "on_failure": ["equipo-data@empresa.com"],
        "on_success": ["dashboard-alerts@empresa.com"]
    },
    
    # Tags
    tags={"equipo": "data-engineering", "prioridad": "alta"}
)

print(f"✅ Workflow creado - ID: {workflow.job_id}")

# ---------------------------------------------------------------------------
# Ejemplo 2: Tareas condicionales (IF/ELSE en workflows)
# ---------------------------------------------------------------------------
# Ejecutar tareas diferentes según el resultado de tareas anteriores.
# Airflow no tiene branching tan limpio.

workflow_condicional = w.jobs.create(
    name="pipeline_condicional",
    tasks=[
        # Tarea de verificación
        Task(
            task_key="verificar_datos",
            notebook_task=NotebookTask(
                notebook_path="/Repos/produccion/checks/verificar"
            )
        ),
        
        # Condición: ¿hay datos nuevos?
        Task(
            task_key="hay_datos_nuevos",
            depends_on=[TaskDependency(task_key="verificar_datos")],
            condition_task=Condition(
                op="GREATER_THAN",
                left="{{tasks.verificar_datos.values.num_registros_nuevos}}",
                right="0"
            )
        ),
        
        # Si hay datos -> procesar
        Task(
            task_key="procesar_datos",
            depends_on=[TaskDependency(
                task_key="hay_datos_nuevos",
                outcome="true"
            )],
            notebook_task=NotebookTask(
                notebook_path="/Repos/produccion/etl/procesar"
            )
        ),
        
        # Si no hay datos -> log y salir
        Task(
            task_key="log_sin_datos",
            depends_on=[TaskDependency(
                task_key="hay_datos_nuevos",
                outcome="false"
            )],
            notebook_task=NotebookTask(
                notebook_path="/Repos/produccion/utils/log_skip"
            )
        ),
    ]
)

print("✅ Workflow condicional con branching creado")

# ---------------------------------------------------------------------------
# Ejemplo 3: For-Each Task - Iterar sobre una lista dinámica
# ---------------------------------------------------------------------------
# Procesar múltiples países/regiones en paralelo.
# Cada iteración es una tarea independiente que puede reintentar.

workflow_foreach = w.jobs.create(
    name="procesar_por_pais",
    tasks=[
        # Generar lista de países a procesar
        Task(
            task_key="obtener_paises",
            notebook_task=NotebookTask(
                notebook_path="/Repos/produccion/etl/listar_paises"
                # Este notebook retorna: ["MX", "CO", "CL", "AR", "PE"]
            )
        ),
        
        # For-Each: procesar cada país en paralelo
        Task(
            task_key="procesar_pais",
            depends_on=[TaskDependency(task_key="obtener_paises")],
            for_each_task={
                "inputs": "{{tasks.obtener_paises.values.paises}}",
                "task": Task(
                    task_key="etl_por_pais",
                    notebook_task=NotebookTask(
                        notebook_path="/Repos/produccion/etl/etl_pais",
                        base_parameters={
                            "pais": "{{input}}"
                        }
                    )
                ),
                "concurrency": 5  # Hasta 5 países en paralelo
            }
        ),
        
        # Consolidar resultados
        Task(
            task_key="consolidar",
            depends_on=[TaskDependency(task_key="procesar_pais")],
            notebook_task=NotebookTask(
                notebook_path="/Repos/produccion/etl/consolidar"
            )
        ),
    ]
)

print("✅ Workflow For-Each: procesa N países en paralelo")

# ---------------------------------------------------------------------------
# Ejemplo 4: Databricks Asset Bundles (DABs) - CI/CD nativo
# ---------------------------------------------------------------------------
# Definir TODA la infraestructura como código (IaC).
# Similar a Terraform pero específico para Databricks.
# No existe equivalente en Snowflake ni BigQuery.

# Archivo: databricks.yml (raíz del proyecto)
DATABRICKS_YML = """
bundle:
  name: ventas-analytics-pipeline

variables:
  ambiente:
    description: "Ambiente de despliegue"
    default: desarrollo

workspace:
  host: https://mi-workspace.cloud.databricks.com

resources:
  jobs:
    pipeline_ventas:
      name: "pipeline_ventas_${var.ambiente}"
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: "America/Mexico_City"
      tasks:
        - task_key: ingesta
          notebook_task:
            notebook_path: ./notebooks/01_ingesta.py
          job_cluster_key: cluster_etl
        - task_key: transformacion
          depends_on:
            - task_key: ingesta
          notebook_task:
            notebook_path: ./notebooks/02_transformacion.py
          job_cluster_key: cluster_etl
        - task_key: calidad
          depends_on:
            - task_key: transformacion
          notebook_task:
            notebook_path: ./notebooks/03_calidad.py
      job_clusters:
        - job_cluster_key: cluster_etl
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            num_workers: 4
  
  pipelines:
    dlt_ventas:
      name: "dlt_ventas_${var.ambiente}"
      target: "ventas_${var.ambiente}"
      libraries:
        - notebook:
            path: ./dlt/pipeline_ventas.py

  schemas:
    ventas_schema:
      catalog_name: "${var.ambiente}"
      name: ventas
      comment: "Schema de ventas para ${var.ambiente}"

targets:
  desarrollo:
    mode: development
    default: true
    variables:
      ambiente: desarrollo
    workspace:
      host: https://dev.cloud.databricks.com

  staging:
    mode: development
    variables:
      ambiente: staging
    workspace:
      host: https://staging.cloud.databricks.com

  produccion:
    mode: production
    variables:
      ambiente: produccion
    workspace:
      host: https://prod.cloud.databricks.com
    run_as:
      service_principal_name: "sp-produccion@empresa.com"
"""

print("✅ Databricks Asset Bundle configurado para CI/CD")

# Comandos de DABs:
# databricks bundle validate        -> Validar configuración
# databricks bundle deploy           -> Desplegar al workspace
# databricks bundle run pipeline_ventas  -> Ejecutar workflow
# databricks bundle destroy          -> Eliminar recursos

# ---------------------------------------------------------------------------
# Ejemplo 5: Parametrizar workflows con widgets
# ---------------------------------------------------------------------------
# Los notebooks pueden recibir parámetros del workflow dinámicamente.

# En el notebook de ingesta:
dbutils.widgets.text("fecha", "", "Fecha de proceso")
dbutils.widgets.dropdown("modo", "incremental", ["incremental", "full"], "Modo")
dbutils.widgets.text("tabla_destino", "silver.ventas", "Tabla destino")

fecha = dbutils.widgets.get("fecha")
modo = dbutils.widgets.get("modo")
tabla_destino = dbutils.widgets.get("tabla_destino")

print(f"Procesando: fecha={fecha}, modo={modo}, destino={tabla_destino}")

# Retornar valores al workflow (para tareas downstream)
dbutils.jobs.taskValues.set(key="registros_procesados", value=15000)
dbutils.jobs.taskValues.set(key="status", value="success")
dbutils.jobs.taskValues.set(key="tabla_actualizada", value=tabla_destino)

# En tareas downstream, acceder a estos valores:
# {{tasks.ingesta_datos.values.registros_procesados}}

print("✅ Notebook parametrizado con comunicación entre tareas")

# ---------------------------------------------------------------------------
# Ejemplo 6: Monitoreo y alertas de workflows
# ---------------------------------------------------------------------------

# Listar ejecuciones recientes
runs = w.jobs.list_runs(
    job_id=workflow.job_id,
    limit=10
)

for run in runs:
    print(f"Run {run.run_id}: {run.state.result_state} "
          f"({run.run_duration // 1000}s) - {run.start_time}")

# Obtener métricas de un run específico
run_detail = w.jobs.get_run(run_id=12345)

for task in run_detail.tasks:
    print(f"  Task: {task.task_key}")
    print(f"  Estado: {task.state.result_state}")
    print(f"  Duración: {task.execution_duration // 1000}s")
    print(f"  Cluster: {task.cluster_instance.cluster_id}")

# Configurar alertas
# Las alertas se pueden configurar por:
# - Duración excesiva
# - Fallo de tarea
# - Fallo de workflow completo
# - Métricas personalizadas

print("✅ Monitoreo de workflows con métricas detalladas por tarea")

# ---------------------------------------------------------------------------
# Ejemplo 7: Repos Git integrados
# ---------------------------------------------------------------------------
# Databricks se integra nativamente con Git (GitHub, GitLab, Azure DevOps).
# Los notebooks viven en repos Git con CI/CD.

# Crear repo en Databricks
repo = w.repos.create(
    url="https://github.com/mi-empresa/ventas-pipeline.git",
    provider="gitHub",
    path="/Repos/produccion/ventas-pipeline"
)

# Actualizar a un branch o tag específico
w.repos.update(
    repo_id=repo.id,
    branch="main"
)

# Los workflows referencian notebooks del repo:
# notebook_path="/Repos/produccion/ventas-pipeline/notebooks/01_ingesta.py"

# Flujo de CI/CD:
# 1. Desarrollar en branch feature/xxx
# 2. PR a main -> tests automáticos
# 3. Merge -> deploy automático con DABs
# 4. Los workflows en producción usan el branch main

print("✅ Repos Git integrados para version control y CI/CD")
