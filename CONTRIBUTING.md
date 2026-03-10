# Contribuir a Databricks Examples

¡Gracias por tu interés en contribuir! 🎉

## Cómo Contribuir

### Reportar Problemas
- Usa [GitHub Issues](https://github.com/darthjorge1980/databricksExamples/issues)
- Describe el problema con detalle
- Incluye la versión de Databricks Runtime que usas

### Agregar Ejemplos

1. **Fork** el repositorio
2. **Crea una rama** desde `main`:
   ```bash
   git checkout -b feature/nombre-del-ejemplo
   ```
3. **Sigue el formato** de los archivos existentes:
   - Header con descripción y diferenciador vs competencia
   - Cada ejemplo con separador `# ---` y número
   - Comentarios explicando el "por qué", no solo el "qué"
   - `print("✅ ...")` al final de cada ejemplo
4. **Commit** con mensaje descriptivo
5. **Push** y abre un **Pull Request**

### Convenciones de Código

- Archivos nombrados con prefijo numérico: `09_tema.py`
- Comentarios en **español**
- Variables y funciones en **snake_case**
- Cada ejemplo debe ser ejecutable de forma independiente
- No incluir credenciales, tokens ni datos reales

### Áreas donde puedes contribuir

- Ejemplos de **Databricks Connect** (desarrollo local)
- Ejemplos de **Genie** (data rooms con lenguaje natural)
- Integración con **Apache Airflow**
- Patrones de **testing** en Databricks
- Ejemplos de **cost optimization**
