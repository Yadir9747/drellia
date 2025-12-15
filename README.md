# drellia

Este repositorio contiene el código fuente para el pipeline de extracción, análisis y sincronización de conversaciones desde Botmaker (vía BigQuery) hacia el ecosistema Drellia.

El sistema se encarga de extraer lotes de conversaciones, realizar análisis cuantitativos y cualitativos (usando IA Generativa/Gemini), generar reportes PDF y finalmente sincronizar los mensajes normalizados con la API de Drellia.

 Estructura del Proyecto
Entrypoints (Cloud Functions)
main.py: Contiene las funciones principales (HTTP triggers):

drellia_extract_lote: Extrae datos de BigQuery y los guarda en Cloud SQL (staging).

drellia_envio: Lee los datos de staging, normaliza mensajes, resuelve clientes y los envía a la API de Drellia.

Módulos de Análisis y Reporte
analisis_cuantitativo.py: Calcula métricas estadísticas (tiempos de respuesta, distribución por departamento, uso de bots vs agentes).

analisis_cualitativo.py: Utiliza Vertex AI (Gemini Pro) para leer conversaciones y generar un resumen semántico (tono, quejas, fraudes, calidad de atención).

analisis_graficos.py: Genera gráficos con matplotlib y compila el reporte final en PDF.

enviar_analisis.py: Orquestador que ejecuta los análisis y envía el reporte por correo electrónico.

Módulos de Procesamiento y Lógica de Negocio
tabla_envio_mensajes.py: Prepara los datos desde la tabla de lotes (lotes_conversaciones) hacia la tabla de envío (envio_mensajes), resolviendo IDs de agentes y departamentos.

manager_customer.py: Se encarga de buscar o crear clientes (Customers) en la API de Drellia para mantener la integridad referencial.

messages_normalizer.py: Parsea y estandariza los formatos de mensajes (JSON, dumps de Python, timestamps) para que sean uniformes.

Utilidades
utils_email.py: Cliente SMTP para el envío de correos con adjuntos.

models.py (Implícito): Definiciones de clases de datos (ej. NormalizedMessage).

config.py / db.py (Implícitos): Configuraciones globales y helpers de conexión a base de datos.

🚀 Flujo de Datos
Extracción (drellia_extract_lote):

Consulta BigQuery usando drellia.sql.

Inserta los resultados en PostgreSQL (lotes_conversaciones).

(Opcional) Dispara el análisis y envío de reporte por email.

Preparación:

tabla_envio_mensajes transforma los datos crudos y resuelve las relaciones (Agentes, Deptos).

Envío (drellia_envio):

Toma los mensajes pendientes.

Sincroniza el cliente con manager_customer.

Normaliza el contenido con messages_normalizer.

Envía la sesión a la API de Drellia.