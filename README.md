````markdown
# Tarea 2 - Sistemas Distribuidos (2025-2)

Este proyecto implementa un sistema distribuido de **preguntas y respuestas** que evoluciona la arquitectura de la Tarea 1 hacia un modelo asíncrono y resiliente utilizando Apache Kafka y Apache Flink. El sistema gestiona un flujo de procesamiento de preguntas, interactúa con un modelo de lenguaje (Gemini), evalúa la calidad de las respuestas y persiste los resultados validados.

La arquitectura está construida con **Docker Compose**, facilitando la orquestación y despliegue de todos los microservicios.

---

## 🏗️ Arquitectura Asíncrona

El sistema utiliza Apache Kafka como bus de mensajes para desacoplar los servicios y gestionar las cargas de trabajo de forma asíncrona. **Apache Flink** se emplea como motor de procesamiento de flujos para analizar la calidad de las respuestas y implementar un ciclo de retroalimentación.

El flujo principal es el siguiente:
1.  Traffic-Gen: Genera preguntas. Antes de enviar una nueva pregunta, consulta a `Storage-API` para verificar si ya existe un resultado persistido. Si no existe, la publica en el tópico Kafka `preguntas_nuevas`.
2.  LLM-Worker: Consume preguntas del tópico `preguntas_nuevas`. Llama al servicio `Responder-LLM` para obtener una respuesta.
    * Si la respuesta es exitosa (200 OK), la publica en el tópico `respuestas_llm_ok`.
    * Si ocurre un error reintentable (ej. 429, 5xx), y no se ha superado el máximo de reintentos, publica el mensaje en `respuestas_llm_fallidas_reintentar`.
3.  Retry-Worker: Consume mensajes de `respuestas_llm_fallidas_reintentar`. Aplica una espera con *exponential backoff* y vuelve a publicar la pregunta en `preguntas_nuevas` para un nuevo intento.
4.  Flink Job: Consume respuestas de `respuestas_llm_ok`. Llama al servicio `Scorer` para calcular la similitud con la respuesta esperada.
    * Si el *score* supera un umbral (`SCORE_THRESHOLD`), considera la respuesta válida y la publica en `resultados_validados`.
    * Si el *score* es bajo y no se ha superado el máximo de reintentos de Flink (`MAX_FLINK_RETRIES`), reinyecta la pregunta publicándola de nuevo en `preguntas_nuevas` para intentar generar una mejor respuesta.
5.  Storage-API: Ahora incluye un consumidor Kafka que escucha el tópico `resultados_validados`. Cuando recibe un resultado validado por Flink, lo persiste en la base de datos PostgreSQL.

---

## 📂 Servicios

-   Postgres: Base de datos relacional para persistencia final de preguntas (`qa_yahoo`) y resultados validados (`qa_results`).
-   Redis (Cache-Service)**: Servicio de caché en memoria (opcionalmente utilizable, aunque el flujo principal es asíncrono).
-   Storage-API: API REST para consultar si existen resultados y consumidor Kafka para persistir resultados validados.
-   Responder-LLM: Microservicio que interactúa con la API de Gemini (o un *stub*) para generar respuestas.
-   Scorer: API REST que calcula un *score* de similitud entre la respuesta del LLM y la respuesta esperada.
-   Traffic-Gen: Generador de carga que simula preguntas de usuarios, verifica existencia y publica en Kafka.
-   Zookeeper: Requerido por Kafka para coordinación.
-   Kafka**: Broker de mensajería para la comunicación asíncrona.
-   Kafka-Init**: Job que crea los tópicos necesarios en Kafka al iniciar.
-   LLM-Worker**: Consumidor Kafka que procesa preguntas, llama al LLM y publica resultados o errores.
-   Retry-Worker: Consumidor Kafka que gestiona los reintentos con *backoff* exponencial.
-   Flink (JobManager & TaskManager)**: Clúster de Flink para procesamiento de flujos.
-   Flink-Job-Submitter: Contenedor que envía el job de Python (`job.py`) al clúster de Flink.

---

## ⚙️ Requisitos

-   Docker
-   Docker Compose
-   Python 3.10+ (para desarrollo local si es necesario)
-   Una clave API de Google Gemini

---

## ▶️ Ejecución

1.  Clonar este repositorio:
    ```bash
    git clone <URL_DEL_REPOSITORIO>
    cd <NOMBRE_CARPETA_PROYECTO>
    ```

2.  Crear el archivo `.env` en la raíz del proyecto con las siguientes variables:
    ```env
    # --- Base de Datos (Postgres) ---
    PG_DB=sd
    PG_USER=sd_user
    PG_PASS=sd_pass

    # --- Caché (Redis) ---
    REDIS_MAXMEMORY=64mb
    REDIS_POLICY=allkeys-lru
    CACHE_TTL_SECONDS=21600

    # --- LLM (Gemini) ---
    GEMINI_API_KEY=tu_api_key_aqui # ¡¡IMPORTANTE: Reemplaza con tu clave!!
    GEMINI_MODEL=gemini-2.5-flash
    GEMINI_API_VERSION=v1beta
    LLM_MODE=GEMINI # Puede ser STUB para pruebas sin API Key
    LLM_LANG=en # Idioma para el LLM (en/es)

    # --- Configuración de Workers y Flink ---
    MAX_RETRIES=3 # Reintentos máximos del LLM-Worker
    RETRY_BASE_DELAY_S=5 # Delay inicial (segundos) para el Retry-Worker
    RETRY_MAX_DELAY_S=60 # Delay máximo (segundos) para el Retry-Worker
    SCORE_THRESHOLD=0.1 # Umbral de score en Flink para considerar válida una respuesta
    MAX_FLINK_RETRIES=1 # Reintentos máximos iniciados por Flink si el score es bajo

    # --- Configuración del Generador de Tráfico y Corrida por defecto ---
    RUN_ID=dev_t2 # Identificador de la corrida (usado por traffic-gen y storage-api)
    BASE_RATE_RPS=5 # Tasa promedio de requests por segundo
    DURATION_SECONDS=30 # Duración de la generación de tráfico (si TOTAL_REQUESTS es 0)
    TRAFFIC_DIST=poisson # Distribución del tráfico (poisson o bursty)
    # TOTAL_REQUESTS=0 # Descomenta y pon un número > 0 para enviar un número fijo de requests
    ```

3.  Levantar todos los servicios:
    ```bash
    docker compose up --build -d
    ```
    El `-d` ejecuta los contenedores en segundo plano. Si quiere ver los logs en tiempo real, omite el `-d`.

4.  Verificar que todos los contenedores estén saludables (después de 1-2 minutos):
    ```bash
    docker compose ps
    ```
    (Todos deberían mostrar `running` o `healthy`).

---

## 📊 Monitoreo y Pruebas

1.  Ver Logs Combinados:
    ```bash
    docker compose logs -f
    ```
    (Presiona `Ctrl+C` para detener la visualización).

2.  **Ver Logs de un Servicio Específico** (ej. Flink TaskManager):
    ```bash
    docker compose logs -f taskmanager
    ```

3.  Dashboard de Flink:
    Abre en tu navegador: `http://localhost:8081`
    * Aquí puedes ver el job corriendo (`ScoreCalculatorJob_KafkaToKafka`) y sus métricas.

4.  Verificar Resultados en la Base de Datos (opcional):
    Puedes conectar a Postgres (puerto 5432) con un cliente SQL (como DBeaver, pgAdmin, o `psql`) usando las credenciales del `.env` y consultar la tabla `qa_results`.

5.  Estadísticas de Caché (si se usara):
    ```bash
    # En PowerShell:
    Invoke-WebRequest -Uri http://localhost:8002/stats

    # En bash/zsh (Linux/Mac):
    curl "http://localhost:8002/stats"
    ```

6.  Endpoints de Salud:
    Puedes verificar la salud de cada API individualmente:
    ```bash
    # PowerShell
    Invoke-WebRequest -Uri http://localhost:8001/health # Storage API
    Invoke-WebRequest -Uri http://localhost:8002/health # Cache Service
    Invoke-WebRequest -Uri http://localhost:8004/health # Scorer
    Invoke-WebRequest -Uri http://localhost:8093/health # Responder LLM (¡Ajusta el puerto si usaste otro!)

    # bash/zsh
    curl http://localhost:8001/health
    curl http://localhost:8002/health
    curl http://localhost:8004/health
    curl http://localhost:8093/health # ¡Ajusta el puerto!
    ```

---

## ⏹️ Detener el Sistema

```bash
docker compose down
````

*(Esto detiene y elimina los contenedores, pero preserva el volumen de Postgres)*.

Si quieres borrar también los datos de Postgres:

```bash
docker compose down -v
```

-----

Estudiante: \[Bryan González] 

Curso: Sistemas Distribuidos 2025-2


```
