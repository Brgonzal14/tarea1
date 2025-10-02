# Tarea 1 - Sistemas Distribuidos (2025-2)

Este proyecto implementa un sistema distribuido de **preguntas y respuestas** con almacenamiento en base de datos, servicio de caché, un modelo de lenguaje (Gemini) y un generador de tráfico para simular cargas.  

La arquitectura está construida con **Docker Compose**, lo que facilita la orquestación, despliegue y pruebas de cada microservicio.

---

## 📂 Servicios

- **Postgres**: Base de datos principal que almacena preguntas, respuestas y resultados de las corridas.
- **Redis (Cache-Service)**: Servicio de caché en memoria para acelerar respuestas y evaluar políticas de reemplazo (**LRU**, **LFU**, **FIFO**).
- **Storage-API**: API REST para gestionar preguntas, resultados y métricas de las corridas.
- **Responder-LLM**: Servicio que conecta con **Gemini** para responder preguntas en inglés.
- **Scorer**: Evalúa las respuestas del LLM contra las respuestas esperadas y asigna un puntaje de similitud.
- **Traffic-Gen**: Generador de tráfico que simula consultas concurrentes para medir desempeño, hit-rate y latencia.

---

## ⚙️ Requisitos

- Docker
- Docker Compose
- Python 3.10+ (solo si deseas probar manualmente los servicios fuera de Docker)

---

## ▶️ Ejecución

1. Clonar este repositorio:
   ```bash
   git clone https://github.com/usuario/tarea1-sistemas-distribuidos.git
   cd tarea1-sistemas-distribuidos
   
2. Crear el archivo .env con las variables necesarias:
    PG_DB=sd
    PG_USER=sd_user
    PG_PASS=sd_pass
    REDIS_MAXMEMORY=64mb
    REDIS_POLICY=allkeys-lru
    GEMINI_API_KEY=tu_api_key
    GEMINI_MODEL=gemini-2.5-flash
    GEMINI_API_VERSION=v1
  
3. Levantar los servicios:
 ``docker compose up --build `` `

4. Verificar que todos los contenedores están healthy:
 `` `docker compose ps `` `

📊 Métricas y Pruebas

1. Para iniciar una corrida con el generador de tráfico:
 `` docker compose logs -f traffic-gen `` 

2. Consultar métricas por run_id:
 `` curl "http://localhost:8001/metrics?run_id=dev1" `` 

3. Revisar estadísticas de caché:
 `` curl "http://localhost:8002/stats" `` 



