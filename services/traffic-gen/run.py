import os
import asyncio
import time
import random
import json
import httpx  # Para llamadas HTTP a storage-api
from kafka import KafkaProducer # Cliente Kafka para Python
from kafka.errors import KafkaError

# ---------- Configuración ----------
RUN_ID = os.getenv("RUN_ID", "dev-t2") + f"-{int(time.time())}" # Nuevo RUN_ID default
BASE_RATE_RPS = float(os.getenv("BASE_RATE_RPS", "10"))
DURATION_SECONDS = int(os.getenv("DURATION_SECONDS", "60"))
TRAFFIC_DIST = os.getenv("TRAFFIC_DIST", "poisson").lower()
TOTAL_REQUESTS = int(os.getenv("TOTAL_REQUESTS", "0"))

URL_STORAGE = os.getenv("URL_STORAGE", "http://storage-api:8001")
# URLs de LLM y Scorer ya no se usan directamente aquí
# URL_CACHE = os.getenv("URL_CACHE", "http://cache-service:8002") # Podría usarse opcionalmente

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_PREGUNTAS = os.getenv("KAFKA_TOPIC_PREGUNTAS", "preguntas_nuevas")

# Límite de peticiones simultáneas (ahora aplica a consultas a storage-api + publicaciones a Kafka)
MAX_INFLIGHT = int(os.getenv("MAX_INFLIGHT", "50")) # Podemos aumentar un poco el límite
_sem = asyncio.Semaphore(MAX_INFLIGHT)

# Contador simple para rastrear cuántas preguntas se publicaron
published_count = 0
skipped_count = 0

print(f"[traffic-gen] RUN_ID={RUN_ID} dist={TRAFFIC_DIST} base_rps={BASE_RATE_RPS} "
      f"dur={DURATION_SECONDS}s total_requests={TOTAL_REQUESTS}")
print(f"[traffic-gen] Conectando a Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"[traffic-gen] Publicando en tópico: {KAFKA_TOPIC_PREGUNTAS}")

# ---------- Cliente Kafka Productor ----------
# Se inicializa globalmente para reutilizar la conexión
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        # Serializador: convierte el diccionario Python a JSON bytes
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Configuraciones para reintentos y esperas (ajustar según sea necesario)
        acks='all', # Esperar confirmación de todos los brokers (en nuestro caso, 1)
        retries=3,
        retry_backoff_ms=100
    )
    print("[traffic-gen] Productor Kafka inicializado.")
except KafkaError as e:
    print(f"[traffic-gen][ERROR] No se pudo inicializar el productor Kafka: {e}")
    # Si no podemos conectar a Kafka al inicio, salimos.
    # Podríamos implementar reintentos aquí también.
    producer = None # Marcar como no disponible


# ---------- Lógica de Generación de Tráfico ----------

def interarrival(rate_rps: float) -> float:
    """Calcula el tiempo de espera entre llegadas para una tasa dada (Poisson)."""
    rate_rps = max(rate_rps, 0.001) # Evitar división por cero
    return random.expovariate(rate_rps)

async def check_question_processed(client: httpx.AsyncClient, question_id: int) -> bool:
    """
    Consulta a storage-api para ver si una pregunta ya tiene resultado.
    ¡NECESITA UN NUEVO ENDPOINT EN STORAGE-API!
    """
    try:
        # Asumimos un nuevo endpoint /results/exists/{question_id}
        # que devuelve 200 OK si existe, 404 Not Found si no existe.
        check_url = f"{URL_STORAGE}/results/exists/{question_id}"
        response = await client.get(check_url, timeout=5)
        return response.status_code == 200
    except httpx.RequestError as e:
        print(f"[traffic-gen][WARN] Error al verificar pregunta {question_id}: {e}")
        return False # Asumir que no existe si hay error de red


async def publish_to_kafka(question_data: dict):
    """Publica un mensaje en el tópico de preguntas nuevas."""
    global published_count
    if not producer:
        print("[traffic-gen][ERROR] Productor Kafka no disponible, saltando publicación.")
        return

    try:
        # El productor es síncrono por defecto en sus métodos de envío,
        # pero la librería maneja la E/S en background threads.
        # get(timeout=10) espera la confirmación o lanza excepción.
        future = producer.send(KAFKA_TOPIC_PREGUNTAS, value=question_data)
        record_metadata = future.get(timeout=10) # Espera confirmación
        # print(f"[traffic-gen] Pregunta {question_data.get('question_id')} publicada en partición {record_metadata.partition}")
        published_count += 1
    except KafkaError as e:
        print(f"[traffic-gen][ERROR] Error al publicar en Kafka: {e}")
    except Exception as e:
        print(f"[traffic-gen][ERROR] Error inesperado al publicar: {e}")

async def one_request(client: httpx.AsyncClient, idx: int):
    """
    Obtiene una pregunta, verifica si ya fue procesada, y si no, la publica en Kafka.
    """
    global skipped_count
    async with _sem:
        t0 = time.perf_counter()

        # 1) Obtener pregunta aleatoria del storage
        try:
            r_q = await client.get(f"{URL_STORAGE}/questions/random", timeout=10)
            if r_q.status_code != 200:
                print(f"[req {idx}] /questions/random status={r_q.status_code}")
                return
            q_data = r_q.json() # Esperamos {"question_id": N, "question": "...", "best_answer": "..."}
            qid = q_data.get("question_id")
            if not qid:
                 print(f"[req {idx}][ERROR] Respuesta de /questions/random sin question_id")
                 return
        except Exception as e:
            print(f"[req {idx}][ERROR] /questions/random: {e}")
            return

        # 2) Verificar si la pregunta ya fue procesada (consultando storage-api)
        already_processed = await check_question_processed(client, qid)

        if already_processed:
            # Si ya existe, la saltamos (simula la "respuesta encontrada" del diagrama T2)
            # print(f"[req {idx}] Pregunta {qid} ya procesada, saltando.")
            skipped_count += 1
        else:
            # 3) Si no existe, publicar en Kafka para procesamiento asíncrono
            #    Añadimos retry_count para futuro manejo de reintentos
            message_to_send = {
                "question_id": qid,
                "question_text": q_data.get("question", ""),
                "best_answer": q_data.get("best_answer", ""), # Necesario para el scorer después
                "retry_count": 0
            }
            # La publicación a Kafka puede bloquear brevemente, pero es manejado por la librería
            await publish_to_kafka(message_to_send)

        latency_ms = int((time.perf_counter() - t0) * 1000)
        # Opcional: Podríamos registrar esta latencia (de generar/verificar/publicar)
        # print(f"[req {idx}] Procesado en {latency_ms}ms. Publicado: {not already_processed}")


async def loop_time(client: httpx.AsyncClient):
    """Bucle principal basado en tiempo."""
    print(f"[loop] Iniciando generación basada en tiempo por {DURATION_SECONDS}s...")
    start_time = time.monotonic()
    end_time = start_time + DURATION_SECONDS
    tasks = []
    i = 0

    # Lógica para distribución 'bursty' (igual que antes)
    is_bursty = (TRAFFIC_DIST == "bursty")
    if is_bursty:
        high_rate = BASE_RATE_RPS * 3
        low_rate = max(BASE_RATE_RPS * 0.2, 0.1)
        current_rate = high_rate
        swap_interval = 5.0 # segundos
        next_swap_time = start_time + swap_interval
        print(f"[loop] Modo Bursty: alternando entre {high_rate:.1f} RPS y {low_rate:.1f} RPS cada {swap_interval}s.")

    while time.monotonic() < end_time:
        i += 1
        # Crea la tarea para procesar una solicitud
        task = asyncio.create_task(one_request(client, i))
        tasks.append(task)
        # task.add_done_callback(lambda t: tasks.remove(t)) # Opcional: limpiar lista

        # Calcular tiempo de espera para la siguiente petición
        if is_bursty:
            # Ajustar tasa si es momento de cambiar
            if time.monotonic() >= next_swap_time:
                current_rate = high_rate if current_rate == low_rate else low_rate
                next_swap_time += swap_interval
                # print(f"[loop] Bursty swap: nueva tasa = {current_rate:.1f} RPS")
            wait_time = interarrival(current_rate)
        else: # Poisson
            wait_time = interarrival(BASE_RATE_RPS)

        await asyncio.sleep(wait_time)

    print(f"[loop] Tiempo de generación ({DURATION_SECONDS}s) completado.")
    print("[loop] Esperando a que las tareas pendientes finalicen...")
    # Esperar a que todas las tareas lanzadas terminen
    if tasks:
        # Usamos wait para manejar timeouts si alguna tarea se queda pegada
        done, pending = await asyncio.wait(tasks, timeout=60) # Timeout de 60s extra
        if pending:
             print(f"[loop][WARN] {len(pending)} tareas no finalizaron después del timeout.")
             for task in pending:
                 task.cancel() # Intentar cancelar las tareas pendientes
    print("[loop] Todas las tareas finalizadas.")


async def loop_count(client: httpx.AsyncClient, n: int):
    """Bucle principal basado en número de peticiones (menos común para simulación)."""
    print(f"[loop] Iniciando {n} peticiones...")
    tasks = []
    # Lanzar todas las tareas (controlado por el semáforo)
    for i in range(1, n + 1):
         tasks.append(asyncio.create_task(one_request(client, i)))

    print(f"[loop] {n} tareas creadas. Esperando finalización...")
    if tasks:
        done, pending = await asyncio.wait(tasks, timeout=n*2 + 60) # Timeout generoso
        if pending:
             print(f"[loop][WARN] {len(pending)} tareas no finalizaron después del timeout.")
             for task in pending:
                 task.cancel()
    print("[loop] Todas las tareas finalizadas.")


async def main():
    # Timeouts por defecto del cliente httpx (para storage-api)
    timeout = httpx.Timeout(connect=5.0, read=15.0, write=10.0, pool=None) # Pool None puede ser mejor para muchas requests cortas

    # Asegurarse que el productor Kafka esté listo
    if not producer:
        print("[main][FATAL] No se pudo conectar a Kafka. Abortando.")
        return

    async with httpx.AsyncClient(timeout=timeout) as client:
        # Warmup (opcional, verifica dependencias)
        # await warmup(client) # Podemos simplificar o quitar warmup ahora

        # Ejecutar el bucle principal
        if TOTAL_REQUESTS > 0:
            await loop_count(client, TOTAL_REQUESTS)
        else:
            await loop_time(client)

        # Imprimir resumen
        print("-" * 30)
        print(f"Resumen de la Corrida {RUN_ID}:")
        print(f"  Preguntas Publicadas en Kafka: {published_count}")
        print(f"  Preguntas Saltadas (ya procesadas): {skipped_count}")
        total_intentos = published_count + skipped_count
        print(f"  Total de Intentos: {total_intentos}")
        if total_intentos > 0:
             print(f"  Tasa de Publicación (nuevas / total): {(published_count / total_intentos) * 100:.2f}%")
        print("-" * 30)

    # Cerrar el productor Kafka al final
    if producer:
        print("[main] Cerrando productor Kafka...")
        producer.flush() # Asegura que todos los mensajes pendientes se envíen
        producer.close()
        print("[main] Productor Kafka cerrado.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[main] Ejecución interrumpida por el usuario.")
        # Intentar cerrar el productor si existe
        if producer:
            print("[main] Intentando cerrar productor Kafka...")
            try:
                producer.flush(timeout=5)
                producer.close(timeout=5)
                print("[main] Productor Kafka cerrado.")
            except Exception as e:
                print(f"[main][WARN] Error al cerrar productor Kafka: {e}")