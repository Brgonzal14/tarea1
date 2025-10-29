import os
import asyncio
import signal
import time
import random
import json
from typing import Optional

import httpx
from kafka import KafkaProducer
from kafka.errors import KafkaError

# =========================
# Configuración (env vars)
# =========================
RUN_ID = os.getenv("RUN_ID", "dev-t2") + f"-{int(time.time())}"
BASE_RATE_RPS = float(os.getenv("BASE_RATE_RPS", "10"))          # tasas promedio (req/s)
DURATION_SECONDS = int(os.getenv("DURATION_SECONDS", "60"))       # duración si TOTAL_REQUESTS==0
TRAFFIC_DIST = os.getenv("TRAFFIC_DIST", "poisson").strip().lower()
TOTAL_REQUESTS = int(os.getenv("TOTAL_REQUESTS", "0"))            # si >0, ignora DURATION_SECONDS
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "64"))        # solicitudes simultáneas máximas
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "5"))              # timeout HTTP a storage-api
QID_START = int(os.getenv("QID_START", "1"))                      # id inicial por si quieres evitar colisiones

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_PREGUNTAS = os.getenv("KAFKA_TOPIC_PREGUNTAS", "preguntas_nuevas")
URL_STORAGE = os.getenv("URL_STORAGE", "http://storage-api:8001")

# =========================
# Utilidades
# =========================

def _now_ms() -> int:
    return int(time.time() * 1000)

def _inter_arrival_seconds(dist: str, base_rps: float) -> float:
    """
    Retorna el tiempo hasta el próximo evento en segundos según la distribución.
    - 'poisson': exponencial con lambda=base_rps
    - 'bursty': alterna ráfagas y pausas (simple)
    """
    if base_rps <= 0:
        return 0.0

    if dist == "poisson":
        return random.expovariate(base_rps)
    elif dist == "bursty":
        # Ráfagas cortas y pausas: 80% prob. ráfaga (más rápida), 20% pausa (más lenta)
        if random.random() < 0.8:
            return random.expovariate(base_rps * 1.8)
        else:
            return random.expovariate(max(base_rps * 0.4, 0.01))
    else:
        # fallback uniforme suave
        return max(1.0 / base_rps, 0.0)

QUESTIONS_POOL = [
    "¿Qué es la fotosíntesis?",
    "Explica el principio de conservación de la energía.",
    "¿Qué diferencia hay entre RAM y ROM?",
    "Define la ley de Ohm y da un ejemplo.",
    "¿Qué es un sistema distribuido?",
    "¿Cómo funciona un índice en una base de datos?",
    "¿Qué es la entropía en termodinámica?",
    "Resume el ciclo de vida del desarrollo de software (SDLC).",
    "¿Qué es la normalización en bases de datos?",
    "Explica la diferencia entre concurrencia y paralelismo."
]

def build_question(qid: int) -> dict:
    return {
        "run_id": RUN_ID,
        "question_id": qid,
        "question_text": random.choice(QUESTIONS_POOL),
        "retry_count": 0,
        "ts_ms": _now_ms()
    }

# =========================
# Clientes / Recursos
# =========================

def make_kafka_producer() -> KafkaProducer:
    # Nota: kafka-python es síncrono; usaremos to_thread para no bloquear el event loop.
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=3,
        retry_backoff_ms=100,
        linger_ms=20,
        batch_size=32768,
        compression_type="lz4",
        request_timeout_ms=15000,
        max_in_flight_requests_per_connection=5,
    )

async def storage_exists(client: httpx.AsyncClient, qid: int) -> Optional[bool]:
    """
    Consulta si la pregunta ya fue persistida.
    Devuelve True/False si se pudo determinar, o None si hubo error transitorio.
    Acepta dos estilos de API:
      - 200 JSON: {"exists": true/false}
      - 200/404 sin cuerpo (interpretamos 404 como no existe)
    """
    url = f"{URL_STORAGE.rstrip('/')}/results/exists/{qid}"
    try:
        resp = await client.get(url, timeout=HTTP_TIMEOUT)
        if resp.status_code == 200:
            # Intentamos parsear JSON; si falla, asumimos que 200 => existe
            try:
                data = resp.json()
                if isinstance(data, dict) and "exists" in data:
                    return bool(data["exists"])
            except Exception:
                return True
            return True
        elif resp.status_code == 404:
            return False
        else:
            return None
    except Exception:
        return None

async def send_to_kafka(producer: KafkaProducer, message: dict) -> bool:
    """
    Envía un dict a Kafka con clave = question_id (para orden por key).
    Retorna True si fue exitoso, False en error.
    """
    key_bytes = str(message.get("question_id", "")).encode("utf-8")
    try:
        # producer.send() retorna un Future (síncrono). Usamos to_thread para no bloquear asyncio.
        def _send_sync():
            fut = producer.send(KAFKA_TOPIC_PREGUNTAS, key=key_bytes, value=message)
            return fut.get(timeout=10)
        _ = await asyncio.to_thread(_send_sync)
        return True
    except KafkaError as ke:
        print(f"[kafka][ERR] {ke}")
        return False
    except Exception as e:
        print(f"[kafka][ERR] {e}")
        return False

# =========================
# Main loop
# =========================

async def run_load():
    """
    Genera carga según distribución y límites.
    - Si TOTAL_REQUESTS > 0: envía exactamente ese número.
    - Si TOTAL_REQUESTS == 0: envía por DURATION_SECONDS.
    Evita duplicados consultando storage-api antes de publicar.
    """
    print(f"[traffic-gen] RUN_ID={RUN_ID} dist={TRAFFIC_DIST} base_rps={BASE_RATE_RPS} "
          f"dur={DURATION_SECONDS}s total_requests={TOTAL_REQUESTS}")
    print(f"[traffic-gen] Kafka: {KAFKA_BOOTSTRAP_SERVERS}  topic: {KAFKA_TOPIC_PREGUNTAS}")
    print(f"[traffic-gen] Storage: {URL_STORAGE}  max_concurrency={MAX_CONCURRENCY}")

    producer = make_kafka_producer()
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    stop = asyncio.Event()

    # Manejo de SIGTERM para cierres limpios en contenedor
    try:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, stop.set)
    except NotImplementedError:
        # Windows/otros: ignorar
        pass

    published = 0
    skipped = 0
    errors = 0

    async with httpx.AsyncClient() as client:
        tasks = []

        async def one_shot(qid: int):
            nonlocal published, skipped, errors
            async with sem:
                # 1) Evitar duplicados
                exists = await storage_exists(client, qid)
                if exists is True:
                    skipped += 1
                    return
                # en caso de error transitorio de exists (None), continuamos igual

                # 2) Construir y enviar
                msg = build_question(qid)
                ok = await send_to_kafka(producer, msg)
                if ok:
                    published += 1
                else:
                    errors += 1

        # Generación de IDs
        next_qid = QID_START

        # 1) Modo por total
        if TOTAL_REQUESTS > 0:
            for _ in range(TOTAL_REQUESTS):
                if stop.is_set():
                    break
                tasks.append(asyncio.create_task(one_shot(next_qid)))
                next_qid += 1
                await asyncio.sleep(_inter_arrival_seconds(TRAFFIC_DIST, BASE_RATE_RPS))
        else:
            # 2) Modo por duración
            deadline = time.time() + DURATION_SECONDS
            while time.time() < deadline and not stop.is_set():
                tasks.append(asyncio.create_task(one_shot(next_qid)))
                next_qid += 1
                await asyncio.sleep(_inter_arrival_seconds(TRAFFIC_DIST, BASE_RATE_RPS))

        # Esperar tareas en vuelo o cancelarlas si llega stop
        if tasks:
            try:
                await asyncio.wait(tasks, timeout=10)
            except Exception:
                pass

    # Flush/Close Kafka
    try:
        producer.flush(timeout=10)
    except Exception as e:
        print(f"[kafka][WARN] flush: {e}")
    try:
        producer.close(timeout=5)
    except Exception as e:
        print(f"[kafka][WARN] close: {e}")

    print(f"[traffic-gen] Resumen → publicados={published}  saltados(exists)={skipped}  errores={errors}")

async def main():
    await run_load()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[traffic-gen] Interrumpido por usuario.")
