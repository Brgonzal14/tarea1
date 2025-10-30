import os
import json
import time
import math
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import logging

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger("retry_worker")

# --- Configuración desde variables de entorno ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC_REINTENTAR = os.getenv('KAFKA_TOPIC_RESP_REINTENTAR', 'respuestas_llm_fallidas_reintentar')
KAFKA_TOPIC_PREGUNTAS = os.getenv('KAFKA_TOPIC_PREGUNTAS', 'preguntas_nuevas') # Tópico destino
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP_RETRY', 'retry_worker_group')
BASE_DELAY_S = int(os.getenv('RETRY_BASE_DELAY_S', 5)) # Delay base en segundos
MAX_DELAY_S = int(os.getenv('RETRY_MAX_DELAY_S', 60)) # Delay máximo en segundos

log.info(f"Conectando a Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
log.info(f"Consumiendo del tópico de reintentos: {KAFKA_TOPIC_REINTENTAR}")
log.info(f"Publicando de vuelta en: {KAFKA_TOPIC_PREGUNTAS}")
log.info(f"Delay base para reintentos: {BASE_DELAY_S}s, Máximo: {MAX_DELAY_S}s")

# --- Clientes Kafka ---
consumer_retry = None
producer_retry = None

def inicializar_kafka_clients_retry():
    """Intenta inicializar el consumidor y productor Kafka para reintentos."""
    global consumer_retry, producer_retry
    max_attempts = 5
    attempt = 0
    wait_time = 5

    while attempt < max_attempts:
        attempt += 1
        try:
            log.info(f"Intento {attempt}/{max_attempts} para conectar a Kafka (retry worker)...")
            consumer_retry = KafkaConsumer(
                KAFKA_TOPIC_REINTENTAR,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_CONSUMER_GROUP,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                consumer_timeout_ms=-1,
                max_poll_interval_ms=300000
            )
            log.info("Consumidor Kafka (retry) inicializado.")

            producer_retry = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                retry_backoff_ms=100
            )
            log.info("Productor Kafka (retry) inicializado.")
            return True # Éxito

        except KafkaError as e:
            log.error(f"Error al conectar con Kafka (retry worker, intento {attempt}/{max_attempts}): {e}")
            if attempt < max_attempts:
                log.info(f"Reintentando en {wait_time} segundos...")
                time.sleep(wait_time)
                wait_time *= 2
            else:
                log.error("Máximos intentos alcanzados. No se pudo conectar a Kafka (retry worker).")
                return False # Fracaso

    return False

def calcular_delay(retry_count: int) -> int:
    """Calcula el delay usando backoff exponencial con jitter."""
    # Delay = BASE * 2^retry_count +/- random Jitter
    delay = BASE_DELAY_S * math.pow(2, retry_count)
    jitter = delay * 0.2 * (2 * random.random() - 1) # Jitter +/- 20%
    total_delay = min(delay + jitter, MAX_DELAY_S) # Aplicar delay máximo
    return max(1, int(total_delay)) # Asegurar al menos 1 segundo

def republicar_mensaje(message: dict):
    """Espera el delay calculado y republica el mensaje en el tópico de preguntas."""
    if not producer_retry:
        log.error("Productor Kafka (retry) no disponible, no se puede republicar.")
        return

    retry_count = message.get('retry_count', 0) # El count ya fue incrementado por llm_worker
    delay = calcular_delay(retry_count - 1) # Calcular delay basado en el intento *anterior*

    qid = message.get('question_id')
    log.info(f"Reintentando Pregunta ID: {qid} (intento {retry_count}). Esperando {delay} segundos...")

    # Esperar (bloqueante, simple para este worker)
    time.sleep(delay)

    try:
        # Republicar en el tópico original de preguntas
        future = producer_retry.send(KAFKA_TOPIC_PREGUNTAS, value=message)
        future.get(timeout=10) # Espera confirmación
        log.info(f"Pregunta ID: {qid} republicada exitosamente en tópico '{KAFKA_TOPIC_PREGUNTAS}' para reintento {retry_count}.")
    except KafkaError as e:
        log.error(f"Error al republicar mensaje para reintento en '{KAFKA_TOPIC_PREGUNTAS}': {e}")
    except Exception as e:
         log.error(f"Error inesperado al republicar para reintento en '{KAFKA_TOPIC_PREGUNTAS}': {e}")


def procesar_reintentos():
    """Bucle principal para consumir mensajes fallidos y republicarlos."""
    if not consumer_retry:
        log.error("Consumidor Kafka (retry) no disponible. Abortando.")
        return

    log.info("Iniciando bucle de consumo de mensajes para reintento...")
    for message in consumer_retry:
        log.info(f"Mensaje de reintento recibido - Tópico: {message.topic}, Offset: {message.offset}")
        pregunta_data = message.value

        if not isinstance(pregunta_data, dict) or 'question_id' not in pregunta_data:
            log.warning(f"Mensaje de reintento inválido recibido, saltando: {pregunta_data}")
            continue

        # Llama a la función que maneja el delay y la republicación
        republicar_mensaje(pregunta_data)

        # Confirmar procesamiento (si es necesario)
        # consumer_retry.commit()

    log.info("Bucle de consumo de reintentos finalizado.")


if __name__ == "__main__":
    if inicializar_kafka_clients_retry():
        try:
            procesar_reintentos()
        except KeyboardInterrupt:
            log.info("Interrupción por teclado recibida (retry worker).")
        finally:
            log.info("Cerrando clientes Kafka (retry worker)...")
            if consumer_retry:
                consumer_retry.close()
            if producer_retry:
                producer_retry.flush()
                producer_retry.close()
            log.info("Clientes Kafka (retry worker) cerrados.")