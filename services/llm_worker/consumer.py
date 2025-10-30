import os
import json
import time
import httpx
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import logging
import asyncio


# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger("llm_worker")

# --- Configuración desde variables de entorno ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC_PREGUNTAS = os.getenv('KAFKA_TOPIC_PREGUNTAS', 'preguntas_nuevas')
KAFKA_TOPIC_RESP_OK = os.getenv('KAFKA_TOPIC_RESP_OK', 'respuestas_llm_ok')
KAFKA_TOPIC_RESP_REINTENTAR = os.getenv('KAFKA_TOPIC_RESP_REINTENTAR', 'respuestas_llm_fallidas_reintentar')
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'llm_worker_group') # ID del grupo consumidor

URL_RESPONDER_LLM = os.getenv('URL_RESPONDER_LLM', 'http://responder-llm:8003/answer')
LLM_TIMEOUT_S = int(os.getenv('LLM_TIMEOUT_S', 45)) # Timeout para la llamada al LLM
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3)) # Máximo número de reintentos por pregunta

log.info(f"Conectando a Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
log.info(f"Consumiendo del tópico: {KAFKA_TOPIC_PREGUNTAS}")
log.info(f"Publicando OK en: {KAFKA_TOPIC_RESP_OK}")
log.info(f"Publicando Reintentos en: {KAFKA_TOPIC_RESP_REINTENTAR}")
log.info(f"URL del LLM: {URL_RESPONDER_LLM}")

# --- Clientes Kafka ---
consumer = None
producer = None

def inicializar_kafka_clients():
    """Intenta inicializar el consumidor y productor Kafka, con reintentos."""
    global consumer, producer
    max_attempts = 5
    attempt = 0
    wait_time = 5

    while attempt < max_attempts:
        attempt += 1
        try:
            log.info(f"Intento {attempt}/{max_attempts} para conectar a Kafka...")
            # Consumidor: Lee del tópico de preguntas nuevas
            consumer = KafkaConsumer(
                KAFKA_TOPIC_PREGUNTAS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_CONSUMER_GROUP,
                # Deserializador: convierte JSON bytes a diccionario Python
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest', # Empieza a leer desde el principio si es un grupo nuevo
                consumer_timeout_ms=-1, # Espera indefinidamente por nuevos mensajes
                max_poll_interval_ms=300000
            )
            log.info("Consumidor Kafka inicializado.")

            # Productor: Publica en los tópicos de respuesta OK o Reintentar
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                retry_backoff_ms=100
            )
            log.info("Productor Kafka inicializado.")
            return True # Éxito

        except KafkaError as e:
            log.error(f"Error al conectar con Kafka (intento {attempt}/{max_attempts}): {e}")
            if attempt < max_attempts:
                log.info(f"Reintentando en {wait_time} segundos...")
                time.sleep(wait_time)
                wait_time *= 2 # Backoff exponencial simple
            else:
                log.error("Máximos intentos alcanzados. No se pudo conectar a Kafka.")
                return False # Fracaso después de todos los intentos

    return False

async def llamar_llm(http_client: httpx.AsyncClient, pregunta_texto: str) -> httpx.Response:
    """Llama al servicio responder-llm con reintentos HTTP básicos."""
    # Nota: httpx puede manejar reintentos, pero aquí lo hacemos manual para distinguir errores
    try:
        response = await http_client.post(URL_RESPONDER_LLM, json={'question': pregunta_texto}, timeout=LLM_TIMEOUT_S)
        return response
    except httpx.RequestError as e:
        log.warning(f"Error de red al llamar al LLM: {e}")
        # Simulamos una respuesta HTTP 503 Service Unavailable para que sea manejada como reintento
        return httpx.Response(status_code=503, text=f"Network error: {e}")
    except Exception as e:
         log.error(f"Error inesperado al llamar al LLM: {e}")
         return httpx.Response(status_code=500, text=f"Unexpected error: {e}")


def publicar_mensaje(topic: str, message: dict):
    """Publica un mensaje en un tópico Kafka específico."""
    if not producer:
        log.error("Productor Kafka no disponible, no se puede publicar.")
        return
    try:
        future = producer.send(topic, value=message)
        future.get(timeout=10) # Espera confirmación
        log.info(f"Mensaje publicado exitosamente en tópico '{topic}'. Question ID: {message.get('question_id')}")
    except KafkaError as e:
        log.error(f"Error al publicar mensaje en tópico '{topic}': {e}")
    except Exception as e:
         log.error(f"Error inesperado al publicar en tópico '{topic}': {e}")


async def procesar_mensajes():
    """Bucle principal para consumir mensajes y procesarlos."""
    if not consumer:
        log.error("Consumidor Kafka no disponible. Abortando.")
        return

    log.info("Iniciando bucle de consumo de mensajes...")
    async with httpx.AsyncClient() as client:
        for message in consumer:
            log.info(f"Mensaje recibido - Tópico: {message.topic}, Partición: {message.partition}, Offset: {message.offset}")
            pregunta_data = message.value # Ya deserializado a diccionario

            if not isinstance(pregunta_data, dict) or 'question_id' not in pregunta_data or 'question_text' not in pregunta_data:
                log.warning(f"Mensaje inválido recibido, saltando: {pregunta_data}")
                continue

            qid = pregunta_data['question_id']
            q_text = pregunta_data['question_text']
            best_answer = pregunta_data.get('best_answer', '') # Importante pasarlo
            retry_count = pregunta_data.get('retry_count', 0)

            log.info(f"Procesando Pregunta ID: {qid}, Intento: {retry_count + 1}")

            # Llamar al servicio LLM
            llm_response = await llamar_llm(client, q_text)

            status_code = llm_response.status_code

            if status_code == 200:
                # Éxito: Publicar en tópico respuestas_llm_ok
                try:
                    respuesta_llm = llm_response.json().get('answer', '')
                    mensaje_ok = {
                        "question_id": qid,
                        "question_text": q_text,
                        "llm_answer": respuesta_llm,
                        "best_answer": best_answer # Incluir para Flink/Scorer
                    }
                    publicar_mensaje(KAFKA_TOPIC_RESP_OK, mensaje_ok)
                except Exception as e:
                    log.error(f"Error al procesar respuesta exitosa del LLM para QID {qid}: {e}")
                    

            elif status_code in [429, 500, 502, 503, 504]: # Errores reintentables
                log.warning(f"LLM devolvió error reintentable ({status_code}) para QID {qid}. Body: {llm_response.text[:200]}")
                if retry_count < MAX_RETRIES:
                    # Reintentar: Publicar en tópico respuestas_llm_fallidas_reintentar
                    pregunta_data['retry_count'] = retry_count + 1
                    # Añade un delay antes de re-publicar podría hacerse aquí o en el consumidor de reintentos
                    publicar_mensaje(KAFKA_TOPIC_RESP_REINTENTAR, pregunta_data)
                else:
                    log.error(f"Máximo número de reintentos ({MAX_RETRIES}) alcanzado para QID {qid}. Descartando.")
            else:
                # Error no recuperable (ej. 400 Bad Request)
                log.error(f"LLM devolvió error NO recuperable ({status_code}) para QID {qid}. Body: {llm_response.text[:200]}")

    log.info("Bucle de consumo finalizado.")


if __name__ == "__main__":
    if inicializar_kafka_clients():
        try:
            asyncio.run(procesar_mensajes())
        except KeyboardInterrupt:
            log.info("Interrupción por teclado recibida.")
        finally:
            log.info("Cerrando clientes Kafka...")
            if consumer:
                consumer.close()
            if producer:
                producer.flush()
                producer.close()
            log.info("Clientes Kafka cerrados.")
    else:
        log.error("No se pudo iniciar el worker debido a problemas con Kafka.")