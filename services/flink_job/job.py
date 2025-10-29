import os
import json
import logging
import requests # Para llamar al scorer

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, FilterFunction, ProcessFunction
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment

conf = Configuration()
conf.set_string("pipeline.jars",
                "file:///opt/flink/opt/flink-sql-connector-kafka-1.17.1.jar")
env = StreamExecutionEnvironment.get_execution_environment(conf)


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger("flink_job")

# --- Configuración (Leer de variables de entorno) ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC_RESP_OK = os.getenv('KAFKA_TOPIC_RESP_OK', 'respuestas_llm_ok')
KAFKA_TOPIC_VALIDADOS = os.getenv('KAFKA_TOPIC_VALIDADOS', 'resultados_validados')
KAFKA_TOPIC_PREGUNTAS = os.getenv('KAFKA_TOPIC_PREGUNTAS', 'preguntas_nuevas') # Para re-encolar
KAFKA_CONSUMER_GROUP_FLINK = os.getenv('KAFKA_CONSUMER_GROUP_FLINK', 'flink_scorer_group')

URL_SCORER = os.getenv('URL_SCORER', 'http://scorer:8004/score')
SCORE_THRESHOLD = float(os.getenv('SCORE_THRESHOLD', '0.1')) # Umbral de calidad (ej: 0.1)
MAX_FLINK_RETRIES = int(os.getenv('MAX_FLINK_RETRIES', 1)) # Máximos reintentos INICIADOS por Flink

log.info(f"Iniciando Job Flink: Score Threshold={SCORE_THRESHOLD}, Max Flink Retries={MAX_FLINK_RETRIES}")
log.info(f"Kafka Servers: {KAFKA_BOOTSTRAP_SERVERS}")
log.info(f"Consumiendo de: {KAFKA_TOPIC_RESP_OK}")
log.info(f"Publicando Validados en: {KAFKA_TOPIC_VALIDADOS}")
log.info(f"Republicando Preguntas en: {KAFKA_TOPIC_PREGUNTAS}")
log.info(f"URL Scorer: {URL_SCORER}")

# --- Definición del Job PyFlink ---

# 1. Crear el entorno de ejecución
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

# 2. Añadir dependencias JAR para el conector Kafka (¡MUY IMPORTANTE!)
# --- CORRECCIÓN ---: Ruta correcta del JAR en la imagen oficial flink:1.17
kafka_jar_path = "file:///opt/flink/opt/flink-sql-connector-kafka-1.17.1.jar"
log.info(f"Añadiendo JAR Kafka desde: {kafka_jar_path}")
env.add_jars(kafka_jar_path)


# 3. Definir el Tipo de Fila (Row Type) para los mensajes Kafka
# --- CORRECCIÓN ---: Añadido retry_count y cambiado question_id a LONG
# Asumiendo que `respuestas_llm_ok` tiene: question_id, question_text, llm_answer, best_answer, retry_count
source_type_info = Types.ROW_NAMED(
    ["question_id", "question_text", "llm_answer", "best_answer", "retry_count"],
    [Types.LONG(), Types.STRING(), Types.STRING(), Types.STRING(), Types.INT()]
)

# 4. Crear el Consumidor Kafka (Source)
kafka_consumer = FlinkKafkaConsumer(
    topics=KAFKA_TOPIC_RESP_OK,
    deserialization_schema=JsonRowDeserializationSchema.builder().type_info(source_type_info).build(),
    properties={
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_CONSUMER_GROUP_FLINK,
        'auto.offset.reset': 'earliest'
    }
)

# 5. Leer el Stream desde Kafka
source_stream = env.add_source(kafka_consumer).name("KafkaSource_RespOK")

# --- Lógica de Procesamiento ---

# 6. Definir una función Map para llamar al Scorer
# --- NUEVA LÓGICA ---: Implementación de la llamada HTTP al scorer
class ScorerMapFunction(MapFunction):
    def map(self, row):
        # row es un objeto Row de Flink
        qid = row.question_id
        llm_answer = row.llm_answer
        best_answer = row.best_answer
        retry_count_llm = row.retry_count # Capturar retry_count original de llm_worker
        score = 0.0 # Default score
        error_msg = None # Default error

        log.info(f"Flink: Calculando score para QID: {qid}")
        try:
            # Validar inputs antes de llamar al scorer
            if not best_answer or not llm_answer:
                 raise ValueError("gold_answer o llm_answer están vacíos o son None")

            # Llamada síncrona al servicio scorer
            response = requests.post(
                URL_SCORER,
                json={"gold_answer": best_answer, "llm_answer": llm_answer},
                timeout=15 # Timeout de 15 segundos para la llamada
            )
            
            response.raise_for_status() # Lanza excepción si hay error HTTP (4xx, 5xx)
            
            score_data = response.json()
            if 'score' in score_data and isinstance(score_data['score'], (int, float)):
                score = score_data['score']
                log.info(f"Flink: Score obtenido para QID {qid}: {score:.4f}")
            else:
                raise ValueError(f"Respuesta del scorer inválida: {score_data}")

        except requests.exceptions.Timeout:
            log.error(f"Flink: Timeout al llamar al scorer para QID {qid}")
            error_msg = "Scorer Timeout"
        except requests.exceptions.HTTPError as e:
            log.error(f"Flink: Error HTTP {e.response.status_code} al llamar al scorer para QID {qid}: {e.response.text[:200]}")
            error_msg = f"Scorer HTTP Error: {e.response.status_code}"
        except requests.exceptions.RequestException as e:
            log.error(f"Flink: Error de red al llamar al scorer para QID {qid}: {e}")
            error_msg = f"Scorer Network Error: {e}"
        except ValueError as e:
             log.error(f"Flink: Error procesando respuesta/inputs del scorer para QID {qid}: {e}")
             error_msg = f"Scorer Data Error: {e}"
        except Exception as e:
             log.error(f"Flink: Error inesperado en ScorerMapFunction para QID {qid}: {e}")
             error_msg = f"Unexpected Error: {e}"

        # Devolver un nuevo Row con el score y el error añadidos
        # Importante: mantenemos los campos originales para los filtros posteriores
        return Types.ROW(
            question_id=qid,
            question_text=row.question_text,
            llm_answer=llm_answer,
            best_answer=best_answer,
            score=float(score),
            error=error_msg,
            retry_count=int(retry_count_llm), # Pasamos el contador original
            flink_retry_count=int(row.flink_retry_count if hasattr(row, 'flink_retry_count') else 0) # Inicializar contador Flink
        )
# --- FIN NUEVA LÓGICA ---

# 7. Aplicar la función Map para obtener el score
# --- CORRECCIÓN ---: Actualizado el tipo de salida
scored_stream = source_stream.map(
    ScorerMapFunction(),
    output_type=Types.ROW_NAMED(
        ["question_id", "question_text", "llm_answer", "best_answer", "score", "error", "retry_count", "flink_retry_count"],
        [Types.LONG(), Types.STRING(), Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.INT(), Types.INT()]
    )
).name("ScoreCalculator")


# 8. Filtrar/Dividir el stream basado en el score y errores (Lógica sin cambios)

# Stream para resultados VÁLIDOS (score >= threshold y sin error)
valid_stream = scored_stream.filter(
    lambda row: row.error is None and row.score >= SCORE_THRESHOLD
).name("FilterValidScore")

# Stream para resultados a RE-ENCOLAR (score < threshold, sin error, y reintentos Flink < MAX)
requeue_stream = scored_stream.filter(
    lambda row: row.error is None and row.score < SCORE_THRESHOLD and row.flink_retry_count < MAX_FLINK_RETRIES
).name("FilterRequeueScore")

# Stream para resultados con ERRORES del scorer o que superaron MAX_FLINK_RETRIES
error_or_max_retry_stream = scored_stream.filter(
    lambda row: row.error is not None or (row.score < SCORE_THRESHOLD and row.flink_retry_count >= MAX_FLINK_RETRIES)
).name("FilterErrorOrMaxRetry")


# --- Sinks (Destinos Kafka) ---

# 9. Sink para resultados VALIDADOS -> `resultados_validados`
# --- CORRECCIÓN ---: Ajustado tipo de question_id a LONG
validated_type_info = Types.ROW_NAMED(
    ["question_id", "llm_answer", "score"], # Ajusta campos según necesites
    [Types.LONG(), Types.STRING(), Types.FLOAT()]
)

kafka_producer_validated = FlinkKafkaProducer(
    topic=KAFKA_TOPIC_VALIDADOS,
    serialization_schema=JsonRowSerializationSchema.builder().with_type_info(validated_type_info).build(),
    producer_config={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "acks": "all"}
)
# Seleccionar y proyectar solo los campos necesarios
valid_stream.map(lambda row: Types.ROW(question_id=row.question_id, llm_answer=row.llm_answer, score=row.score),
                 output_type=validated_type_info)\
            .add_sink(kafka_producer_validated).name("KafkaSink_Validated")


# 10. Sink para RE-ENCOLAR -> `preguntas_nuevas`
# --- CORRECCIÓN ---: Ajustado tipo de question_id a LONG
requeue_type_info = Types.ROW_NAMED(
    ["question_id", "question_text", "best_answer", "retry_count", "flink_retry_count"],
    [Types.LONG(), Types.STRING(), Types.STRING(), Types.INT(), Types.INT()]
)

kafka_producer_requeue = FlinkKafkaProducer(
    topic=KAFKA_TOPIC_PREGUNTAS, # De vuelta al inicio del ciclo
    serialization_schema=JsonRowSerializationSchema.builder().with_type_info(requeue_type_info).build(),
    producer_config={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "acks": "all"}
)
# Incrementar el contador de reintentos de Flink
requeue_stream.map(lambda row: Types.ROW(question_id=row.question_id,
                                          question_text=row.question_text,
                                          best_answer=row.best_answer,
                                          retry_count=row.retry_count, # Mantenemos el original
                                          flink_retry_count=row.flink_retry_count + 1), # Incrementamos Flink retry
                   output_type=requeue_type_info)\
              .add_sink(kafka_producer_requeue).name("KafkaSink_Requeue")


# 11. (Opcional) Sink para Errores / Max Retries -> Log (Lógica sin cambios)
class LogErrorsProcessFunction(ProcessFunction):
    def process_element(self, row, ctx):
        log.error(f"Flink: Pregunta QID {row.question_id} descartada. Error: {row.error}, Score: {row.score:.4f}, Flink Retries: {row.flink_retry_count}")

error_or_max_retry_stream.process(LogErrorsProcessFunction()).name("LogErrorOrMaxRetry")


# 12. Ejecutar el Job
log.info("Definición del Job Flink completada. Ejecutando...")
env.execute("ScoreCalculatorJob_KafkaToKafka")
